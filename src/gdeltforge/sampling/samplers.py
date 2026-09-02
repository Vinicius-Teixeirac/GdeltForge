"""
samplers.py

Sampling utilities for GDELT parquet datasets.
Provides:
    - IndexedSampler
    - DailySampler
    - FilteredSampler
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from tqdm import tqdm

from gdeltforge.scraping.scraper import filter_paths_by_date, parse_file_date
from gdeltforge.utils.io import clearer_dataset_errors
from gdeltforge.utils.logging import get_logger

from . import cameo_codes
from .indexer import FileIndex
from .rng import ReproducibleRNG

logger = get_logger(__name__)

# ----------------------------------------------------------
# Filter operation types
# ----------------------------------------------------------
class FilterType(Enum):
    EQUALS = "equals"
    IN_LIST = "in_list"
    RANGE = "range"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    BETWEEN = "between"


# ----------------------------------------------------------
# Indexed Sampler
# ----------------------------------------------------------
class IndexedSampler:
    """
    Draws samples from many parquet files without loading the entire dataset.
    Uses FileIndex to resolve "global row index" -> (file, row).

    When historical_folder is provided the sampler includes Hive-partitioned
    historical files in the global index alongside the flat daily files.
    """

    def __init__(
        self,
        folder_path: str,
        historical_folder: str | None = None,
        random_state: int | None = 42,
        columns: set[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        date_parser: Callable[[str], tuple[date | None, date | None]] = parse_file_date,
    ):
        self.folder = Path(folder_path)
        self.columns = columns
        parquet_files = sorted(self.folder.glob("*.parquet"))

        if historical_folder:
            hist_path = Path(historical_folder)
            if hist_path.exists():
                parquet_files = sorted(
                    set(parquet_files) | set(hist_path.rglob("*.parquet"))
                )

        parquet_files = filter_paths_by_date(
            parquet_files, start_date, end_date, date_parser=date_parser
        )

        if not parquet_files:
            raise FileNotFoundError(
                f"No parquet files found in {self.folder}"
                + (f" or {historical_folder}" if historical_folder else "")
                + (f" within [{start_date} - {end_date}]" if start_date or end_date else "")
            )

        self.rng   = ReproducibleRNG(random_state)
        self.index = FileIndex(parquet_files)

        logger.info(
            f"IndexedSampler: {len(self.index.files)} files, "
            f"{self.index.total_rows:,} total rows."
        )

    def get_random_sample(self, n: int) -> pl.DataFrame:
        """Sample n rows uniformly across all parquet files."""
        if n > self.index.total_rows:
            raise ValueError("Requested sample size > total available rows")

        random_indices = self.rng.choice(self.index.total_rows, n, replace=False)
        logger.info(f"Sampling {n} rows across {len(self.index.files)} files")

        indices_by_file = self.index.group_indices_by_file(random_indices)
        logger.info("Mapped the indices to the correspondent files")

        read_columns = list(self.columns) if self.columns else None
        sampled = []
        for file_path, relative_rows in tqdm(indices_by_file.items(), desc="Loading samples"):
            df = pl.read_parquet(file_path, columns=read_columns)
            sampled.append(df.gather(relative_rows))

        return pl.concat(sampled)


# ----------------------------------------------------------
# Daily Sampler
# ----------------------------------------------------------
class DailySampler:
    """
    For each day, sample a fixed number of rows per parquet file.

    When historical_folder is provided the sampler includes Hive-partitioned
    historical files alongside the flat daily files.
    """

    def __init__(
        self,
        folder_path: str,
        historical_folder: str | None = None,
        random_state: int | None = 42,
        columns: set[str] | None = None,
        date_column: str = "Day",
        start_date: date | None = None,
        end_date: date | None = None,
        date_parser: Callable[[str], tuple[date | None, date | None]] = parse_file_date,
    ):
        self.folder = Path(folder_path)
        self.historical_folder: Path | None = (
            Path(historical_folder) if historical_folder else None
        )
        self.columns = columns
        self.date_column = date_column
        self.start_date = start_date
        self.end_date = end_date
        self.date_parser = date_parser
        self.rng = ReproducibleRNG(random_state)

    def get_daily_samples(self, samples_per_day: int = 10) -> pl.DataFrame:
        flat_files = list(self.folder.glob("*.parquet"))
        hist_files = (
            list(self.historical_folder.rglob("*.parquet"))
            if self.historical_folder and self.historical_folder.exists()
            else []
        )
        parquet_files = flat_files + hist_files

        parquet_files = filter_paths_by_date(
            parquet_files, self.start_date, self.end_date, date_parser=self.date_parser
        )

        if not parquet_files:
            raise FileNotFoundError(
                f"No parquet files found in {self.folder}"
                + (f" or {self.historical_folder}" if self.historical_folder else "")
                + (
                    f" within [{self.start_date} - {self.end_date}]"
                    if self.start_date or self.end_date else ""
                )
            )

        # date_column drives the grouping below, so it has to be read even
        # if the caller didn't ask for it in --columns, otherwise every
        # file would silently look like it has no date column and get skipped.
        read_columns = list(self.columns | {self.date_column}) if self.columns else None

        daily: dict[Any, list[pl.DataFrame]] = {}

        for file_path in tqdm(parquet_files, desc="Daily sampling"):
            df = pl.read_parquet(file_path, columns=read_columns)
            if self.date_column not in df.columns:
                continue

            # polars' own group_by always yields a tuple key, even for a
            # single-column group_by (confirmed directly; pandas' groupby
            # instead returns a bare scalar in that case).
            for (day,), group in df.group_by(self.date_column, maintain_order=False):
                size = min(samples_per_day, len(group))
                if size == 0:
                    continue

                idx = self.rng.choice(len(group), size=size, replace=False)
                sample = group.gather(idx)
                daily.setdefault(day, []).append(sample)

        if not daily:
            return pl.DataFrame()

        all_samples = [pl.concat(chunks) for chunks in daily.values()]
        return pl.concat(all_samples)


# ----------------------------------------------------------
# FilteredSampler
# ----------------------------------------------------------
class FilteredSampler:
    """
    Filter + sample from folder of parquet files using a polars lazy scan
    with predicate pushdown + batch streaming.

    When historical_folder is provided the sampler unions a Hive-partitioned
    historical dataset with the flat daily files. Filters that reference Year
    or MonthYear benefit from directory-level predicate pushdown on the
    historical side.
    """

    def __init__(
        self,
        folder_path: str,
        gdelt_columns: list[str],
        columns: set[str] | None = None,
        filter_dict: dict[str, Any] | None = None,
        random_state: int | None = 42,
        historical_folder: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        date_parser: Callable[[str], tuple[date | None, date | None]] = parse_file_date,
    ):
        self._gdelt_columns_ordered = list(gdelt_columns)
        self.gdelt_columns = set(gdelt_columns)

        self.folder = Path(folder_path)
        self.historical_folder: Path | None = (
            Path(historical_folder) if historical_folder else None
        )
        self.columns     = columns or self.gdelt_columns
        self.filter_dict = filter_dict or {}
        self.rng         = ReproducibleRNG(random_state)
        self.start_date  = start_date
        self.end_date    = end_date
        self.date_parser = date_parser

        self._validate_columns()
        self._validate_filter_dict()

    # ---------- validation ----------
    def _validate_columns(self):
        invalid = self.columns - self.gdelt_columns
        if invalid:
            raise ValueError(f"Invalid columns: {invalid}")

    def _validate_filter_dict(self):
        def validate_block(block):
            if not isinstance(block, dict):
                raise ValueError("filter_dict must be dict/nested dicts")

            for key, val in block.items():
                if key in ("AND", "OR"):
                    if not isinstance(val, dict):
                        raise ValueError(f"{key} must contain a dict")
                    validate_block(val)
                else:
                    if key not in self.gdelt_columns:
                        raise ValueError(f"Invalid filter column: {key}")
                    self._warn_unrecognized_codes(key, val)

        validate_block(self.filter_dict)

    @staticmethod
    def _condition_values(cond: Any) -> list[str]:
        """String values an equality/in_list-style condition checks for.
        gt/lt/between/range conditions are numeric, so they never apply to
        country-code columns and are skipped."""
        if isinstance(cond, str):
            return [cond]
        if isinstance(cond, list):
            return [v for v in cond if isinstance(v, str)]
        if isinstance(cond, dict):
            op = cond.get("op")
            if op == FilterType.EQUALS.value and isinstance(cond.get("value"), str):
                return [cond["value"]]
            if op == FilterType.IN_LIST.value:
                return [v for v in cond.get("values", []) if isinstance(v, str)]
        return []

    def _warn_unrecognized_codes(self, column: str, cond: Any) -> None:
        """
        Warn (never raise) when a filter value on a known CAMEO-coded
        column isn't in GDELT's reference list for that column's code
        family. Column, not just value, matters here: "USA" is a real
        CAMEO actor-country code but not a real FIPS geo-country code, and
        vice versa; see the cameo_codes module docstring for the full set
        of families this covers.
        """
        unrecognized = [
            v for v in self._condition_values(cond)
            if cameo_codes.is_recognized_code(column, v) is False
        ]
        if unrecognized:
            family_name = cameo_codes.family_name_for_column(column)
            logger.warning(
                f"{column}: {unrecognized} not recognized as {family_name} "
                f"code(s). Could be a typo, or a legitimate code newer than this "
                f"reference list. Run `gdeltforge codes {column}` to check."
            )

    # ---------- recursively collect all column names referenced in a filter block ----------
    def _filter_columns(self, block: dict[str, Any]) -> set[str]:
        cols: set[str] = set()
        for key, val in block.items():
            if key in ("AND", "OR"):
                if isinstance(val, dict):
                    cols |= self._filter_columns(val)
            else:
                cols.add(key)
        return cols

    # ---------- convert simple condition -> polars expression ----------
    def _expr_for_condition(self, column: str, cond: Any) -> pl.Expr:
        f = pl.col(column)

        if isinstance(cond, (str, int, float, bool)):
            return f == cond

        if isinstance(cond, list):
            return f.is_in(cond)

        if isinstance(cond, tuple) and len(cond) == 2:
            lo, hi = cond
            return (f >= lo) & (f <= hi)

        if isinstance(cond, dict):
            op = cond.get("op")
            if op == FilterType.EQUALS.value:
                return f == cond["value"]
            if op == FilterType.IN_LIST.value:
                return f.is_in(cond["values"])
            if op == FilterType.GREATER_THAN.value:
                return f > cond["value"]
            if op == FilterType.LESS_THAN.value:
                return f < cond["value"]
            if op in (FilterType.RANGE.value, FilterType.BETWEEN.value):
                return (f >= cond["min"]) & (f <= cond["max"])

        raise ValueError(f"Invalid condition for {column}: {cond}")

    # ---------- recursive builder: filter_dict -> polars expression ----------
    def _build_expression(
        self, block: dict[str, Any], _join_with: str = "AND"
    ) -> pl.Expr | None:
        """
        Return a polars Expr or None if block is empty.
        Supports nested AND/OR and base column conditions.
        """
        if not block:
            return None

        expr = None

        def _combine(acc: pl.Expr | None, new: pl.Expr) -> pl.Expr:
            if acc is None:
                return new
            return (acc & new) if _join_with == "AND" else (acc | new)

        for key, val in block.items():
            if key == "AND":
                sub = self._build_expression(val, _join_with="AND")
                if sub is None:
                    continue
                expr = _combine(expr, sub)

            elif key == "OR":
                sub = self._build_expression(val, _join_with="OR")
                if sub is None:
                    continue
                expr = _combine(expr, sub)

            else:
                sub = self._expr_for_condition(key, val)
                expr = _combine(expr, sub)

        return expr

    # ---------- dataset: union of flat daily + historical partition files ----------
    def _dataset(self) -> pl.LazyFrame:
        all_files: list[Path] = list(self.folder.glob("*.parquet"))

        if self.historical_folder and self.historical_folder.exists():
            all_files += list(self.historical_folder.rglob("*.parquet"))

        all_files = filter_paths_by_date(
            all_files, self.start_date, self.end_date, date_parser=self.date_parser
        )

        if not all_files:
            raise FileNotFoundError(
                f"No parquet files found in {self.folder}"
                + (f" or {self.historical_folder}" if self.historical_folder else "")
                + (
                    f" within [{self.start_date} - {self.end_date}]"
                    if self.start_date or self.end_date else ""
                )
            )

        # polars' scan_parquet infers its schema from a single file rather
        # than the union of every file's schema the way pyarrow.dataset.
        # dataset() does, confirmed directly: a column present in some
        # files and absent from others (e.g. a historical partition file
        # converted under an older, narrower output_columns) raises
        # "extra column ... outside of expected schema" for whichever
        # file doesn't match the schema scan_parquet happened to infer,
        # regardless of file order. Reading every file's own footer
        # schema first (cheap, metadata-only, the same order of cost
        # pyarrow.dataset's own schema unification already paid) and
        # passing the union explicitly as schema= reproduces that
        # unification regardless of which file scan_parquet would
        # otherwise have picked. Predicate pushdown on Year / MonthYear
        # still works via row-group statistics: each historical partition
        # file has constant values for those columns, so non-matching
        # files are skipped without reading any row data.
        schema: dict[str, pl.DataType] = {}
        for f in all_files:
            for name, dtype in pl.read_parquet_schema(f).items():
                schema.setdefault(name, dtype)
        return pl.scan_parquet(all_files, schema=schema, missing_columns="insert")

    def _batches(self, needed_columns: list[str]):
        """
        Yield polars DataFrame batches matching the configured filter, via
        a lazy scan with predicate pushdown.
        """
        # Wrapped from dataset construction onward: _dataset() itself can
        # raise (every file's footer schema is read there), not only the
        # later batch collection. Confirmed empirically that which one
        # raises depends on where in the list a corrupt/non-parquet file
        # happens to land.
        with clearer_dataset_errors(f"filtered sample dataset in {self.folder}"):
            lf = self._dataset()
            expr = self._build_expression(self.filter_dict)
            if expr is not None:
                lf = lf.filter(expr)
            lf = lf.select(needed_columns)
            yield from lf.collect_batches(chunk_size=64_000)

    def _needed_columns(self) -> list[str]:
        """Union of requested columns and any column referenced in the filter expression."""
        return list(self.columns | self._filter_columns(self.filter_dict))

    # ---------- shared replacement-phase writer for both reservoir methods ----------
    @staticmethod
    def _dedup_last_write_per_slot(
        rand_slots: np.ndarray, capacity: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Resolve one batch's Algorithm R draws to the writes that should
        actually land: (target_slots, source_positions), accepted-only and
        deduplicated.

        rand_slots[k] < capacity means row k of the batch is accepted into
        slot rand_slots[k]. Two rows in the same batch can draw the same
        slot; true sequential Algorithm R applies draws in position order,
        so only the last (highest-position) writer for a given slot should
        survive: np.unique on the reversed array picks exactly that,
        since target_slots is already in ascending position order.
        """
        accept_mask = rand_slots < capacity
        target_slots = rand_slots[accept_mask]
        source_pos = np.where(accept_mask)[0]

        if target_slots.size > 1:
            _, last_occurrence_rev = np.unique(target_slots[::-1], return_index=True)
            keep = target_slots.size - 1 - last_occurrence_rev
            target_slots = target_slots[keep]
            source_pos = source_pos[keep]

        return target_slots, source_pos

    @staticmethod
    def _assign_column(arr: np.ndarray, idx: np.ndarray, values: np.ndarray) -> np.ndarray:
        """
        Write values into arr at idx, positionally, returning the (possibly
        new) array to write back into the caller's dict.

        A batch row can carry a NaN in a column that's been int64 so far
        (nullable numeric GDELT fields; polars' own nullable Int64 upcasts
        to float64+NaN the same way through .to_numpy(), confirmed
        directly, so this hazard survived the pandas-to-polars port
        unchanged). Checked this against plain numpy assignment directly:
        unlike pandas, numpy does NOT raise here -- `int_arr[idx] =
        nan_values` silently casts NaN to INT64_MIN with only a
        RuntimeWarning, so a try/except around the assignment can't catch
        it. Compute the correct common dtype up front instead (via
        np.result_type, e.g. int64+float64 -> float64) and upcast before
        ever writing, rather than reacting to a write that already
        corrupted data.
        """
        target_dtype = np.result_type(arr.dtype, values.dtype)
        if target_dtype != arr.dtype:
            arr = arr.astype(target_dtype)
        arr[idx] = values
        return arr

    @classmethod
    def _apply_reservoir_replacements(
        cls,
        reservoir_cols: dict[str, np.ndarray],
        batch: pl.DataFrame,
        rand_slots: np.ndarray,
        capacity: int,
    ) -> None:
        """
        Write accepted rows into their drawn reservoir slots, in place,
        column by column as plain numpy arrays rather than through a
        DataFrame-level positional setter. On a wide reservoir (GDELT has
        ~58 columns), that route costs roughly 15x more per write than
        direct numpy array assignment under the previous pandas
        implementation (iloc's positional-index setter goes through
        pandas' block manager instead of touching each column's array
        directly); that gap is what dominates a full-archive scan's
        wall-clock time, not the number of individual per-column calls.
        The reservoir is only turned back into a DataFrame once, at the
        very end of the scan (see get_random_sample / get_stratified_sample).
        """
        target_slots, source_pos = cls._dedup_last_write_per_slot(rand_slots, capacity)
        if target_slots.size == 0:
            return

        accepted = batch.gather(source_pos)
        for col, arr in reservoir_cols.items():
            reservoir_cols[col] = cls._assign_column(arr, target_slots, accepted[col].to_numpy())

    # ---------- API ----------
    def filter_dataset(self) -> pl.DataFrame:
        needed = self._needed_columns()

        frames: list[pl.DataFrame] = []
        for batch in tqdm(self._batches(needed), desc="Filtering parquet files"):
            try:
                if not batch.is_empty():
                    frames.append(batch.select(needed))
            except Exception as e:
                logger.warning(f"Skipping batch due to error: {e}")

        if not frames:
            return pl.DataFrame()
        return pl.concat(frames)

    # ---------- reservoir sampling for random sample ----------
    def get_random_sample(self, n: int) -> pl.DataFrame:
        if n <= 0:
            return pl.DataFrame()

        needed = self._needed_columns()
        fill_chunks: list[pl.DataFrame] = []
        filled    = 0
        reservoir_cols: dict[str, np.ndarray] | None = None
        total_seen = 0

        for df_batch in tqdm(self._batches(needed), desc="Sampling (random)"):
            batch_size = len(df_batch)
            if batch_size == 0:
                continue

            # Fill phase: accumulate chunks until the reservoir has n rows,
            # then turn it into plain per-column numpy arrays; see
            # _apply_reservoir_replacements for why.
            if filled < n:
                take = min(n - filled, batch_size)
                fill_chunks.append(df_batch.head(take))
                filled     += take
                total_seen += take

                if filled == n:
                    filled_df = pl.concat(fill_chunks)
                    # writable=True forces a genuinely independent copy:
                    # polars' own to_numpy() otherwise returns a read-only
                    # array sharing memory with the source column
                    # (confirmed directly; assigning into it without this
                    # raises "assignment destination is read-only"), unlike
                    # pandas' to_numpy(copy=True) equivalent this replaces.
                    reservoir_cols = {
                        c: filled_df[c].to_numpy(writable=True) for c in filled_df.columns
                    }
                    fill_chunks.clear()

                if take == batch_size:
                    continue

                df_batch   = df_batch.slice(take)
                batch_size = len(df_batch)

            # Replacement phase: vectorized slot selection via Vitter's Algorithm R.
            # For each row at global position p, draw j uniformly from [0, p].
            # Accept (replace reservoir slot j) iff j < n.
            # reservoir_cols is always set by this point: either from a
            # previous batch, or by the fill phase above within this same batch.
            assert reservoir_cols is not None
            positions  = np.arange(total_seen, total_seen + batch_size)
            rand_slots = self.rng.rng.integers(0, positions + 1)
            self._apply_reservoir_replacements(reservoir_cols, df_batch, rand_slots, n)

            total_seen += batch_size

        reservoir = (
            pl.DataFrame(reservoir_cols) if reservoir_cols is not None
            else pl.concat(fill_chunks) if fill_chunks
            else None
        )

        if reservoir is None or reservoir.is_empty():
            return pl.DataFrame()

        keep_cols = [c for c in self._gdelt_columns_ordered if c in reservoir.columns]
        return reservoir.select(keep_cols)

    # ---------- stratified reservoir sampling ----------
    # A dedicated grouping-key column, added and dropped inside the loop
    # below rather than substituting nulls into stratify_col itself: the
    # substitution must only affect which group a row lands in, not the
    # actual stratify_col value carried through to the final output (a
    # row with a genuine null in that column must still show a null
    # there, not the literal string "__NA__").
    _STRATIFY_GROUP_KEY = "__stratify_group_key__"

    def get_stratified_sample(self, stratify_col: str, n_per_group: int) -> pl.DataFrame:
        if n_per_group <= 0:
            return pl.DataFrame()

        needed = list(self.columns | {stratify_col} | self._filter_columns(self.filter_dict))

        fill_chunks:    dict[Any, list[pl.DataFrame]]   = {}
        filled:         dict[Any, int]                  = {}
        reservoir_cols: dict[Any, dict[str, np.ndarray]] = {}
        total_seen:     dict[Any, int]                  = {}

        for df_batch in tqdm(self._batches(needed), desc="Sampling (stratified)"):
            if df_batch.is_empty() or stratify_col not in df_batch.columns:
                continue

            # polars' group_by keeps null as its own group natively
            # (unlike pandas, which drops null-key groups by default),
            # but the "__NA__" sentinel is kept anyway rather than relied
            # on for a group KEY, since it's the group's dict key returned
            # to the caller too, and None and the literal string "__NA__"
            # need to be distinguishable there the same way they were
            # before this port.
            keyed_batch = df_batch.with_columns(
                pl.col(stratify_col).fill_null("__NA__").alias(self._STRATIFY_GROUP_KEY)
            )
            for (g,), group_df in keyed_batch.group_by(
                self._STRATIFY_GROUP_KEY, maintain_order=False
            ):
                group_df   = group_df.drop([self._STRATIFY_GROUP_KEY])
                group_size = len(group_df)

                if g not in total_seen:
                    total_seen[g]   = 0
                    fill_chunks[g]  = []
                    filled[g]       = 0

                # Fill phase: see get_random_sample's fill phase, same
                # pattern, once per stratify group.
                if filled[g] < n_per_group:
                    take = min(n_per_group - filled[g], group_size)
                    fill_chunks[g].append(group_df.head(take))
                    filled[g]      += take
                    total_seen[g]  += take

                    if filled[g] == n_per_group:
                        filled_df = pl.concat(fill_chunks[g])
                        # writable=True: see get_random_sample's identical
                        # fill-phase comment for why this can't be omitted.
                        reservoir_cols[g] = {
                            c: filled_df[c].to_numpy(writable=True) for c in filled_df.columns
                        }
                        fill_chunks[g] = []

                    if take == group_size:
                        continue

                    group_df   = group_df.slice(take)
                    group_size = len(group_df)

                # Replacement phase: vectorized slot selection via Vitter's Algorithm R
                positions  = np.arange(total_seen[g], total_seen[g] + group_size)
                rand_slots = self.rng.rng.integers(0, positions + 1)
                self._apply_reservoir_replacements(
                    reservoir_cols[g], group_df, rand_slots, n_per_group
                )

                total_seen[g] += group_size

        reservoirs: dict[Any, pl.DataFrame] = {
            g: pl.DataFrame(cols) for g, cols in reservoir_cols.items()
        }
        for g, chunks in fill_chunks.items():
            if chunks and g not in reservoirs:
                reservoirs[g] = pl.concat(chunks)

        if not reservoirs:
            return pl.DataFrame()

        sample    = pl.concat(list(reservoirs.values()))
        keep_cols = [c for c in self._gdelt_columns_ordered if c in sample.columns]
        return sample.select(keep_cols)
