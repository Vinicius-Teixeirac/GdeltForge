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
from typing import Any, cast

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
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

    def get_random_sample(self, n: int) -> pd.DataFrame:
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
            df = pd.read_parquet(file_path, columns=read_columns)
            sampled.append(df.iloc[relative_rows])

        return pd.concat(sampled, ignore_index=True)


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

    def get_daily_samples(self, samples_per_day: int = 10) -> pd.DataFrame:
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

        daily: dict[Any, list[pd.DataFrame]] = {}

        for file_path in tqdm(parquet_files, desc="Daily sampling"):
            df = pd.read_parquet(file_path, columns=read_columns)
            if self.date_column not in df.columns:
                continue

            for day, group in df.groupby(self.date_column):
                size = min(samples_per_day, len(group))
                if size == 0:
                    continue

                idx = self.rng.choice(len(group), size=size, replace=False)
                sample = group.iloc[idx]
                daily.setdefault(day, []).append(sample)

        if not daily:
            return pd.DataFrame()

        all_samples = [pd.concat(chunks) for chunks in daily.values()]
        return pd.concat(all_samples, ignore_index=True)


# ----------------------------------------------------------
# FilteredSampler
# ----------------------------------------------------------
class FilteredSampler:
    """
    Filter + sample from folder of parquet files using PyArrow dataset scanning
    + batch streaming.

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

    # ---------- convert simple condition -> pyarrow expression ----------
    def _expr_for_condition(self, column: str, cond: Any) -> ds.Expression:
        f = ds.field(column)

        if isinstance(cond, (str, int, float, bool)):
            return f == cond

        if isinstance(cond, list):
            return f.isin(cond)

        if isinstance(cond, tuple) and len(cond) == 2:
            lo, hi = cond
            return (f >= lo) & (f <= hi)

        if isinstance(cond, dict):
            op = cond.get("op")
            if op == FilterType.EQUALS.value:
                return f == cond["value"]
            if op == FilterType.IN_LIST.value:
                return f.isin(cond["values"])
            if op == FilterType.GREATER_THAN.value:
                return f > cond["value"]
            if op == FilterType.LESS_THAN.value:
                return f < cond["value"]
            if op in (FilterType.RANGE.value, FilterType.BETWEEN.value):
                return (f >= cond["min"]) & (f <= cond["max"])

        raise ValueError(f"Invalid condition for {column}: {cond}")

    # ---------- recursive builder: filter_dict -> pyarrow.dataset expression ----------
    def _build_expression(
        self, block: dict[str, Any], _join_with: str = "AND"
    ) -> ds.Expression | None:
        """
        Return a pyarrow.dataset Expression or None if block is empty.
        Supports nested AND/OR and base column conditions.
        """
        if not block:
            return None

        expr = None

        def _combine(acc: ds.Expression | None, new: ds.Expression) -> ds.Expression:
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
    def _dataset(self) -> ds.Dataset:
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

        # Pass files as a flat list so PyArrow uses a single physical schema.
        # Predicate pushdown on Year / MonthYear works via row-group statistics:
        # each historical partition file has constant values for those columns,
        # so non-matching files are skipped without reading any row data.
        return ds.dataset(all_files, format="parquet")

    def _batches(self, needed_columns: list[str]):
        """
        Yield pyarrow.RecordBatch objects matching the configured filter,
        using pyarrow.dataset Scanner with filter pushdown.
        """
        # Wrapped from dataset construction onward: ds.dataset() itself
        # can raise (schema inference reads at least the first file in
        # the list), not only the later scanner consumption. Confirmed
        # empirically that which one raises depends on where in the list
        # a corrupt/non-parquet file happens to land.
        with clearer_dataset_errors(f"filtered sample dataset in {self.folder}"):
            dataset = self._dataset()
            expr    = self._build_expression(self.filter_dict)

            scanner = dataset.scanner(columns=needed_columns, filter=expr, batch_size=64_000)
            yield from scanner.to_batches()

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
        (nullable numeric GDELT fields). Checked this against plain numpy
        assignment directly: unlike pandas, numpy does NOT raise here --
        `int_arr[idx] = nan_values` silently casts NaN to INT64_MIN with
        only a RuntimeWarning, so a try/except around the assignment can't
        catch it. Compute the correct common dtype up front instead (via
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
        batch: pd.DataFrame,
        rand_slots: np.ndarray,
        capacity: int,
    ) -> None:
        """
        Write accepted rows into their drawn reservoir slots, in place,
        column by column as plain numpy arrays rather than through pandas'
        DataFrame.iloc setter. On a wide reservoir (GDELT has ~58 columns),
        iloc's positional-index setter costs roughly 15x more per write
        than direct numpy array assignment, since it goes through pandas'
        block manager instead of touching each column's array directly;
        that gap is what dominates a full-archive scan's wall-clock time,
        not the number of individual per-column calls. The reservoir is
        only turned back into a DataFrame once, at the very end of the
        scan (see get_random_sample / get_stratified_sample).
        """
        target_slots, source_pos = cls._dedup_last_write_per_slot(rand_slots, capacity)
        if target_slots.size == 0:
            return

        accepted = batch.iloc[source_pos]
        for col, arr in reservoir_cols.items():
            reservoir_cols[col] = cls._assign_column(arr, target_slots, accepted[col].to_numpy())

    # ---------- API ----------
    def filter_dataset(self) -> pd.DataFrame:
        needed = self._needed_columns()

        frames: list[pd.DataFrame] = []
        for batch in tqdm(self._batches(needed), desc="Filtering parquet files"):
            try:
                df_batch = batch.to_pandas()
                if not df_batch.empty:
                    frames.append(df_batch[needed])
            except Exception as e:
                logger.warning(f"Skipping batch due to error: {e}")

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ---------- reservoir sampling for random sample ----------
    def get_random_sample(self, n: int) -> pd.DataFrame:
        if n <= 0:
            return pd.DataFrame()

        needed = self._needed_columns()
        fill_chunks: list[pd.DataFrame] = []
        filled    = 0
        reservoir_cols: dict[str, np.ndarray] | None = None
        total_seen = 0

        for batch in tqdm(self._batches(needed), desc="Sampling (random)"):
            df_batch   = batch.to_pandas()
            batch_size = len(df_batch)
            if batch_size == 0:
                continue

            # Fill phase: accumulate chunks until the reservoir has n rows,
            # then turn it into plain per-column numpy arrays; see
            # _apply_reservoir_replacements for why.
            if filled < n:
                take = min(n - filled, batch_size)
                fill_chunks.append(df_batch.iloc[:take])
                filled     += take
                total_seen += take

                if filled == n:
                    filled_df = pd.concat(fill_chunks, ignore_index=True)
                    reservoir_cols = {
                        c: filled_df[c].to_numpy(copy=True) for c in filled_df.columns
                    }
                    fill_chunks.clear()

                if take == batch_size:
                    continue

                df_batch   = df_batch.iloc[take:].reset_index(drop=True)
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
            pd.DataFrame(reservoir_cols) if reservoir_cols is not None
            else pd.concat(fill_chunks, ignore_index=True) if fill_chunks
            else None
        )

        if reservoir is None or reservoir.empty:
            return pd.DataFrame()

        keep_cols = [c for c in self._gdelt_columns_ordered if c in reservoir.columns]
        # Indexing a DataFrame with a list of columns always returns a
        # DataFrame at runtime; pandas-stubs' overloads can't always prove
        # that statically.
        return cast(pd.DataFrame, reservoir.reset_index(drop=True).loc[:, keep_cols])

    # ---------- stratified reservoir sampling ----------
    def get_stratified_sample(self, stratify_col: str, n_per_group: int) -> pd.DataFrame:
        if n_per_group <= 0:
            return pd.DataFrame()

        needed = list(self.columns | {stratify_col} | self._filter_columns(self.filter_dict))

        fill_chunks:    dict[Any, list[pd.DataFrame]]   = {}
        filled:         dict[Any, int]                  = {}
        reservoir_cols: dict[Any, dict[str, np.ndarray]] = {}
        total_seen:     dict[Any, int]                  = {}

        for batch in tqdm(self._batches(needed), desc="Sampling (stratified)"):
            df_batch = batch.to_pandas()
            if df_batch.empty or stratify_col not in df_batch.columns:
                continue

            for g, group_df in df_batch.groupby(
                df_batch[stratify_col].fillna("__NA__"), sort=False
            ):
                group_df   = group_df.reset_index(drop=True)
                group_size = len(group_df)

                if g not in total_seen:
                    total_seen[g]   = 0
                    fill_chunks[g]  = []
                    filled[g]       = 0

                # Fill phase: see get_random_sample's fill phase, same
                # pattern, once per stratify group.
                if filled[g] < n_per_group:
                    take = min(n_per_group - filled[g], group_size)
                    fill_chunks[g].append(group_df.iloc[:take])
                    filled[g]      += take
                    total_seen[g]  += take

                    if filled[g] == n_per_group:
                        filled_df = pd.concat(fill_chunks[g], ignore_index=True)
                        reservoir_cols[g] = {
                            c: filled_df[c].to_numpy(copy=True) for c in filled_df.columns
                        }
                        fill_chunks[g] = []

                    if take == group_size:
                        continue

                    group_df   = group_df.iloc[take:].reset_index(drop=True)
                    group_size = len(group_df)

                # Replacement phase: vectorized slot selection via Vitter's Algorithm R
                positions  = np.arange(total_seen[g], total_seen[g] + group_size)
                rand_slots = self.rng.rng.integers(0, positions + 1)
                self._apply_reservoir_replacements(
                    reservoir_cols[g], group_df, rand_slots, n_per_group
                )

                total_seen[g] += group_size

        reservoirs: dict[Any, pd.DataFrame] = {
            g: pd.DataFrame(cols) for g, cols in reservoir_cols.items()
        }
        for g, chunks in fill_chunks.items():
            if chunks and g not in reservoirs:
                reservoirs[g] = pd.concat(chunks, ignore_index=True)

        if not reservoirs:
            return pd.DataFrame()

        sample    = pd.concat(list(reservoirs.values()), ignore_index=True)
        keep_cols = [c for c in self._gdelt_columns_ordered if c in sample.columns]
        return cast(pd.DataFrame, sample.loc[:, keep_cols])
