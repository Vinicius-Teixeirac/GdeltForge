"""
samplers.py

Sampling utilities for GDELT parquet datasets.
Provides:
    - IndexedSampler
    - CalendarSampler
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
from gdeltforge.utils.io import clearer_dataset_errors, narrow_to_available_columns
from gdeltforge.utils.logging import get_logger

from . import cameo_codes
from .indexer import FileIndex
from .rng import ReproducibleRNG

logger = get_logger(__name__)


# ----------------------------------------------------------
# Shared dataset discovery/scan helpers
# ----------------------------------------------------------
# Used by both CalendarSampler and FilteredSampler (via its own _dataset
# wrapper below), so a period-grouped calendar sample and a filtered/
# stratified sample scan the flat + historical file union the same way,
# rather than risking two subtly different implementations of the same
# file-discovery and schema-union logic.
def _discover_dataset_files(
    folder: Path,
    historical_folder: Path | None,
    start_date: date | None,
    end_date: date | None,
    date_parser: Callable[[str], tuple[date | None, date | None]],
) -> list[Path]:
    flat_files = list(folder.glob("*.parquet"))
    hist_files = (
        list(historical_folder.rglob("*.parquet"))
        if historical_folder and historical_folder.exists()
        else []
    )
    all_files = flat_files + hist_files
    all_files = filter_paths_by_date(all_files, start_date, end_date, date_parser=date_parser)

    if not all_files:
        raise FileNotFoundError(
            f"No parquet files found in {folder}"
            + (f" or {historical_folder}" if historical_folder else "")
            + (f" within [{start_date} - {end_date}]" if start_date or end_date else "")
        )
    return all_files


def _scan_dataset(files: list[Path]) -> pl.LazyFrame:
    # polars' scan_parquet infers its schema from a single file rather
    # than the union of every file's schema the way pyarrow.dataset.
    # dataset() does, confirmed directly: a column present in some files
    # and absent from others (e.g. a historical partition file converted
    # under an older, narrower output_columns) raises "extra column ...
    # outside of expected schema" for whichever file doesn't match the
    # schema scan_parquet happened to infer, regardless of file order.
    # Reading every file's own footer schema first (cheap, metadata-only,
    # the same order of cost pyarrow.dataset's own schema unification
    # already paid) and passing the union explicitly as schema=
    # reproduces that unification regardless of which file scan_parquet
    # would otherwise have picked. Predicate pushdown on Year / MonthYear
    # still works via row-group statistics: each historical partition
    # file has constant values for those columns, so non-matching files
    # are skipped without reading any row data.
    schema: dict[str, pl.DataType] = {}
    for f in files:
        for name, dtype in pl.read_parquet_schema(f).items():
            schema.setdefault(name, dtype)
    return pl.scan_parquet(files, schema=schema, missing_columns="insert")


# ----------------------------------------------------------
# Shared reservoir-sampling mechanics
# ----------------------------------------------------------
# Module-level rather than methods on any one sampler class: CalendarSampler
# and FilteredSampler (get_random_sample/get_stratified_sample) both need
# exactly this machinery, grouped by different kinds of keys, so they share
# one implementation of Vitter's Algorithm R instead of risking a second,
# subtly different one.
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
    survive: np.unique on the reversed array picks exactly that, since
    target_slots is already in ascending position order.
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


def _assign_column(arr: np.ndarray, idx: np.ndarray, values: np.ndarray) -> np.ndarray:
    """
    Write values into arr at idx, positionally, returning the (possibly
    new) array to write back into the caller's dict.

    A batch row can carry a NaN in a column that's been int64 so far
    (nullable numeric GDELT fields; polars' own nullable Int64 upcasts to
    float64+NaN the same way through .to_numpy()). Checked this against
    plain numpy assignment directly: numpy does NOT raise here --
    `int_arr[idx] = nan_values` silently casts NaN to INT64_MIN with only
    a RuntimeWarning, so a try/except around the assignment can't catch
    it. Compute the correct common dtype up front instead (via
    np.result_type, e.g. int64+float64 -> float64) and upcast before ever
    writing, rather than reacting to a write that already corrupted data.
    """
    target_dtype = np.result_type(arr.dtype, values.dtype)
    if target_dtype != arr.dtype:
        arr = arr.astype(target_dtype)
    arr[idx] = values
    return arr


def _apply_reservoir_replacements(
    reservoir_cols: dict[str, np.ndarray],
    batch: pl.DataFrame,
    rand_slots: np.ndarray,
    capacity: int,
) -> None:
    """
    Write accepted rows into their drawn reservoir slots, in place, column
    by column as plain numpy arrays rather than through a DataFrame-level
    positional setter (roughly 15x cheaper per write on GDELT's ~58-column
    width than going through a DataFrame's own positional setter, which is
    what dominates a full-archive scan's wall-clock time). The reservoir
    is only turned back into a DataFrame once, at the very end of the scan.
    """
    target_slots, source_pos = _dedup_last_write_per_slot(rand_slots, capacity)
    if target_slots.size == 0:
        return

    # DataFrame.gather() doesn't exist until a polars release newer than
    # this project's own declared minimum (polars>=1.34): confirmed
    # directly, installing exactly 1.34.0 raises AttributeError here.
    # Bracket indexing with an integer array selects the same rows and
    # has been stable across every polars 1.x release.
    accepted = batch[source_pos]
    for col, arr in reservoir_cols.items():
        reservoir_cols[col] = _assign_column(arr, target_slots, accepted[col].to_numpy())


def _reservoir_to_dataframe(
    reservoir_cols: dict[str, np.ndarray], schema: dict[str, pl.DataType]
) -> pl.DataFrame:
    """
    Rebuilds a DataFrame from a reservoir's plain numpy arrays (see
    _apply_reservoir_replacements above for why the reservoir is kept as
    numpy arrays rather than a DataFrame during the scan itself).

    Only a numpy *object* array (what .to_numpy() gives back for any
    polars string column) is actually ambiguous: one that happens to be
    entirely None at reconstruction time, e.g. a small per-period/per-
    group reservoir where every sampled row's value for that column was
    genuinely null, can't be confidently inferred as Utf8 from its
    content alone, so polars falls back to pl.Object for it instead.
    Confirmed directly: this is invisible for a single reservoir
    considered on its own, but concatenating it against another
    reservoir/chunk where the same column correctly inferred as Utf8
    then fails with a SchemaError ("... incompatible with expected type
    String") during the vstack concat performs internally, non-
    deterministically depending on which period/group happened to draw
    an all-null slice for that column.

    pl.DataFrame(reservoir_cols, schema=schema), passing schema back for
    every column uniformly, looks like the obvious fix, and resolves
    that case, but has two real problems, both confirmed directly:
      - Passing schema= alongside a numpy OBJECT array doesn't construct
        the Series as that dtype directly: polars infers a dtype from
        the array's own content first (String, most of the time) and
        only casts to the requested dtype when that guess disagrees,
        and a cast from Object to String isn't implemented at all, so
        the run fails outright on whichever reservoir polars' inference
        happens to land on Object for, instead of just failing to
        vstack later.
      - Forcing the schema back onto a NUMERIC column is actively wrong,
        not just unnecessary: _apply_reservoir_replacements' own
        _assign_column upcasts a reservoir column's real dtype mid-scan
        (Int64 -> Float64) the moment a null needs writing into it, so
        the dtype captured once at fill time can already be stale by
        reconstruction time. Forcing that stale Int64 back onto an
        array that has since become float64-with-real-NaNs raises
        (float64 NaN has no valid Int64 representation to cast to)
        instead of reconstructing the column that upcast correctly
        produced.

    Only an object-dtype array is ambiguous enough to need help at all:
    a numeric numpy array's own dtype already uniquely determines the
    right polars type (int64/float64 map to Int64/Float64 with nothing
    to guess), exactly the reasoning _assign_column's own docstring
    gives for using numpy here in the first place, so that path is left
    exactly as fast and schema-free as it was before this function
    existed. Only the ambiguous case pays any extra cost, and even
    there going through a plain Python list is not the slow choice it
    looks like: a numpy *object* array is already just boxed Python
    string/None pointers, not a contiguous native buffer the way a
    numeric array is, so there's no real vectorization to lose. Measured
    directly against a 100,000-row object column: converting through
    tolist() first is faster than bridging through pyarrow instead
    (pa.array(arr, type=pa.string())), not slower.

    Two more consequences of that same Int64 -> Float64 upcast, both
    confirmed directly and both fixed at the two call sites that combine
    more than one of this function's own outputs (CalendarSampler.
    get_calendar_samples, FilteredSampler.get_stratified_sample), not
    here:
      - Two periods/groups can upcast independently of each other: one
        whose reservoir never needed to write a null into that column
        stays Int64, a sibling whose reservoir did becomes Float64, and
        a plain pl.concat of the two raises ("type Int64 is incompatible
        with expected type Float64") since vertical concat requires
        identical schemas. Their own pl.concat calls pass
        how="vertical_relaxed" to reconcile this instead of erroring.
      - pl.Series(c, arr) alone does NOT turn the upcast's real NaN
        placeholders back into actual polars nulls: nan_to_null=True
        below is what does that. Skipping it silently changes what the
        column means, not just its dtype, since a plain reservoir with
        no cross-period concat involved would otherwise return
        null_count() == 0 for a column whose source data genuinely had
        a null in it, invisible until something downstream checks
        is_null() and gets nothing back for a row is_nan() would.
    """
    return pl.DataFrame({
        c: pl.Series(c, arr.tolist(), dtype=schema[c]) if arr.dtype == object
        else pl.Series(c, arr, nan_to_null=True)
        for c, arr in reservoir_cols.items()
    })


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
            # See _apply_reservoir_replacements above: DataFrame.gather()
            # isn't available on this project's declared minimum polars
            # version, bracket indexing is.
            sampled.append(df[relative_rows])

        return pl.concat(sampled)


# ----------------------------------------------------------
# Calendar Sampler
# ----------------------------------------------------------
class CalendarSampler:
    """
    Groups rows by a calendar period (day, month, or year) derived from a
    real date column, then reservoir-samples a fixed number of rows per
    period across the flat + historical file union in a single streamed
    scan.

    Replaces the old DailySampler, which capped samples_per_day per
    (file, day) independently, then concatenated across files -- correct
    only when a period maps to exactly one file (true for Events'/GKG
    1.0's flat daily archives), wrong the moment a period spans more than
    one file: a historical Year=YYYY file, or GKG 2.1/Mentions' own
    roughly-96-files-per-day cadence, would each independently contribute
    up to the cap, multiplying the true per-period total by however many
    files happen to cover it. Reservoir sampling over the true per-period
    group, scanned across every contributing file together, makes the cap
    correct regardless of how many files a period's rows are spread
    across.

    When historical_folder is provided the sampler includes Hive-partitioned
    historical files alongside the flat daily files.
    """

    _PERIOD_KEY = "__calendar_period_key__"
    # Length of the YYYYMMDD.../YYYYMMDDHHMMSS... date-column prefix that
    # identifies each period: works for both GDELT's 8-digit plain-date
    # columns (Day, GKG 1.0's Date) and 14-digit timestamp columns (GKG
    # 2.1's V2.1DATE, Mentions' MentionTimeDate) uniformly, since both
    # start with the same YYYY[MM[DD]] prefix.
    _PERIOD_PREFIX_LENGTH = {"day": 8, "month": 6, "year": 4}

    def __init__(
        self,
        folder_path: str,
        historical_folder: str | None = None,
        random_state: int | None = 42,
        columns: set[str] | None = None,
        date_column: str = "Day",
        period: str = "day",
        start_date: date | None = None,
        end_date: date | None = None,
        date_parser: Callable[[str], tuple[date | None, date | None]] = parse_file_date,
    ):
        if period not in self._PERIOD_PREFIX_LENGTH:
            raise ValueError(
                f"period must be one of {sorted(self._PERIOD_PREFIX_LENGTH)}, got {period!r}"
            )
        self.folder = Path(folder_path)
        self.historical_folder: Path | None = (
            Path(historical_folder) if historical_folder else None
        )
        self.columns = columns
        self.date_column = date_column
        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.date_parser = date_parser
        self.rng = ReproducibleRNG(random_state)

    def _batches(self, needed_columns: list[str] | None):
        with clearer_dataset_errors(f"calendar sample dataset in {self.folder}"):
            files = _discover_dataset_files(
                self.folder, self.historical_folder,
                self.start_date, self.end_date, self.date_parser,
            )
            lf = _scan_dataset(files)
            if needed_columns is not None:
                lf = lf.select(needed_columns)
            yield from lf.collect_batches(chunk_size=64_000)

    def get_calendar_samples(self, samples_per_period: int = 10) -> pl.DataFrame:
        if samples_per_period <= 0:
            return pl.DataFrame()

        prefix_len = self._PERIOD_PREFIX_LENGTH[self.period]

        # A dataset scan built over files with differing schemas fills a
        # column absent from SOME of them with null when it's present in
        # at least one (missing_columns="insert" on _scan_dataset), so a
        # single stray file missing date_column doesn't need special
        # handling here: those rows just come back null and get counted
        # as unparseable below. But date_column absent from EVERY file
        # isn't in the union schema at all, and each batch's own "column
        # not in df_batch.columns" check below would then just skip every
        # batch silently, the same silent-skip flaw this class replaces
        # DailySampler specifically to fix. Checked explicitly here
        # instead, so a misconfigured --date-column fails with a message
        # about the actual problem rather than a quietly empty result.
        with clearer_dataset_errors(f"calendar sample dataset in {self.folder}"):
            files = _discover_dataset_files(
                self.folder, self.historical_folder,
                self.start_date, self.end_date, self.date_parser,
            )
            available = set(_scan_dataset(files).collect_schema().names())
        if self.date_column not in available:
            raise ValueError(
                f"{self.date_column!r} is not a column in any file under {self.folder}"
                + (f" or {self.historical_folder}" if self.historical_folder else "")
                + ". Check --date-column (or the per-dataset default) against this "
                  "dataset's real schema."
            )

        # date_column drives the grouping below, so it has to be read even
        # if the caller didn't ask for it in --columns, otherwise every
        # file would silently look like it has no date column and get
        # skipped; narrow_to_available_columns treats it the same strict
        # way as a genuine required column for that reason (already
        # confirmed present just above, so this can never raise on it).
        # self.columns being unset (no explicit --columns) needs no
        # narrowing at all: _batches leaves the projection off entirely
        # and just reads whatever a file actually has, the same as it
        # always did before this existed.
        needed = (
            narrow_to_available_columns(
                logger, f"calendar sample dataset in {self.folder}",
                self.columns, {self.date_column}, available,
            )
            if self.columns else None
        )

        fill_chunks:      dict[Any, list[pl.DataFrame]]      = {}
        filled:           dict[Any, int]                     = {}
        reservoir_cols:   dict[Any, dict[str, np.ndarray]]   = {}
        reservoir_schema: dict[Any, dict[str, pl.DataType]]  = {}
        total_seen:       dict[Any, int]                     = {}
        n_unparseable = 0

        for df_batch in tqdm(self._batches(needed), desc="Sampling (calendar)"):
            if df_batch.is_empty() or self.date_column not in df_batch.columns:
                continue

            keyed_batch = df_batch.with_columns(
                pl.col(self.date_column).cast(pl.Utf8).str.slice(0, prefix_len)
                .alias(self._PERIOD_KEY)
            )
            n_unparseable += keyed_batch[self._PERIOD_KEY].null_count()

            # Unlike get_stratified_sample's fillna("__NA__") (which keeps
            # a null stratum as its own real group), a row whose date
            # can't be resolved to a period has nothing meaningful to be
            # grouped under, so it's dropped here (counted above instead)
            # rather than sampled as if "unparseable" were itself a
            # calendar period. Polars' own group_by, unlike pandas'
            # groupby's dropna=True default, keeps a null key as its own
            # group, so this has to be explicit rather than assumed.
            keyed_batch = keyed_batch.drop_nulls(subset=[self._PERIOD_KEY])

            for (period_key,), group_df in keyed_batch.group_by(
                self._PERIOD_KEY, maintain_order=False
            ):
                group_df   = group_df.drop([self._PERIOD_KEY])
                group_size = len(group_df)

                if period_key not in total_seen:
                    total_seen[period_key]  = 0
                    fill_chunks[period_key] = []
                    filled[period_key]      = 0

                # Fill phase: see FilteredSampler.get_random_sample's fill
                # phase, same pattern, once per calendar period.
                if filled[period_key] < samples_per_period:
                    take = min(samples_per_period - filled[period_key], group_size)
                    fill_chunks[period_key].append(group_df.head(take))
                    filled[period_key]     += take
                    total_seen[period_key] += take

                    if filled[period_key] == samples_per_period:
                        filled_df = pl.concat(fill_chunks[period_key])
                        reservoir_schema[period_key] = dict(filled_df.schema)
                        reservoir_cols[period_key] = {
                            c: filled_df[c].to_numpy(writable=True) for c in filled_df.columns
                        }
                        fill_chunks[period_key] = []

                    if take == group_size:
                        continue

                    group_df   = group_df.slice(take)
                    group_size = len(group_df)

                # Replacement phase: vectorized slot selection via Vitter's Algorithm R.
                positions  = np.arange(total_seen[period_key], total_seen[period_key] + group_size)
                rand_slots = self.rng.rng.integers(0, positions + 1)
                _apply_reservoir_replacements(
                    reservoir_cols[period_key], group_df, rand_slots, samples_per_period
                )

                total_seen[period_key] += group_size

        if n_unparseable:
            logger.warning(
                f"{n_unparseable} row(s) with an unparseable {self.date_column} "
                f"were dropped from calendar sampling."
            )

        reservoirs: dict[Any, pl.DataFrame] = {
            g: _reservoir_to_dataframe(cols, reservoir_schema[g])
            for g, cols in reservoir_cols.items()
        }
        for g, chunks in fill_chunks.items():
            if chunks and g not in reservoirs:
                reservoirs[g] = pl.concat(chunks)

        if not reservoirs:
            return pl.DataFrame()

        # vertical_relaxed, not the plain vertical default: each period's
        # reservoir is typed independently by _reservoir_to_dataframe, and
        # _apply_reservoir_replacements/_assign_column upcasts a numeric
        # column from Int64 to Float64 the moment a null lands in THAT
        # period's own reservoir. Two periods can legitimately disagree on
        # the same column's dtype this way, one Int64 (no null ever drawn
        # into it), the other Float64 (one was), with nothing wrong in
        # either reservoir on its own; plain vertical concat then raises
        # ("type Int64 is incompatible with expected type Float64")
        # instead of reconciling them. Confirmed directly against
        # events-reduced's SourceGeoType/TargetGeoType columns, which hit
        # this deterministically at real scale.
        return pl.concat(list(reservoirs.values()), how="vertical_relaxed")


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
        files = _discover_dataset_files(
            self.folder, self.historical_folder,
            self.start_date, self.end_date, self.date_parser,
        )
        return _scan_dataset(files)

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

    def _needed_columns(self, extra_required: set[str] | None = None) -> list[str]:
        """
        self.columns (the output projection, defaulting to this dataset's
        full declared schema when --columns isn't passed) and any column
        the filter expression itself references, narrowed down to what
        this dataset's real, already-scanned files actually have. See
        narrow_to_available_columns for why that narrowing exists at
        all: a declared schema column isn't guaranteed to survive an
        earlier convert/filter run's own output_columns pruning.

        extra_required names a column beyond the filter's own (e.g.
        get_stratified_sample's stratify_col) that this call can't
        function without at all, so it's checked for real availability
        the same strict way the filter's own columns are, rather than
        being treated as a droppable, output-only request.
        """
        # Wrapped the same as _batches below: _dataset() itself can raise
        # (every file's footer schema is read there), and this runs
        # before _batches ever does, so a corrupt/non-parquet file needs
        # the same clear error here too, not a bare arrow one.
        with clearer_dataset_errors(f"filtered sample dataset in {self.folder}"):
            available = set(self._dataset().collect_schema().names())
        required = (extra_required or set()) | self._filter_columns(self.filter_dict)
        return narrow_to_available_columns(
            logger, f"filtered sample dataset in {self.folder}", self.columns, required, available
        )

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
        reservoir_schema: dict[str, pl.DataType] | None = None
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
                    reservoir_schema = dict(filled_df.schema)
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
            _apply_reservoir_replacements(reservoir_cols, df_batch, rand_slots, n)

            total_seen += batch_size

        reservoir = (
            _reservoir_to_dataframe(reservoir_cols, reservoir_schema)
            if reservoir_cols is not None and reservoir_schema is not None
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

        needed = self._needed_columns(extra_required={stratify_col})

        fill_chunks:      dict[Any, list[pl.DataFrame]]     = {}
        filled:           dict[Any, int]                    = {}
        reservoir_cols:   dict[Any, dict[str, np.ndarray]]  = {}
        reservoir_schema: dict[Any, dict[str, pl.DataType]] = {}
        total_seen:       dict[Any, int]                    = {}

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
                        reservoir_schema[g] = dict(filled_df.schema)
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
                _apply_reservoir_replacements(
                    reservoir_cols[g], group_df, rand_slots, n_per_group
                )

                total_seen[g] += group_size

        reservoirs: dict[Any, pl.DataFrame] = {
            g: _reservoir_to_dataframe(cols, reservoir_schema[g])
            for g, cols in reservoir_cols.items()
        }
        for g, chunks in fill_chunks.items():
            if chunks and g not in reservoirs:
                reservoirs[g] = pl.concat(chunks)

        if not reservoirs:
            return pl.DataFrame()

        # vertical_relaxed: see CalendarSampler.get_calendar_samples' own
        # identical concat for why a plain vertical concat can raise here,
        # one stratify group's reservoir Int64 for a column, a sibling
        # group's Float64 for the same column, both correct on their own.
        sample    = pl.concat(list(reservoirs.values()), how="vertical_relaxed")
        keep_cols = [c for c in self._gdelt_columns_ordered if c in sample.columns]
        return sample.select(keep_cols)
