"""
filter.py

Filtering utilities for GDELT Parquet datasets.

The GDELTFilter class drops rows with NaN values in specified columns. For example,
a user may wish to keep only events where both actors are identified. In that case,
they can define in `settings.yaml`:

filter:
  columns_to_check:
    - Actor1Code
    - Actor2Code

The GDELTFilter class will then remove all rows where any of the specified columns
contain NaN.

Optionally, a dataset can also be projected down to a subset of columns on write,
given its own compression codec, and have specific float64 columns narrowed to
float32, independent of row-filtering:

filter:
  output_columns:
    gdelt_gkg_v2:
      - GKGRECORDID
      - V2.1DATE
      - V2DOCUMENTIDENTIFIER
      - V1THEMES
  compression:
    gdelt_gkg_v2: zstd
  float32_columns:
    gdelt_event:
      - Actor1Geo_Lat
      - Actor1Geo_Long

output_columns and float32_columns are opt-in per dataset; omitting them keeps
every column at full float64 precision. compression defaults to zstd (measured
roughly 30% smaller than snappy on real GDELT data, at comparable or faster
write speed, with no precision impact since it is lossless), overridable per
dataset the same way.

float32_columns is a real precision change, not just a smaller encoding: real
GDELT float columns routinely carry more significant figures than float32 can
hold (AvgTone alone has been observed with 15, well past float32's ~7), so
casting a column here means values it holds will measurably change, not just
compress smaller. Only use it for columns where that tradeoff is acceptable
for your use case.

When converter.partitioning.enabled is true the historical Hive-partitioned dataset
(under parquet_historical_directory) is filtered in addition to the flat daily files.
The Hive directory structure is preserved in filtered_historical_directory.

Provides:
    - GDELTFilter: main filtering class
    - run_filter: wrapper that calls the main method `filter_all_files`
"""

import glob
import logging
import os
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from tqdm import tqdm

from gdeltforge.crossref.crossref import warn_if_output_columns_drops_join_key
from gdeltforge.scraping.scraper import date_parser_for, filter_paths_by_date, parse_file_date
from gdeltforge.utils.config import dataset_path_key
from gdeltforge.utils.io import (
    config_fingerprint,
    is_marked_done,
    mark_done,
    warn_if_delete_source_drops_recoverable_data,
)
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)


class GDELTFilter:
    """
    Filters Parquet files by removing rows with NaN in specified columns.
    Handles both flat daily files and Hive-partitioned historical files.

    A file already filtered under the exact same columns_to_check,
    output_columns, float32_columns, and compression is skipped on a
    resumed run (see .done markers in utils.io); a run started after any
    of those changed reprocesses every file instead of serving output
    shaped by the old configuration.
    """

    def __init__(
        self,
        input_folder: str,
        output_folder: str,
        columns_to_check: list[str],
        historical_input_folder: str | None = None,
        historical_output_folder: str | None = None,
        max_workers: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        date_parser: Callable[[str], tuple[date | None, date | None]] = parse_file_date,
        output_columns: list[str] | None = None,
        compression: str = "zstd",
        float32_columns: list[str] | None = None,
        delete_source: bool = False,
        verbose: bool = False,
        quiet: bool = False,
        force: bool = False,
        dry_run: bool = False,
    ):
        self.input_folder  = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.columns_to_check = columns_to_check
        # Optional column projection applied after row-filtering, so wide
        # datasets (GKG's free-text fields in particular) don't have to be
        # written out in full just to reach the columns a downstream
        # consumer actually reads. None keeps every column, matching the
        # behavior before this existed.
        self.output_columns = output_columns
        # zstd default: measured roughly 30% smaller than snappy on real
        # GDELT data at comparable or faster write speed, and it is lossless
        # (unlike float32_columns below), so there is no accuracy tradeoff
        # to weigh before defaulting to it.
        self.compression = compression
        # Optional, per dataset: narrows these float64 columns to float32 on
        # write. Off by default (None), and stays off unless explicitly
        # configured: this is a real precision change, not free compression.
        # Real GDELT float columns (AvgTone in particular) have been
        # observed with up to 15 significant figures, well past what
        # float32's ~7 can represent, so this measurably changes values,
        # it does not just store the same ones more compactly.
        self.float32_columns = float32_columns
        # Off by default: deletes the source (unfiltered, converted)
        # parquet once its filtered output is written and marked done, so
        # a full historical pull doesn't need to hold both the converted
        # and filtered copies at once. A caller explicitly opts in per run
        # (CLI: --delete-source), same shape as start_date/end_date rather
        # than a persistent settings.yaml value, since it's a deliberate
        # one-off choice about this particular run. Also means whatever
        # this run's own columns_to_check/output_columns/float32_columns
        # narrowed away is gone unless an earlier stage is redone -- see
        # warn_if_delete_source_drops_recoverable_data in run_filter.
        self.delete_source = delete_source
        # Stored as real instance attributes, not just a level flipped
        # here and forgotten: filter_single_file runs inside a
        # ProcessPoolExecutor worker, a genuinely separate process that
        # re-imports this module fresh (get_logger sets INFO again,
        # independent of whatever this process just did), so self.verbose/
        # self.quiet travel across the pickle boundary and
        # filter_single_file re-applies whichever is set itself, rather
        # than relying on a level change made here ever reaching the
        # worker. verbose wins if a caller somehow passes both (argparse's
        # mutually exclusive group already prevents that from the CLI).
        self.verbose = verbose
        self.quiet = quiet
        if verbose:
            logger.setLevel(logging.DEBUG)
        elif quiet:
            logger.setLevel(logging.WARNING)
        # force bypasses the is_marked_done check in filter_all_files, so
        # a file already marked done is reprocessed and its output
        # overwritten. dry_run short-circuits filter_all_files after
        # to_process is built, before any worker is submitted.
        self.force = force
        self.dry_run = dry_run

        self.historical_input_folder: Path | None = (
            Path(historical_input_folder) if historical_input_folder else None
        )
        self.historical_output_folder: Path | None = (
            Path(historical_output_folder) if historical_output_folder else None
        )
        # None is a valid value here: ProcessPoolExecutor treats
        # max_workers=None as "use os.cpu_count()" on its own.
        self.max_workers = max_workers
        # GDELTFilter stays dataset-agnostic (it never sees a dataset name,
        # only already-resolved paths/columns -- see run_filter below), so
        # the caller resolves which filename convention date_parser needs
        # to understand rather than GDELTFilter guessing from a dataset it
        # doesn't have.
        self.start_date = start_date
        self.end_date = end_date
        self.date_parser = date_parser

        # Determines whether a .done marker from a previous run is still
        # valid: these are exactly the settings a user plausibly iterates
        # on between runs, and each one changes what the filtered output
        # actually contains (which rows survive, which columns are kept,
        # which are narrowed to float32, how it's compressed on disk). A
        # marker written under different values must not cause this run
        # to skip reprocessing that file and silently serve output shaped
        # by the old configuration.
        self._config_fingerprint = config_fingerprint(
            columns_to_check=self.columns_to_check,
            output_columns=self.output_columns,
            float32_columns=self.float32_columns,
            compression=self.compression,
        )

        self.output_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f"Filter output folder ensured: {self.output_folder}")

        if self.historical_output_folder:
            self.historical_output_folder.mkdir(parents=True, exist_ok=True)
            logger.info(
                f"Historical filter output folder ensured: {self.historical_output_folder}"
            )

    # ======================================================================
    # PUBLIC API
    # ======================================================================

    def filter_all_files(self, pattern: str = "*.parquet") -> tuple[int, int]:
        """
        Filter all parquet files in input_folder (flat) and, if configured,
        all parquet files under historical_input_folder (Hive tree).
        """
        flat_files = glob.glob(str(self.input_folder / pattern))
        historical_files = (
            list(self.historical_input_folder.rglob("*.parquet"))
            if self.historical_input_folder and self.historical_input_folder.exists()
            else []
        )

        flat_files = filter_paths_by_date(
            flat_files, self.start_date, self.end_date, date_parser=self.date_parser
        )
        historical_files = filter_paths_by_date(
            historical_files, self.start_date, self.end_date, date_parser=self.date_parser
        )

        all_files = [(Path(p), False) for p in flat_files] + \
                    [(p, True) for p in historical_files]

        if not all_files:
            logger.warning(
                f"No parquet files found in: {self.input_folder}"
                + (f" or {self.historical_input_folder}" if self.historical_input_folder else "")
            )
            return 0, 0

        to_process = []
        for parquet_path, is_historical in all_files:
            if not self.force and is_marked_done(parquet_path, self._config_fingerprint):
                logger.debug(f"Skipping already filtered: {parquet_path.name}")
                continue
            to_process.append((parquet_path, is_historical))

        if not to_process:
            logger.info("Nothing to filter; all files already processed.")
            return 0, 0

        if self.dry_run:
            flat_preview = sum(1 for _, is_hist in to_process if not is_hist)
            historical_preview = len(to_process) - flat_preview
            logger.info(
                f"[dry run] Would filter {flat_preview} flat file(s) "
                f"and {historical_preview} historical file(s):"
            )
            for parquet_path, _ in to_process:
                logger.debug(f"[dry run]   {parquet_path.name}")
            return 0, 0

        flat_to_process = sum(1 for _, is_hist in to_process if not is_hist)
        historical_to_process = len(to_process) - flat_to_process
        logger.info(
            f"Filtering {flat_to_process} flat file(s) "
            f"and {historical_to_process} historical file(s) using "
            f"{self.max_workers or os.cpu_count() or '?'} worker process(es)..."
        )

        total_rows_before = 0
        total_rows_after  = 0
        files_processed   = 0
        files_failed      = 0

        # Each file is filtered independently (its own read, own output
        # path), so file-level parallelism across processes is safe --
        # this is CPU-bound (per-batch pandas dropna + parquet write), so
        # ProcessPoolExecutor beats threads here, matching GDELTConverter's
        # identical reasoning for process_all_files.
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self.filter_single_file,
                    parquet_path,
                    self._output_path_for(parquet_path, is_historical),
                ): parquet_path
                for parquet_path, is_historical in to_process
            }

            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Filtering parquet files"
            ):
                parquet_path = futures[future]
                try:
                    rows_before, rows_after = future.result()
                    mark_done(parquet_path, self._config_fingerprint)

                    if self.delete_source:
                        self._delete_source(parquet_path)

                    total_rows_before += rows_before
                    total_rows_after  += rows_after
                    files_processed   += 1

                    rate = (rows_after / rows_before * 100) if rows_before else 0
                    # DEBUG, not INFO: unconditional, once per file, same
                    # rationale as convert's equivalent per-file lines --
                    # see run_filter's verbose docstring.
                    logger.debug(
                        f"{parquet_path.name}: "
                        f"{rows_before:,} -> {rows_after:,} rows ({rate:.1f}% kept)"
                    )

                except Exception as e:
                    files_failed += 1
                    logger.error(f"Failed to filter {parquet_path.name}: {e}")

        logger.info("===============================================")
        logger.info("FILTERING SUMMARY")
        logger.info("===============================================")
        logger.info(f"Files processed successfully: {files_processed}")
        logger.info(f"Files failed: {files_failed}")
        logger.info(f"Total rows before: {total_rows_before:,}")
        logger.info(f"Total rows after: {total_rows_after:,}")

        if total_rows_before > 0:
            dropped    = total_rows_before - total_rows_after
            retention  = total_rows_after / total_rows_before * 100
            logger.info(f"Overall retention rate: {retention:.2f}%")
            logger.info(f"Total rows removed: {dropped:,}")

        return files_processed, files_failed

    # ======================================================================
    # PER-FILE PROCESSING
    # ======================================================================

    def filter_single_file(
        self,
        parquet_path: str | Path,
        output_path: Path | None = None,
    ) -> tuple[int, int]:
        """
        Filter a single parquet file and return (rows_before, rows_after).
        Streams the file in batches to keep peak RAM bounded, and writes
        through a temp file + atomic rename so a worker process killed
        mid-write leaves nothing at output_path rather than a truncated
        file, matching the pattern already used for converter output.

        output_path overrides the default flat naming convention; used to
        preserve Hive subdirectory structure for historical files.
        """
        # Re-applied here, not just in __init__: this method runs inside
        # a ProcessPoolExecutor worker, a genuinely separate process that
        # re-imports this module fresh (get_logger sets INFO again), so
        # __init__'s own logger.setLevel call, made in the main process,
        # never reaches it. self.verbose/self.quiet survive the pickle
        # boundary fine; the logger's mutated level does not.
        if self.verbose:
            logger.setLevel(logging.DEBUG)
        elif self.quiet:
            logger.setLevel(logging.WARNING)

        file_path = Path(parquet_path)
        logger.debug(f"Filtering file: {file_path.name}")

        pf = pq.ParquetFile(file_path)

        if pf.metadata.num_rows == 0:
            logger.warning(f"Empty parquet file skipped: {file_path.name}")
            return 0, 0

        schema_cols = pf.schema_arrow.names
        existing_columns = [c for c in self.columns_to_check if c in schema_cols]
        missing_columns  = [c for c in self.columns_to_check if c not in schema_cols]

        if missing_columns:
            logger.warning(
                f"{file_path.name}: Missing {len(missing_columns)} column(s): {missing_columns}"
            )

        # An empty columns_to_check (the bundled default config ships this
        # for every dataset) is a deliberate no-op, not an error: dropna
        # against an empty column list is a documented no-op, so every row
        # is meant to survive. That's a different case from columns_to_check
        # being non-empty but matching nothing in this file's schema, which
        # genuinely can't be checked and bails out below without writing.
        if self.columns_to_check and not existing_columns:
            logger.error(f"{file_path.name}: None of the filter columns exist.")
            return pf.metadata.num_rows, pf.metadata.num_rows

        if output_path is None:
            output_path = self.output_folder / f"{file_path.stem}_filtered.parquet"

        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = output_path.with_name(output_path.name + ".tmp")

        # Same existing/missing split as columns_to_check above: a
        # configured output column that isn't in this file's schema is
        # dropped rather than treated as fatal, since schemas can drift
        # release to release the same way they already can for row-filters.
        keep_columns = (
            [c for c in self.output_columns if c in schema_cols]
            if self.output_columns is not None
            else None
        )
        # Same existing/missing split as columns_to_check and output_columns
        # above, plus this only applies to columns that are actually
        # floating-point in this file's schema: a configured column that
        # doesn't exist, or exists but isn't a float (e.g. a stale config
        # pointed at a renamed/retyped column), is skipped rather than
        # treated as fatal.
        float32_cols = (
            [
                c for c in self.float32_columns
                if c in schema_cols and pa.types.is_floating(pf.schema_arrow.field(c).type)
            ]
            if self.float32_columns is not None
            else []
        )

        write_schema = (
            pf.schema_arrow
            if keep_columns is None
            else pa.schema([pf.schema_arrow.field(c) for c in keep_columns])
        )
        if float32_cols:
            write_schema = pa.schema([
                pa.field(f.name, pa.float32()) if f.name in float32_cols else f
                for f in write_schema
            ])

        rows_before = 0
        rows_after  = 0

        writer = pq.ParquetWriter(tmp_path, write_schema, compression=self.compression)
        try:
            for batch in pf.iter_batches(batch_size=64_000):
                df_batch = batch.to_pandas()
                rows_before += len(df_batch)

                df_clean = df_batch.dropna(subset=existing_columns)
                rows_after += len(df_clean)

                if not df_clean.empty:
                    table = pa.Table.from_pandas(df_clean, preserve_index=False)
                    if keep_columns is not None:
                        table = table.select(keep_columns)
                    for c in float32_cols:
                        if c in table.column_names:
                            table = table.set_column(
                                table.column_names.index(c), c,
                                pc.cast(table.column(c), pa.float32()),
                            )
                    writer.write_table(table)
        except Exception:
            writer.close()
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        else:
            writer.close()
            os.replace(tmp_path, output_path)

        logger.debug(f"Saved filtered file -> {output_path}")
        return rows_before, rows_after

    # ======================================================================
    # VALIDATION
    # ======================================================================

    def validate_columns(
        self, sample_file: str | None = None
    ) -> dict[str, str | int | list[str]]:
        """
        Check if required columns exist in a sample parquet file.
        """
        if sample_file is None:
            files = glob.glob(str(self.input_folder / "*.parquet"))
            if not files:
                return {"error": "No parquet files found for validation."}
            sample_file = files[0]

        sample_path = Path(sample_file)
        logger.info(f"Validating column presence in: {sample_path.name}")

        try:
            schema_cols = pq.read_schema(sample_path).names
            existing = [c for c in self.columns_to_check if c in schema_cols]
            missing  = [c for c in self.columns_to_check if c not in schema_cols]

            return {
                "sample_file": sample_path.name,
                "total_expected_columns": len(self.columns_to_check),
                "existing_columns": existing,
                "missing_columns": missing,
            }

        except Exception as e:
            logger.error(f"Column validation error: {e}")
            return {"error": str(e)}

    # ======================================================================
    # INTERNAL HELPERS
    # ======================================================================

    def _output_path_for(self, parquet_path: Path, is_historical: bool) -> Path:
        """
        Compute the output path for a given input file.

        Flat daily files  -> output_folder/<stem>_filtered.parquet
        Historical files  -> historical_output_folder/<relative_partition_path>/
                             <stem>_filtered.parquet
        """
        if not is_historical:
            return self.output_folder / f"{parquet_path.stem}_filtered.parquet"

        # Only called with is_historical=True for files collected from
        # historical_input_folder, so both are guaranteed set here.
        assert self.historical_input_folder is not None
        assert self.historical_output_folder is not None

        relative = parquet_path.relative_to(self.historical_input_folder)
        return (
            self.historical_output_folder
            / relative.parent
            / f"{parquet_path.stem}_filtered.parquet"
        )

    def _delete_source(self, parquet_path: Path) -> None:
        """
        Delete the source (unfiltered, converted) parquet once its
        filtered output is confirmed written and marked done. Only
        called from the success branch of filter_all_files, never on a
        failed or in-progress filter, so a killed run can't lose an input
        whose filtered output doesn't actually exist yet. A failure here
        (permissions, the file already gone) is logged and swallowed
        rather than counted as a filter failure: the filtering itself
        already succeeded, this is best-effort cleanup on top of it.
        """
        try:
            parquet_path.unlink()
            logger.debug(
                f"Deleted source parquet after successful filtering: {parquet_path.name}"
            )
        except OSError as e:
            logger.warning(f"Could not delete source parquet {parquet_path.name}: {e}")


# ======================================================================
# RUN WRAPPER (used by main.py)
# ======================================================================

def run_filter(
    config: dict,
    dataset: str = "gdelt_event",
    start_date: date | None = None,
    end_date: date | None = None,
    delete_source: bool = False,
    verbose: bool = False,
    quiet: bool = False,
    force: bool = False,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Convenience wrapper so main.py can call the filter cleanly.

    verbose raises this module's own logger to DEBUG, revealing the
    per-file "{name}: rows -> rows"/"Skipping already filtered"/"Deleted
    source parquet" lines that are DEBUG-level (invisible) by default,
    exactly matching scrape's own already-DEBUG per-attempt detail and
    convert's identical treatment of its own per-file lines. Off by
    default: at GKG 2.1/Mentions scale, those lines unconditionally at
    INFO used to mean hundreds of thousands of terminal lines fighting
    the tqdm progress bar below for the screen. quiet is the inverse:
    raises the logger to WARNING, suppressing even the default setup/
    summary INFO lines for scripted or cron use that only cares about
    problems. Mutually exclusive at the CLI; verbose wins if a caller
    passes both directly. Both passed straight through to GDELTFilter
    rather than raised here directly: filter_single_file re-applies
    whichever is set independently inside each ProcessPoolExecutor
    worker, since a level change made in this process never reaches
    those.

    force reprocesses files already marked done instead of skipping
    them. dry_run reports what would be filtered without processing
    anything; it sees force's effect on the skip list, since it runs
    after that check.
    """
    part_cfg = config.get("converter", {}).get("partitioning", {})
    historical_input = historical_output = None

    if part_cfg.get("enabled", False):
        historical_input = config["paths"].get(
            dataset_path_key(dataset, "parquet_historical_directory")
        )
        historical_output = config["paths"].get(
            dataset_path_key(dataset, "filtered_historical_directory")
        )

    columns_to_check = config["filter"]["columns_to_check"][dataset]
    output_columns = config["filter"].get("output_columns", {}).get(dataset)
    float32_columns = config["filter"].get("float32_columns", {}).get(dataset)
    warn_if_output_columns_drops_join_key(logger, "filter", dataset, output_columns)
    warn_if_delete_source_drops_recoverable_data(
        logger, "filter", delete_source,
        narrowing=[
            name for name, value in (
                ("columns_to_check", columns_to_check),
                ("output_columns", output_columns),
                ("float32_columns", float32_columns),
            )
            if value
        ],
    )

    filterer = GDELTFilter(
        input_folder=config["paths"][dataset_path_key(dataset, "parquet_data_directory")],
        output_folder=config["paths"][dataset_path_key(dataset, "filtered_data_directory")],
        columns_to_check=columns_to_check,
        historical_input_folder=historical_input,
        historical_output_folder=historical_output,
        max_workers=config["filter"].get("max_workers"),
        start_date=start_date,
        end_date=end_date,
        date_parser=date_parser_for(dataset),
        output_columns=output_columns,
        compression=config["filter"].get("compression", {}).get(dataset, "zstd"),
        float32_columns=float32_columns,
        delete_source=delete_source,
        verbose=verbose,
        quiet=quiet,
        force=force,
        dry_run=dry_run,
    )
    return filterer.filter_all_files()
