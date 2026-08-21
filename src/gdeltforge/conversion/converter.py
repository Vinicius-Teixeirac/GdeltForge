"""
converter.py

Tools for converting downloaded GDELT data (https://data.gdeltproject.org/events/)
from CSV inside ZIP archives to Parquet format.

All GDELT event files are distributed as compressed ZIP archives containing CSVs, which
are less efficient and less memory-friendly than Parquet for large tabular datasets.

Source files come in four granularities, each identified by filename pattern:
  - Yearly         : 1979.zip, 2005.zip                      (one file per year, up to ~2005)
  - Monthly        : 200601.zip, 201303.zip                  (one file per month, 2006-early 2013)
  - Daily          : 20130401.export.CSV.zip                 (one file per day, April 2013-present)
  - Quarter-hourly : 20150219080000.mentions.CSV.zip          (one file per 15 minutes,
                      GKG 2.1/Mentions, February 2015-present)

When converter.partitioning.enabled is true, a granularity with a matching entry
in converter.partitioning.rules is written as a Hive-partitioned dataset under
parquet_historical_directory (yearly and monthly are the only ones any real
config defines a rule for). Everything else, daily and quarter-hourly included,
goes to parquet_data_directory as flat files, same as when partitioning is off
entirely.

This module provides:
    - GDELTConverter: class responsible for performing file-by-file conversion
    - run_converter: wrapper that calls the main conversion routine `process_all_files`
"""

import glob
import logging
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from gdeltforge.crossref.crossref import warn_if_output_columns_drops_join_key
from gdeltforge.scraping.scraper import date_parser_for, filter_paths_by_date, sort_paths_by_date
from gdeltforge.utils.config import dataset_path_key
from gdeltforge.utils.io import (
    config_fingerprint,
    is_marked_done,
    mark_done,
    unzip_file,
    warn_if_delete_source_drops_recoverable_data,
    write_parquet_atomic,
)
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

# ------------------------------------------------------------------
# Source file-type patterns
# ------------------------------------------------------------------
_YEARLY_PAT         = re.compile(r'^\d{4}\.zip$',           re.IGNORECASE)
_MONTHLY_PAT        = re.compile(r'^\d{6}\.zip$',           re.IGNORECASE)
_DAILY_PAT          = re.compile(r'^\d{8}\..+\.zip$',       re.IGNORECASE)
# YYYYMMDDHHMMSS, GKG 2.1/Mentions' 15-minute cadence. Distinct from
# _DAILY_PAT (8 digits then a literal dot) rather than a variant of it:
# a 14-digit prefix never satisfies "8 digits then a dot", so a real
# quarter-hourly filename matched none of the three original patterns at
# all and _detect_file_type would have returned "unknown" for one, had
# anything ever actually called it on such a name (see process_single_file
# below for why nothing did).
_QUARTER_HOURLY_PAT = re.compile(r'^\d{14}\..+\.zip$',      re.IGNORECASE)

# Columns that are semantically integers; cast from float64 after pd.to_numeric.
# Applies to both flat and historical writes for a consistent union schema.
_DATE_INT_COLS = ("Year", "MonthYear", "Day")

# GKG 1.0 (both the main file and its separate Counts file) ships with a
# literal header line (DATE\tNUMARTS\t...); Events, GKG 2.1, and Mentions
# are all genuinely headerless. Confirmed by downloading and inspecting one
# real file per dataset, not assumed from the codebook/parser source alone.
_DATASETS_WITH_HEADER_ROW = frozenset({"gdelt_gkg_v1", "gdelt_gkg_v1_counts"})


class GDELTConverter:
    """
    Pipeline component: Converts GDELT compressed zip files into Parquet.
    Configuration is INJECTED (not loaded internally).
    """

    def __init__(
        self,
        config: dict,
        dataset: str = "gdelt_event",
        start_date: date | None = None,
        end_date: date | None = None,
        order: str = "asc",
        delete_source: bool = False,
        verbose: bool = False,
        quiet: bool = False,
        force: bool = False,
        dry_run: bool = False,
    ):
        self.config = config
        self.dataset = dataset
        self.start_date = start_date
        self.end_date = end_date
        # "asc" (oldest first, the default) or "desc" (newest first).
        # Only controls submission order into process_all_files' own
        # ProcessPoolExecutor, not real completion order under
        # concurrency, see sort_paths_by_date.
        self.order = order
        # Stored as real instance attributes, not just a level flipped
        # here and forgotten: process_single_file runs inside a
        # ProcessPoolExecutor worker, a genuinely separate process that
        # re-imports this module fresh (get_logger sets INFO again,
        # independent of whatever this process just did), so self.verbose/
        # self.quiet travel across the pickle boundary and
        # process_single_file re-applies whichever is set itself, rather
        # than relying on a level change made here ever reaching the
        # worker. verbose wins if a caller somehow passes both (argparse's
        # mutually exclusive group already prevents that from the CLI).
        self.verbose = verbose
        self.quiet = quiet
        if verbose:
            logger.setLevel(logging.DEBUG)
        elif quiet:
            logger.setLevel(logging.WARNING)
        # Off by default: deletes the source zip once its parquet is
        # written and marked done, so a full historical pull doesn't need
        # to hold the raw archive and the converted output at once. Only
        # the zip; keep_unzipped above already governs the intermediate
        # extracted CSV independently. A caller explicitly opts in per
        # run (CLI: --delete-source), matching start_date/end_date's own
        # shape rather than living in settings.yaml, since it's a
        # deliberate one-off choice about this particular run, not a
        # persistent structural setting.
        self.delete_source = delete_source
        # force bypasses the _is_done check in process_all_files, so a
        # zip already marked done is reprocessed and its output
        # overwritten. dry_run short-circuits process_all_files after
        # to_process is built, before any worker is submitted: it only
        # reports what would happen, so it needs no worker-process
        # propagation the way verbose/quiet do above.
        self.force = force
        self.dry_run = dry_run

        def path_for(base_key: str) -> str:
            return config["paths"][dataset_path_key(dataset, base_key)]

        self.input_folder  = Path(path_for("downloaded_data_directory"))
        self.unzip_folder  = Path(path_for("unzipped_data_directory"))
        self.parquet_folder = Path(path_for("parquet_data_directory"))
        self.keep_unzipped = config["converter"]["keep_unzipped"]
        self.pattern       = config["converter"].get("file_pattern", "*.zip")
        # None is a valid value here: ProcessPoolExecutor treats
        # max_workers=None as "use os.cpu_count()" on its own.
        # max_workers_by_dataset overrides the scalar default for one
        # dataset when set: worker-count safety is dataset-specific (it
        # depends on peak per-worker memory, which output_columns above
        # changes a lot for wide datasets like GKG 2.1), so a value safe
        # for one dataset isn't necessarily safe for another.
        self.max_workers: int | None = config["converter"].get(
            "max_workers_by_dataset", {}
        ).get(dataset, config["converter"].get("max_workers"))

        self.COLUMN_NAMES    = config["columns"][dataset]
        self.NUMERIC_COLUMNS = config["columns_numeric"][dataset]
        # Optional, per dataset: restrict pandas to materializing only these
        # columns while parsing the CSV. self.COLUMN_NAMES is still passed
        # to read_csv in full, since that is what maps each tab-separated
        # position to a name on files that carry no header row. pandas' C
        # parser still skips allocating and decoding the columns left out
        # of usecols, though, which is where GKG 2.1's free-text fields
        # (quotations, all-names, GCAM, extras XML, image/video embeds)
        # cost the most CPU and RAM.
        # None (the default) parses every column, matching prior behavior.
        self.output_columns: list[str] | None = config["converter"].get(
            "output_columns", {}
        ).get(dataset)
        # zstd default, matching filter.compression: measured ~30% smaller
        # than snappy on real Events data at comparable or faster write
        # speed, and lossless, so there's no accuracy tradeoff to weigh
        # before defaulting to it here too. Per-dataset override available
        # the same way as output_columns above.
        self.compression: str = config["converter"].get(
            "compression", {}
        ).get(dataset, "zstd")

        part_cfg = config["converter"].get("partitioning", {})
        self._partitioning_enabled = part_cfg.get("enabled", False)
        self._partition_rules: list[dict] = part_cfg.get("rules", [])

        self.historical_folder: Path | None = None
        if self._partitioning_enabled:
            hist_key = dataset_path_key(dataset, "parquet_historical_directory")
            hist_path = config["paths"].get(hist_key)
            if not hist_path:
                raise ValueError(
                    f"converter.partitioning.enabled is true but "
                    f"paths.{hist_key} is not set."
                )
            self.historical_folder = Path(hist_path)

        # Determines whether a .done marker from a previous run is still
        # valid: output_columns and compression are the converter settings
        # a user plausibly reruns with a different value, and each changes
        # what the output parquet actually contains or how it's stored. A
        # marker written under different values must not cause this run to
        # skip reprocessing that file.
        self._config_fingerprint = config_fingerprint(
            output_columns=self.output_columns, compression=self.compression
        )

        self._create_folders()

    def _create_folders(self):
        for folder in [self.unzip_folder, self.parquet_folder]:
            folder.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured folder exists: {folder}")
        if self.historical_folder:
            self.historical_folder.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured historical folder exists: {self.historical_folder}")

    # ------------------------------------------------------------------
    # File-type detection
    # ------------------------------------------------------------------

    def _detect_file_type(self, zip_name: str) -> str:
        if _QUARTER_HOURLY_PAT.match(zip_name):
            return "quarter_hourly"
        if _DAILY_PAT.match(zip_name):
            return "daily"
        if _MONTHLY_PAT.match(zip_name):
            return "monthly"
        if _YEARLY_PAT.match(zip_name):
            return "yearly"
        return "unknown"

    def _partition_rule_for(self, file_type: str) -> list[str] | None:
        """Return the partition column list for this file type, or None (flat)."""
        for rule in self._partition_rules:
            if rule.get("file_type") == file_type:
                return rule.get("by")
        return None

    # ------------------------------------------------------------------
    # .done marker helpers
    #
    # Applies to every file type, not just historical (Hive-partitioned)
    # ones: flat writes (daily, quarter-hourly, or partitioning off
    # entirely) used to have no resumability at all, so a process killed
    # mid-conversion redid every already-converted file
    # from scratch on the next relaunch, unlike scrape (which skips
    # already-downloaded files). Confirmed for real against a 30,137-file
    # Mentions batch: two consecutive kills each independently died around
    # the same ~51% mark after 30+ minutes, having made no net progress
    # relaunch to relaunch, because every attempt reprocessed every zip
    # from file 1, needlessly overwriting output that was already correct.
    #
    # The marker's content, not just its existence, is what's checked: it
    # stores a fingerprint of this run's output_columns (see
    # config_fingerprint), so a zip processed under a different
    # output_columns value is correctly treated as not done rather than
    # silently served stale output shaped by the old setting.
    #
    # A marker keyed to the source zip is used rather than checking for
    # the output parquet's existence directly, because that's not always
    # a single predictable path: historical writes can fan one input zip
    # out into several partition files (one per Year/MonthYear group),
    # so there is no one output path to check.
    # ------------------------------------------------------------------

    def _is_done(self, zip_path: Path) -> bool:
        return is_marked_done(zip_path, self._config_fingerprint)

    def _mark_done(self, zip_path: Path) -> None:
        mark_done(zip_path, self._config_fingerprint)

    def _delete_source(self, zip_path: Path) -> None:
        """
        Delete the source zip once its parquet output is confirmed
        written and marked done. Only called from the success branch of
        process_all_files, never on a failed or in-progress conversion,
        so a killed run can't lose a zip whose output doesn't actually
        exist yet. A failure here (permissions, the file already gone) is
        logged and swallowed rather than counted as a conversion failure:
        the conversion itself already succeeded, this is best-effort
        cleanup on top of it, not the operation that matters.
        """
        try:
            zip_path.unlink()
            logger.debug(f"Deleted source zip after successful conversion: {zip_path.name}")
        except OSError as e:
            logger.warning(f"Could not delete source zip {zip_path.name}: {e}")

    # ------------------------------------------------------------
    # PROCESS ALL ZIP FILES
    # ------------------------------------------------------------
    def process_all_files(self) -> tuple[list[str], list[str]]:
        """
        Convert every matching ZIP in input_folder.

        Returns (outputs, failed) where outputs are the created parquet paths
        and failed are the source ZIP filenames that raised during processing.
        """
        zip_files = glob.glob(str(self.input_folder / self.pattern))

        if not zip_files:
            logger.warning(
                f"No zip files found in {self.input_folder} with pattern '{self.pattern}'"
            )
            return [], []

        zip_files = filter_paths_by_date(
            zip_files, self.start_date, self.end_date, date_parser=date_parser_for(self.dataset)
        )
        if not zip_files:
            logger.info("Nothing to convert; no files in the given date range.")
            return [], []

        zip_files = sort_paths_by_date(
            zip_files, self.order, date_parser=date_parser_for(self.dataset)
        )

        to_process = []
        for zip_file in zip_files:
            zip_path = Path(zip_file)

            if not self.force and self._is_done(zip_path):
                logger.debug(f"Skipping already converted: {zip_path.name}")
                continue

            to_process.append(zip_file)

        if not to_process:
            logger.info("Nothing to convert; all files already processed.")
            return [], []

        if self.dry_run:
            logger.info(f"[dry run] Would convert {len(to_process)} zip file(s):")
            for zip_file in to_process:
                logger.debug(f"[dry run]   {Path(zip_file).name}")
            return [], []

        logger.info(
            f"Converting {len(to_process)} zip file(s) using "
            f"{self.max_workers or os.cpu_count() or '?'} worker process(es)..."
        )
        all_outputs: list[str] = []
        failed: list[str] = []

        # Each zip is processed independently (its own extracted CSV names,
        # own output parquet paths), so file-level parallelism across
        # processes is safe: this is CPU-bound (CSV parsing + parquet
        # writing), so ProcessPoolExecutor beats threads here.
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.process_single_file, zip_file): zip_file
                for zip_file in to_process
            }

            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Converting ZIP files", unit="zip"
            ):
                zip_path = Path(futures[future])
                try:
                    outputs = future.result()
                    all_outputs.extend(outputs)
                    self._mark_done(zip_path)

                    if self.delete_source:
                        self._delete_source(zip_path)

                except Exception as e:
                    logger.error(f"Failed to process {zip_path.name}: {e}")
                    failed.append(zip_path.name)

        logger.info(
            f"Conversion complete. Total Parquets created: {len(all_outputs)}, "
            f"{len(failed)} zip file(s) failed."
        )
        self._cleanup_unzipped_folder()
        return all_outputs, failed

    # ------------------------------------------------------------
    # PROCESS SINGLE ZIP FILE
    # ------------------------------------------------------------
    def process_single_file(self, zip_path: str) -> list[str]:
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

        zip_p = Path(zip_path)
        # DEBUG, not INFO: unconditional, once per file, which at GKG 2.1/
        # Mentions scale (hundreds of thousands of 15-minute files) means
        # hundreds of thousands of lines fighting the tqdm progress bar
        # below for the terminal. --verbose (run_converter) raises this
        # module's logger to DEBUG for whoever actually wants per-file
        # detail; the default matches scrape's own shape (setup line,
        # progress bar, summary), which never had this problem since its
        # own per-file detail was already DEBUG-only.
        logger.debug(f"Processing ZIP: {zip_p.name}")
        created_parquets = []

        # "flat" is a routing shortcut, not a real granularity: it means
        # partitioning is off entirely, so nothing needs classifying, not
        # that this particular file is daily-cadence. When partitioning
        # is on, file_type is the real detected granularity, and whether
        # that specific granularity actually goes to the historical path
        # is decided below by whether a partitioning.rules entry exists
        # for it, not by comparing file_type against any one hardcoded
        # value: rules are typically only defined for yearly/monthly, so
        # daily and quarter_hourly correctly fall through to a flat write
        # the same as when partitioning is off, without a per-file "no
        # partition rule" warning for every single one of them.
        file_type = (
            self._detect_file_type(zip_p.name)
            if self._partitioning_enabled
            else "flat"
        )
        partition_rule = (
            self._partition_rule_for(file_type) if self._partitioning_enabled else None
        )

        extracted_files = unzip_file(zip_path, self.unzip_folder)
        if not extracted_files:
            logger.warning(f"No extracted files from {zip_p.name}")
            return created_parquets

        for csv_path in extracted_files:
            if csv_path.suffix.lower() != ".csv":
                logger.debug(f"Skipping non-CSV file: {csv_path.name}")
                continue

            try:
                df = self._read_csv(csv_path)
                if df.empty:
                    continue

                if partition_rule is not None:
                    written = self._save_historical_parquet(df, zip_p, file_type)
                    created_parquets.extend(str(p) for p in written)
                else:
                    parquet_path = self._save_parquet(df, csv_path.stem)
                    if parquet_path:
                        created_parquets.append(str(parquet_path))

                if not self.keep_unzipped:
                    csv_path.unlink()

            except Exception as e:
                logger.error(f"Error processing CSV {csv_path.name}: {e}")

        return created_parquets

    # ------------------------------------------------------------
    # READ CSV
    # ------------------------------------------------------------
    def _read_csv(self, csv_path: str | Path) -> pd.DataFrame:
        try:
            # header=0 + names=... together mean "the first line is a real
            # header, skip it, then use our own names instead of its literal
            # text", not "there's no header at all" (that's header=None).
            header = 0 if self.dataset in _DATASETS_WITH_HEADER_ROW else None
            # usecols must reference names from the full COLUMN_NAMES list
            # (pandas resolves position -> name from `names` first, then
            # applies usecols), and drops any configured name that isn't
            # actually one of this dataset's columns rather than erroring,
            # matching how columns_to_check/output_columns are handled
            # elsewhere in the pipeline.
            usecols = (
                [c for c in self.output_columns if c in self.COLUMN_NAMES]
                if self.output_columns is not None
                else None
            )
            df = pd.read_csv(
                csv_path,
                sep="\t",
                header=header,
                dtype=str,
                encoding="utf-8",
                low_memory=False,
                names=self.COLUMN_NAMES,
                usecols=usecols,
                on_bad_lines="warn",
            )

            for col in self.NUMERIC_COLUMNS:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            return df

        except Exception as e:
            logger.error(f"Error reading CSV {csv_path}: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------
    # SAVE PARQUET  (flat files)
    # ------------------------------------------------------------
    def _save_parquet(self, df: pd.DataFrame, base_name: str) -> Path | None:
        if df.empty:
            logger.warning(f"DataFrame empty, skipping parquet: {base_name}")
            return None

        parquet_path = self.parquet_folder / f"{base_name}.parquet"

        try:
            # Cast date/ID columns to Int64 so the schema is consistent with
            # historical Hive files, enabling clean union datasets.
            for col in _DATE_INT_COLS:
                if col in df.columns and pd.api.types.is_float_dtype(df[col]):
                    df[col] = df[col].astype("Int64")

            write_parquet_atomic(
                df, parquet_path,
                engine="pyarrow",
                compression=self.compression,
                index=False,
            )
            return parquet_path

        except Exception as e:
            logger.error(f"Error saving parquet {parquet_path}: {e}")
            return None

    # ------------------------------------------------------------
    # SAVE HISTORICAL PARQUET  (Hive-partitioned)
    # ------------------------------------------------------------
    def _save_historical_parquet(
        self, df: pd.DataFrame, zip_path: Path, file_type: str
    ) -> list[Path]:
        """
        Write df into the historical Hive directory, partitioned by the columns
        defined in settings under converter.partitioning.rules for this file_type.

        Each (partition key combination) becomes one parquet file named after
        the source ZIP stem, e.g.:
            parquet_historical/Year=1979/MonthYear=197901/1979.parquet
        """
        by = self._partition_rule_for(file_type)
        if not by:
            logger.warning(
                f"No partition rule for file_type={file_type!r}; "
                f"falling back to flat write for {zip_path.name}"
            )
            result = self._save_parquet(df, zip_path.stem)
            return [result] if result else []

        df_clean = df.dropna(subset=by).copy()
        if df_clean.empty:
            logger.warning(f"No valid rows after dropping NaN in {by} for {zip_path.name}")
            return []

        # Cast partition columns to int for clean directory names and
        # consistent Arrow schema with the flat files.
        for col in by:
            df_clean[col] = df_clean[col].astype("Int64")

        # Only called when partitioning is enabled, which requires
        # historical_folder to be set (see __init__).
        assert self.historical_folder is not None

        created: list[Path] = []

        for key, group in df_clean.groupby(by, sort=True):
            if not isinstance(key, tuple):
                key = (key,)

            dir_parts = "/".join(f"{col}={int(val)}" for col, val in zip(by, key, strict=True))
            out_dir = self.historical_folder / dir_parts
            out_dir.mkdir(parents=True, exist_ok=True)

            out_path = out_dir / f"{zip_path.stem}.parquet"
            tmp_path = out_path.with_name(out_path.name + ".tmp")
            table = pa.Table.from_pandas(group, preserve_index=False)
            try:
                pq.write_table(table, tmp_path, compression=self.compression)
                os.replace(tmp_path, out_path)
            except Exception:
                # Same atomic tmp-then-rename guarantee as _save_parquet's
                # write_parquet_atomic, just against pq.write_table's Table
                # API instead of DataFrame.to_parquet: a kill or crash
                # mid-write must never leave a truncated file at out_path,
                # since nothing here would ever detect or clean it up later.
                if tmp_path.exists():
                    tmp_path.unlink()
                raise
            created.append(out_path)
            logger.debug(f"Written: {out_path}")

        return created

    # ------------------------------------------------------------
    # CLEAN UP
    # ------------------------------------------------------------
    def _cleanup_unzipped_folder(self) -> None:
        if self.keep_unzipped:
            return
        try:
            self.unzip_folder.rmdir()
            logger.info(f"Deleted empty unzipped folder: {self.unzip_folder}")
        except OSError:
            logger.warning(
                f"Unzipped folder not removed because it is not empty: {self.unzip_folder}"
            )


# ------------------------------------------------------------
# WRAPPER
# ------------------------------------------------------------
def run_converter(
    config: dict,
    dataset: str = "gdelt_event",
    start_date: date | None = None,
    end_date: date | None = None,
    order: str = "asc",
    delete_source: bool = False,
    verbose: bool = False,
    quiet: bool = False,
    force: bool = False,
    dry_run: bool = False,
) -> tuple[list[str], list[str]]:
    """
    Convenience wrapper so main.py can call the converter cleanly.

    Returns (outputs, failed): see GDELTConverter.process_all_files.

    verbose raises this module's own logger to DEBUG, revealing the
    per-file "Processing ZIP"/"Skipping already converted"/"Deleted
    source zip" lines that are DEBUG-level (invisible) by default,
    exactly matching scrape's own already-DEBUG per-attempt detail. Off
    by default: at GKG 2.1/Mentions scale, those lines unconditionally
    at INFO used to mean hundreds of thousands of terminal lines
    fighting the tqdm progress bar below for the screen. quiet is the
    inverse: raises the logger to WARNING, suppressing even the default
    setup/summary INFO lines for scripted or cron use that only cares
    about problems. Mutually exclusive at the CLI; verbose wins if a
    caller passes both directly.

    force reprocesses zips already marked done instead of skipping them.
    dry_run reports what would be converted without processing anything;
    it sees force's effect on the skip list, since it runs after that
    check.
    """
    output_columns = config["converter"].get("output_columns", {}).get(dataset)
    warn_if_output_columns_drops_join_key(logger, "convert", dataset, output_columns)
    warn_if_delete_source_drops_recoverable_data(
        logger, "convert", delete_source,
        narrowing=["output_columns"] if output_columns is not None else [],
    )

    # verbose/quiet are passed straight through rather than raised here
    # directly: GDELTConverter.__init__ sets them (covering this
    # process's own log calls), and process_single_file re-applies
    # whichever is set independently inside each ProcessPoolExecutor
    # worker, since a level change made in this process never reaches
    # those.
    converter = GDELTConverter(
        config, dataset=dataset, start_date=start_date, end_date=end_date, order=order,
        delete_source=delete_source, verbose=verbose, quiet=quiet,
        force=force, dry_run=dry_run,
    )
    return converter.process_all_files()


# ------------------------------------------------------------
# STANDALONE EXECUTION
# ------------------------------------------------------------
if __name__ == "__main__":
    from gdeltforge.utils.config import load_config

    logger.info("Running GDELT conversion pipeline as standalone script...")
    cfg = load_config()
    outputs, failed = run_converter(cfg)
    logger.info(f"Created {len(outputs)} parquet files, {len(failed)} failed.")
