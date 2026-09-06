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

A bare .csv, matched directly by converter.file_pattern, is also accepted
alongside the ZIP archives above: it's read as-is, with no extraction step,
for a CSV that didn't come from a fresh `scrape` (see process_single_file's
is_bare_csv). Its filename plays no part in file-type detection, so it
always flat-writes regardless of converter.partitioning.enabled.

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
from typing import cast

import polars as pl

# polars genuinely exports this type alias at runtime; it's just under a
# private-looking module name, the same shape of stub gap pyproject.toml's
# reportPrivateImportUsage = false already exists for (pyarrow.dataset's
# Expression/field/Dataset).
from polars._typing import ParquetCompression
from tqdm import tqdm

from gdeltforge.crossref.crossref import warn_if_output_columns_drops_join_key
from gdeltforge.scraping.scraper import date_parser_for, filter_paths_by_date, sort_paths_by_date
from gdeltforge.utils.config import dataset_is_always_historical, dataset_path_key, get_dict
from gdeltforge.utils.io import (
    config_fingerprint,
    delete_done_marker,
    is_marked_done,
    mark_done,
    unzip_file,
    warn_if_delete_source_drops_recoverable_data,
    write_parquet_atomic,
)
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

# unzip_file's own "Unzipping: ..."/"Extracted N files from ..." lines
# (one pair per zip, unconditional) log through utils.io's own logger,
# a separate instance from this module's: get_logger caches one logger
# object per name, so this module's own --verbose/--quiet handling
# below, which only ever calls logger.setLevel on *this* module's
# logger, never reached it. --quiet configured on convert then still
# printed both lines for every zip, exactly the shape of "not really
# quiet" a real report was found from. Fetching it by name here (get_
# logger returns the same cached instance utils.io already configured,
# confirmed by its own `if not logger.handlers` guard, so this doesn't
# re-add a handler or reset the level) lets _apply_verbosity below
# raise both loggers together instead of just this module's.
_io_logger = get_logger("gdeltforge.utils.io")


def _apply_verbosity(verbose: bool, quiet: bool) -> None:
    """
    Raises/lowers this module's logger and utils.io's (unzip_file's own
    per-zip "Unzipping"/"Extracted" lines) together, so --quiet/--verbose
    on convert actually cover everything a conversion run logs, not just
    this module's own lines. Called from __init__ (main process) and
    again from process_single_file/process_reduced_file (each worker
    process re-imports this module fresh, so a level set only in the
    main process never reaches them); verbose wins if both are somehow
    set, matching this module's own existing precedence.
    """
    if verbose:
        logger.setLevel(logging.DEBUG)
        _io_logger.setLevel(logging.DEBUG)
    elif quiet:
        logger.setLevel(logging.WARNING)
        _io_logger.setLevel(logging.WARNING)


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

# Every configured numeric column is cast to either pl.Int64 or pl.Float64
# in _read_csv, and this set decides which: every columns_numeric entry
# across every dataset is integer-semantic (an ID, a type/flag code, a
# count, a YYYYMMDD[HHMMSS] timestamp) except the small, genuinely
# fractional set named here, so this lists the exception rather than the
# rule. A future columns_numeric entry defaults to being cast as an
# integer unless added here, matching how the overwhelming majority of
# real entries are shaped.
#
# This distinction mattered even more under the previous pandas
# implementation: pd.to_numeric(errors="coerce") forced a column to
# float64 the moment a single blank value appeared anywhere in a real
# file (GDELT's own raw archive does this, e.g. a real, live
# 20130901.export.CSV.zip carries a blank DATEADDED for literally every
# row that day), since plain int64 can't represent NaN at all, requiring
# a separate restore-to-nullable-Int64 pass afterward or the column
# silently stayed float64 forever. Polars' Int64 is natively nullable, so
# casting straight to it with strict=False already produces the correct
# integer column with nulls where needed, no separate restore step
# needed; this set's job is now purely picking the correct target dtype
# up front, not working around a downstream footgun.
#
# Every entry below (and every column left out of it) was checked against
# the actual "(integer)"/"(floating point)"/"(numeric)" type tag GDELT's
# own codebooks give that field, not inferred from real data alone:
# https://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
# (Events + Mentions) and
# https://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf
# (GKG 2.1). Every entry matches its documented tag except one, kept here
# deliberately against the letter of its tag:
#
# MentionDocTone is tagged "(integer)" in the Event Codebook V2.0's own
# Mentions table section. Its full documented description is "The same
# contents as the AvgTone field in the Events table, but computed for
# this particular article", and AvgTone itself is tagged "(numeric)",
# not integer, described there as ranging -100 to +100 with "common
# values between -10 and +10". A field the codebook explicitly says
# shares AvgTone's contents cannot correctly be a stricter type than
# AvgTone. Real converted data confirms the contradiction resolves in
# AvgTone's favor: MentionDocTone carries genuine fractional values
# (e.g. -4.4776119402985 in real GKG-adjacent output), which a real
# tone score computed the same way as AvgTone requires and an integer
# cannot represent without discarding it. Treated here as a
# documentation inconsistency in GDELT's own codebook, not a real type,
# and kept in this float set accordingly, since casting it to Int64
# would silently truncate genuine analytical precision to "fix" a
# match against a tag that contradicts the same paragraph's own
# description.
_FLOAT_NUMERIC_COLUMNS = frozenset({
    # gdelt_event / gdelt_event_15min
    "FractionDate", "GoldsteinScale", "AvgTone",
    "Actor1Geo_Lat", "Actor1Geo_Long",
    "Actor2Geo_Lat", "Actor2Geo_Long",
    "ActionGeo_Lat", "ActionGeo_Long",
    # gdelt_mentions
    "MentionDocTone",  # see the MentionDocTone note above
    # gdelt_gkg_v1_counts (GKG 1.0's own, older codebook; not covered by
    # either URL above, matched by analogy with GKG 2.1's own Location
    # Latitude/Longitude fields and confirmed against real converted data)
    "Geo_Lat", "Geo_Long",
    # gdelt_event_reduced: GDELT.MASTERREDUCEDV2.1979-2013.zip, confirmed
    # against its own real header and sample rows, not a codebook (this
    # file predates both codebooks linked above and has none of its own).
    # Source/Target/CAMEOCode are excluded from columns_numeric entirely
    # (CAMEOCode carries meaningful leading zeros, e.g. "043"), so every
    # other column in this dataset is integer-semantic except these seven.
    "Goldstein",
    "SourceGeoLat", "SourceGeoLong",
    "TargetGeoLat", "TargetGeoLong",
    "ActionGeoLat", "ActionGeoLong",
})

# GDELT.MASTERREDUCEDV2.1979-2013.zip's single member is 6.58GB / roughly
# 87.3M rows uncompressed; reading it whole with schema_overrides=pl.Utf8
# for every column (every other dataset's individual files are small
# enough for this) would need tens of GB of RAM. 500,000 is a starting
# estimate (rough overhead reasoning: on the order of 1-2GB peak per
# in-flight chunk before numeric casting shrinks it), to be confirmed
# against the real file rather than treated as final.
_EVENT_REDUCED_CHUNK_SIZE = 500_000

# GKG 1.0 (both the main file and its separate Counts file) ships with a
# literal header line (DATE\tNUMARTS\t...); Events, GKG 2.1, and Mentions
# are all genuinely headerless. Confirmed by downloading and inspecting one
# real file per dataset, not assumed from the codebook/parser source alone.
_DATASETS_WITH_HEADER_ROW = frozenset({"gdelt_gkg_v1", "gdelt_gkg_v1_counts"})

# gdelt_event's monthly/yearly archive (pre-April-2013, before the daily
# cadence starts) is a genuinely older, narrower schema, not the same 58
# columns as the modern daily files: SOURCEURL was added to GDELT's export
# later and is simply absent from these files' own trailing field, real
# monthly/yearly files confirmed to have exactly 57 tab-separated columns,
# ending on what would be DATEADDED. Reading them with columns.gdelt_event's
# full 58-name list unmodified means the last requested column position
# (57, 0-indexed) doesn't exist in the file at all, raising a raw
# "projection index: 57 is out of bounds for csv schema with length: 57"
# with no indication it's a schema-vintage mismatch. Found via a live
# comprehensive QA pass against real 2005 (yearly) and 2008-01 (monthly)
# archives, both downloaded and confirmed live.
#
# Keyed by dataset, since this is specific to gdelt_event's own schema
# history, not a general "historical files are narrower" rule; nothing
# else in this pipeline has been found to vary by vintage this way.
_HISTORICAL_MISSING_COLUMNS: dict[str, tuple[str, ...]] = {
    "gdelt_event": ("SOURCEURL",),
}


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
        _apply_verbosity(verbose, quiet)
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
        self.max_workers: int | None = get_dict(
            config["converter"], "max_workers_by_dataset"
        ).get(dataset, config["converter"].get("max_workers"))

        self.COLUMN_NAMES    = config["columns"][dataset]
        self.NUMERIC_COLUMNS = config["columns_numeric"][dataset]
        # Optional, per dataset: restrict _read_csv to materializing only
        # these columns while parsing the CSV, by integer position (see
        # _read_csv itself for why position, not name). self.COLUMN_NAMES
        # is what maps each tab-separated position to a name in the first
        # place, on files that carry no header row of their own; this is
        # where GKG 2.1's free-text fields (quotations, all-names, GCAM,
        # extras XML, image/video embeds) cost the most CPU and RAM once
        # narrowed away.
        # None (the default) parses every column, matching prior behavior.
        self.output_columns: list[str] | None = get_dict(
            config["converter"], "output_columns"
        ).get(dataset)
        # gdelt_event_reduced's Year partition key is computed from its own
        # Date column during conversion (see process_reduced_file), not
        # read from the filename the way every other dataset's partition
        # key is: this file's name carries no date at all. Narrowing
        # output_columns to drop Date would make that computation
        # impossible, so it is rejected here rather than failing later,
        # confusingly, mid-conversion.
        if (
            dataset == "gdelt_event_reduced"
            and self.output_columns is not None
            and "Date" not in self.output_columns
        ):
            raise ValueError(
                "converter.output_columns for gdelt_event_reduced must "
                "include \"Date\": its historical Hive partition key "
                "(Year) is computed from that column during conversion, "
                "not read from the filename the way every other "
                "dataset's partition key is."
            )
        # zstd default, matching filter.compression: measured ~30% smaller
        # than snappy on real Events data at comparable or faster write
        # speed, and lossless, so there's no accuracy tradeoff to weigh
        # before defaulting to it here too. Per-dataset override available
        # the same way as output_columns above.
        self.compression: str = get_dict(
            config["converter"], "compression"
        ).get(dataset, "zstd")

        part_cfg = get_dict(config["converter"], "partitioning")
        self._partitioning_enabled = part_cfg.get("enabled", False)
        self._partition_rules: list[dict] = part_cfg.get("rules", [])

        # gdelt_event_reduced has no flat output mode at all: its converted
        # output only ever exists Hive-partitioned by Year, so its
        # historical directory must resolve regardless of
        # converter.partitioning.enabled, a toggle that otherwise only
        # ever governed Events' own opt-in yearly/monthly split.
        self.historical_folder: Path | None = None
        if self._partitioning_enabled or dataset_is_always_historical(dataset):
            hist_key = dataset_path_key(dataset, "parquet_historical_directory")
            hist_path = config["paths"].get(hist_key)
            if not hist_path:
                reason = (
                    f"{dataset} has no flat output mode; its historical "
                    f"directory is required regardless of "
                    f"converter.partitioning.enabled"
                    if dataset_is_always_historical(dataset)
                    else "converter.partitioning.enabled is true"
                )
                raise ValueError(f"{reason} but paths.{hist_key} is not set.")
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

    def _delete_source(self, source_path: Path) -> None:
        """
        Delete the source file (a ZIP for process_all_files, a leftover
        CSV for recover_unzipped_files) once its parquet output is
        confirmed written and marked done. Only called from the success
        branch of _process_files, never on a failed or in-progress
        conversion, so a killed run can't lose a source whose output
        doesn't actually exist yet. A failure here (permissions, the file
        already gone) is logged and swallowed rather than counted as a
        conversion failure: the conversion itself already succeeded, this
        is best-effort cleanup on top of it, not the operation that matters.

        Also removes the source's own .done marker: once the source is
        gone, the marker gates nothing (a deleted file can never be found
        by that method's own glob on a later run), so leaving it behind
        just accumulates one orphaned file per deleted source in a
        directory this flag's whole point was to shrink.
        """
        try:
            source_path.unlink()
            delete_done_marker(source_path)
            logger.debug(f"Deleted source after successful conversion: {source_path.name}")
        except OSError as e:
            logger.warning(f"Could not delete source {source_path.name}: {e}")

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

        # gdelt_event_reduced's single file needs a genuinely different
        # reader (chunked, no flat write path at all, see
        # process_reduced_file); every other dataset's individual files
        # are small enough for process_single_file's whole-file read.
        worker = (
            self.process_reduced_file
            if self.dataset == "gdelt_event_reduced"
            else self.process_single_file
        )
        return self._process_files(zip_files, worker, unit="zip")

    # ------------------------------------------------------------
    # RECOVER LEFTOVER CSVs (unzipped, never successfully converted)
    # ------------------------------------------------------------
    def recover_unzipped_files(self) -> tuple[list[str], list[str]]:
        """
        Convert every leftover .csv sitting directly in unzip_folder,
        instead of the ZIPs in input_folder process_all_files reads.

        A CSV only ever survives there when something went wrong: either
        its own CSV-to-Parquet step genuinely failed (process_single_file's
        csv_path.unlink() sits after the parquet write in its try block, so
        an exception skips it, and the whole ZIP is correctly left
        undeleted and unmarked for that case, see _delete_source), or
        keep_unzipped is true and the ZIP that produced it is now gone
        (--delete-source ran after a batch of *other* files succeeded, or
        it was cleaned up by hand). Either way, once the ZIP itself is
        gone, process_all_files' own ZIP-only discovery has no way back to
        that data; this is the dedicated, explicit path for it. Never runs
        automatically inside a plain convert: worker processes are
        actively extracting fresh CSVs into this same folder during a
        normal run, so an implicit scan here would race with them.

        Each CSV gets its own .done marker (next to the CSV itself, same
        mechanism process_all_files uses for ZIPs) under this run's own
        config fingerprint, so a CSV already successfully converted under
        the current output_columns/compression is skipped rather than
        silently reprocessed on every recovery run, matching
        keep_unzipped's own "kept after success" case; one converted under
        different settings (or never converted at all) is retried.
        --force/--dry-run/--delete-source apply exactly as they do for
        process_all_files, since both go through the same _process_files.

        gdelt_event_reduced is out of scope: its single ~6.58GB file uses
        a different write model entirely (process_reduced_file, chunked,
        no per-CSV is_bare_csv concept), and keep_unzipped=true for a file
        that size isn't a realistic setup to begin with.
        """
        if self.dataset == "gdelt_event_reduced":
            raise ValueError(
                "recover_unzipped_files isn't supported for gdelt_event_reduced: "
                "its own process_reduced_file already streams and partitions "
                "the source file directly, with no per-CSV leftover to recover."
            )

        csv_files = glob.glob(str(self.unzip_folder / "*.csv"))

        if not csv_files:
            logger.info(f"No leftover CSVs found in {self.unzip_folder}")
            return [], []

        csv_files = filter_paths_by_date(
            csv_files, self.start_date, self.end_date, date_parser=date_parser_for(self.dataset)
        )
        if not csv_files:
            logger.info("Nothing to recover; no files in the given date range.")
            return [], []

        csv_files = sort_paths_by_date(
            csv_files, self.order, date_parser=date_parser_for(self.dataset)
        )

        return self._process_files(csv_files, self.process_single_file, unit="csv")

    # ------------------------------------------------------------
    # SHARED WORKER-POOL DRIVER
    # ------------------------------------------------------------
    def _process_files(
        self, files: list[str], worker, unit: str
    ) -> tuple[list[str], list[str]]:
        """
        Filters already-done files, submits the rest to a worker pool, and
        marks/deletes sources on success. Shared by process_all_files (ZIP
        input) and recover_unzipped_files (leftover-CSV input); the only
        difference between the two is what list of source paths and which
        worker function they hand in here.
        """
        to_process = []
        for source_file in files:
            source_path = Path(source_file)

            if not self.force and self._is_done(source_path):
                logger.debug(f"Skipping already converted: {source_path.name}")
                continue

            to_process.append(source_file)

        if not to_process:
            logger.info("Nothing to convert; all files already processed.")
            return [], []

        if self.dry_run:
            logger.info(f"[dry run] Would convert {len(to_process)} {unit} file(s):")
            for source_file in to_process:
                logger.debug(f"[dry run]   {Path(source_file).name}")
            return [], []

        logger.info(
            f"Converting {len(to_process)} {unit} file(s) using "
            f"{self.max_workers or os.cpu_count() or '?'} worker process(es)..."
        )
        all_outputs: list[str] = []
        failed: list[str] = []

        # Each source file is processed independently (its own extracted
        # CSV names, own output parquet paths), so file-level parallelism
        # across processes is safe: this is CPU-bound (CSV parsing +
        # parquet writing), so ProcessPoolExecutor beats threads here.
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(worker, source_file): source_file
                for source_file in to_process
            }

            try:
                for future in tqdm(
                    as_completed(futures), total=len(futures),
                    desc=f"Converting {unit.upper()} files", unit=unit,
                ):
                    source_path = Path(futures[future])
                    try:
                        outputs = future.result()
                        all_outputs.extend(outputs)
                        self._mark_done(source_path)

                        if self.delete_source:
                            self._delete_source(source_path)

                    except Exception as e:
                        logger.error(f"Failed to process {source_path.name}: {e}")
                        failed.append(source_path.name)
            except KeyboardInterrupt:
                # Every future was submitted up front, so the executor's
                # own default __exit__ (shutdown(wait=True)) would drain
                # every one of them, including ones that haven't even
                # started yet, before actually exiting, the same real
                # timing gap found in scrape's identical loop (see
                # download_gdelt_files for the full measurement).
                # cancel_futures=True (Python >=3.9, this project's own
                # floor is 3.10) cancels every not-yet-started future
                # immediately; wait=False doesn't additionally block here
                # for the (at most max_workers) files already in flight,
                # since a running worker process can't be safely
                # force-killed mid-write anyway, and the executor's own
                # __exit__ will still wait for those specific ones as this
                # exception continues propagating.
                still_running = sum(1 for f in futures if f.running())
                logger.warning(
                    f"Interrupted: waiting for {still_running} in-flight {unit}(s) to finish."
                )
                executor.shutdown(wait=False, cancel_futures=True)
                raise

        logger.info(
            f"Conversion complete. Total Parquets created: {len(all_outputs)}, "
            f"{len(failed)} {unit} file(s) failed."
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
        _apply_verbosity(self.verbose, self.quiet)

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
        #
        # _detect_file_type's own patterns all require a literal .zip
        # suffix, so a bare .csv input (see is_bare_csv below) always
        # comes back "unknown" here regardless of what its name otherwise
        # looks like, and therefore always flat-writes even when
        # partitioning is enabled. Deliberately conservative rather than
        # rewriting those patterns to also recognize a .csv-suffixed
        # name: the real yearly/monthly historical shape this partitioning
        # exists for is a scrape artifact (1979.zip, 200601.zip), not
        # something a bare .csv input is expected to represent.
        # Detected unconditionally, independent of _partitioning_enabled
        # below: a monthly/yearly gdelt_event file's real schema (see
        # _HISTORICAL_MISSING_COLUMNS) doesn't depend on whether the user
        # has opted into Hive-partitioned output for it, only on the
        # file's own actual vintage.
        detected_type = self._detect_file_type(zip_p.name)

        file_type = detected_type if self._partitioning_enabled else "flat"
        partition_rule = (
            self._partition_rule_for(file_type) if self._partitioning_enabled else None
        )

        # A bare .csv pointed at directly (converter.file_pattern set to
        # match one, e.g. for CSVs that didn't come from a fresh `scrape`)
        # needs no extraction at all: it already is the file _read_csv
        # wants, unlike every other input this method handles, which is
        # always an archive unzip_file has to open first. Detected here
        # rather than left to fail into unzip_file's own zipfile.ZipFile
        # call, which raised a confusing BadZipFile for this input before
        # this branch existed.
        is_bare_csv = zip_p.suffix.lower() == ".csv"
        private_extract_dir = None
        if is_bare_csv:
            extracted_files = [zip_p]
        else:
            # Extracted into a private, per-process subdirectory rather
            # than directly into the shared unzip_folder: found via a live
            # comprehensive QA pass that two concurrent gdeltforge
            # invocations converting overlapping files raced on that
            # shared path, both extracting the same zip's same member
            # name into it, one process's os.replace() finding its own
            # .tmp already renamed away by the other, and one process
            # deleting a CSV while the other was still mid-read against
            # it. Naming this uniquely per OS process (a real, separate
            # gdeltforge invocation's own worker gets a distinct PID)
            # means two concurrent processes never share a single
            # extracted file at any point during the read/convert window.
            private_extract_dir = self.unzip_folder / f".pid{os.getpid()}_{zip_p.stem}"
            extracted_files = unzip_file(zip_path, private_extract_dir)
            if not extracted_files:
                logger.warning(f"No extracted files from {zip_p.name}")
                try:
                    private_extract_dir.rmdir()
                except OSError:
                    pass  # not empty: a non-file member extracted alongside
                return created_parquets

        failed_csvs = []
        for csv_path in extracted_files:
            if csv_path.suffix.lower() != ".csv":
                logger.debug(f"Skipping non-CSV file: {csv_path.name}")
                continue

            try:
                df = self._read_csv(csv_path, file_type=detected_type)
                if df.is_empty():
                    continue

                if partition_rule is not None:
                    written = self._save_historical_parquet(df, zip_p, file_type)
                    created_parquets.extend(str(p) for p in written)
                else:
                    parquet_path = self._save_parquet(df, csv_path.stem)
                    if parquet_path:
                        created_parquets.append(str(parquet_path))

                # is_bare_csv's csv_path IS zip_p, the source file itself,
                # not a scratch copy unzip_file extracted into a private
                # directory: only --delete-source (via process_all_files'
                # own _delete_source, gated on that flag) is allowed to
                # remove it. Deleting it here regardless of keep_unzipped
                # would silently destroy the only copy of a source that
                # was never actually unzipped.
                if not is_bare_csv:
                    if self.keep_unzipped:
                        # Moved into the shared, flat unzip_folder only now
                        # that conversion has already succeeded: this is
                        # what keeps it discoverable by name for
                        # recover_unzipped_files' own flat glob, the same
                        # contract it had before this fix, without ever
                        # exposing a partially-written or actively-being-
                        # read file at that shared path.
                        os.replace(csv_path, self.unzip_folder / csv_path.name)
                    else:
                        csv_path.unlink()

            except Exception as e:
                logger.error(f"Error processing CSV {csv_path.name}: {e}")
                failed_csvs.append(csv_path.name)
                if not is_bare_csv:
                    # Moved back to the shared, flat unzip_folder so
                    # recover_unzipped_files' own existing "found a
                    # leftover CSV whose ZIP is now gone" recovery path
                    # still finds it there, matching this method's
                    # documented behavior before this fix: a genuinely
                    # failed CSV survives, findable, once its own
                    # process's work on it is over either way.
                    os.replace(csv_path, self.unzip_folder / csv_path.name)

        if private_extract_dir is not None:
            try:
                private_extract_dir.rmdir()
            except OSError:
                pass  # not empty: a non-CSV member extracted alongside, left as before

        if failed_csvs:
            # Raising (rather than swallowing and returning whatever
            # created_parquets accumulated) is what makes process_all_files'
            # own except Exception branch fire: the zip is counted in
            # `failed`, not marked done, and a plain rerun retries it. The
            # old behavior returned normally here even on a real read
            # failure, which meant the zip got marked done with zero
            # output and never appeared in the run's failed count, see
            # _read_csv's UnicodeDecodeError handling above for the
            # concrete case this was found from.
            raise RuntimeError(
                f"{len(failed_csvs)} of {len(extracted_files)} CSV file(s) in "
                f"{zip_p.name} could not be processed: {', '.join(failed_csvs)}"
            )

        return created_parquets

    # ------------------------------------------------------------
    # PROCESS THE SINGLE gdelt_event_reduced FILE  (chunked, always
    # Hive-partitioned by Year, no flat write path at all)
    # ------------------------------------------------------------
    def process_reduced_file(self, zip_path: str) -> list[str]:
        """
        Converts GDELT.MASTERREDUCEDV2.1979-2013.zip, the one file
        gdelt_event_reduced ever has. Reads its single ~6.58GB / 87.3M-row
        member in chunks via a lazy scan rather than loading it whole the
        way process_single_file does for every other dataset's much
        smaller individual files, and always writes Hive-partitioned by
        Year (computed from this file's own Date column, since its
        filename carries no date at all), never as a flat file: there is
        no flat mode for this dataset, see dataset_is_always_historical.

        Year is directory-only, never written as a column: it doesn't
        exist in the raw file the way Events' own Year/MonthYear
        partition keys do, and keeping columns.gdelt_event_reduced as
        exactly its real 17 columns keeps that list unambiguous for both
        this read and FilteredSampler's column whitelist. Row-level year
        filtering is still fully available after conversion through the
        real Date column (e.g. {"Date": {"op": "between", ...}}).

        Reads with encoding="utf8-lossy" directly rather than _read_csv's
        own utf8-then-retry-on-ComputeError pattern: retrying a partially
        streamed chunked read from scratch would mean re-writing every
        chunk already committed before a bad one turned up, where a plain
        eager read just discards its one in-memory frame and starts over.
        utf8-lossy costs nothing on the genuinely valid UTF-8 this file is
        expected to contain (no free-text fields at all, unlike GKG 2.1's
        quotations/all-names), and only substitutes U+FFFD for the rare
        undecodable byte instead of failing the whole file over it.

        Part files are named deterministically from the chunk index, so a
        rerun (--force, or a retry after a mid-run crash) overwrites the
        same filenames rather than accumulating duplicates; the .done
        marker set by process_all_files afterward is all-or-nothing for
        this dataset, since there is only ever one source file.
        """
        _apply_verbosity(self.verbose, self.quiet)

        zip_p = Path(zip_path)
        logger.debug(f"Processing ZIP: {zip_p.name}")

        extracted_files = unzip_file(zip_path, self.unzip_folder)
        txt_files = [p for p in extracted_files if p.suffix.upper() == ".TXT"]
        if len(txt_files) != 1:
            raise RuntimeError(
                f"Expected exactly one .TXT member in {zip_p.name}, found "
                f"{len(txt_files)}: {[p.name for p in extracted_files]}"
            )
        txt_path = txt_files[0]

        usecols = (
            [c for c in self.output_columns if c in self.COLUMN_NAMES]
            if self.output_columns is not None
            else self.COLUMN_NAMES
        )

        # gdelt_event_reduced is always in dataset_is_always_historical
        # (see __init__), which guarantees historical_folder is set.
        assert self.historical_folder is not None

        # has_header=True + new_columns=... together mean "the first line
        # is a real header, skip it, then use our own names instead of
        # its literal text" (same idiom _read_csv uses; a real header row
        # here is confirmed by opening this file directly). scan_csv has
        # no columns= position-selection parameter the way read_csv does,
        # so every column is named via new_columns and narrowed to
        # usecols via select() below instead; confirmed directly (via
        # .explain()) that polars still pushes that projection down into
        # the scan itself, decoding only the selected columns rather than
        # reading every column and dropping the rest afterward.
        lf = pl.scan_csv(
            txt_path,
            separator="\t",
            has_header=True,
            new_columns=self.COLUMN_NAMES,
            schema_overrides={c: pl.Utf8 for c in self.COLUMN_NAMES},
            truncate_ragged_lines=True,
            null_values=[""],
            encoding="utf8-lossy",
        ).select(usecols)

        created: set[Path] = set()
        try:
            for chunk_idx, chunk in enumerate(
                lf.collect_batches(chunk_size=_EVENT_REDUCED_CHUNK_SIZE)
            ):
                cast_exprs = [
                    pl.col(col).cast(
                        pl.Float64 if col in _FLOAT_NUMERIC_COLUMNS else pl.Int64,
                        strict=False,
                    )
                    for col in self.NUMERIC_COLUMNS
                    if col in chunk.columns
                ]
                if cast_exprs:
                    chunk = chunk.with_columns(cast_exprs)

                chunk = chunk.with_columns((pl.col("Date") // 10000).alias("_Year"))
                n_unparseable = chunk["_Year"].null_count()
                if n_unparseable:
                    logger.warning(
                        f"{zip_p.name} chunk {chunk_idx}: dropping "
                        f"{n_unparseable} row(s) with an unparseable Date "
                        f"(Year can't be computed for partitioning)."
                    )
                chunk = chunk.drop_nulls(subset=["_Year"])

                # Polars' own group_by always yields a tuple key, even for
                # a single-column group_by (see _save_historical_parquet's
                # identical note on this).
                for (year,), group in chunk.group_by("_Year", maintain_order=False):
                    out_dir = self.historical_folder / f"Year={int(year)}"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    # Just the chunk index, not zip_p.stem: there is only ever
                    # one source file for this dataset, so repeating its full
                    # name ("GDELT.MASTERREDUCEDV2.1979-2013", 32 characters)
                    # in every single part file added nothing but length, and
                    # a real run from a moderately nested project directory
                    # pushed the resulting path past Windows' 260-character
                    # MAX_PATH, confirmed directly: the identical conversion
                    # succeeded from a short path and failed only from a
                    # deep one, with no other difference.
                    out_path = out_dir / f"part{chunk_idx:05d}.parquet"
                    self._write_partition_file(group.drop("_Year"), out_path)
                    created.add(out_path)
        finally:
            if not self.keep_unzipped:
                txt_path.unlink(missing_ok=True)

        return [str(p) for p in sorted(created)]

    # ------------------------------------------------------------
    # READ CSV
    # ------------------------------------------------------------
    def _read_csv(self, csv_path: str | Path, file_type: str = "flat") -> pl.DataFrame:
        # has_header=True + new_columns=... together mean "the first line
        # is a real header, skip it, then use our own names instead of
        # its literal text" (confirmed directly: new_columns renames past
        # whatever the real header row said), not "there's no header at
        # all" (that's has_header=False).
        has_header = self.dataset in _DATASETS_WITH_HEADER_ROW

        # A monthly/yearly gdelt_event file genuinely lacks the columns in
        # _HISTORICAL_MISSING_COLUMNS (SOURCEURL); reading it against the
        # full column list would request a position past the real file's
        # last one. missing_columns is only ever non-empty for that one
        # dataset/file_type combination, everything else reads its full
        # declared schema exactly as before.
        missing_columns = (
            _HISTORICAL_MISSING_COLUMNS.get(self.dataset, ())
            if file_type in ("monthly", "yearly")
            else ()
        )
        readable_columns = [c for c in self.COLUMN_NAMES if c not in missing_columns]

        # usecols must reference names from readable_columns (position ->
        # name is its own order, header or not, matching what's actually
        # in the file for this vintage), and drops any configured name
        # that isn't actually one of this dataset's columns rather than
        # erroring, matching how columns_to_check/output_columns are
        # handled elsewhere in the pipeline. Passed to read_csv as integer
        # positions (not the names themselves): confirmed directly that
        # selecting by position at parse time, rather than reading every
        # column and projecting down afterward, is what makes polars skip
        # allocating/decoding the dropped columns at all, the same
        # optimization pandas' own usecols gave GKG 2.1's expensive
        # free-text fields under output_columns. It also happens to be
        # what keeps a genuinely too-wide raw line from silently growing
        # the schema with an extra autogenerated column, confirmed
        # directly: an explicit schema_overrides naming every column
        # does that when columns= is left unset, even with
        # truncate_ragged_lines=True (which only helps the too-short
        # case, kept below for that real, verified hazard in this
        # project's own downloaded data).
        usecols = (
            [c for c in self.output_columns if c in readable_columns]
            if self.output_columns is not None
            else readable_columns
        )
        positions = [readable_columns.index(c) for c in usecols]

        read_kwargs = {
            "separator": "\t",
            "has_header": has_header,
            "columns": positions,
            "new_columns": usecols,
            "schema_overrides": {c: pl.Utf8 for c in usecols},
            "truncate_ragged_lines": True,
            # A bare, unquoted blank tab-separated field (real GDELT's own
            # shape) is already null by polars' own default, confirmed
            # directly; this is only for a QUOTED empty field (""), which
            # polars otherwise keeps as a real empty-string value rather
            # than null, diverging from pandas' read_csv default of
            # nulling it too. Without this, a string column whose source
            # quotes a blank value (Actor1EthnicCode, Actor1Religion1Code,
            # and their Actor2 equivalents among others, if a source ever
            # does this) comes out "" instead of null, which silently
            # breaks columns_to_check's documented contract
            # (configuration.md: "rows with a NaN/null value in any of
            # these columns are dropped") for anyone who lists such a
            # column there. Confirmed via a real content-equality diff
            # against pandas' output on a 10M-row fixture; see
            # TestBlankStringFieldsBecomeNull for the full story on why
            # only the quoted form was ever actually broken.
            "null_values": [""],
            # GDELT's own export never applies CSV-style quote-escaping to
            # any field, tab-separated or otherwise: confirmed directly
            # against a real, freshly-scraped GKG 2.1 file where a raw,
            # unescaped '"' character sits inside V2EXTRASXML's embedded
            # <PAGE_TITLE> text whenever a headline itself quotes someone
            # (e.g. <PAGE_TITLE>"President Donald J Trump is absolutely
            # correct!": ...). With polars' default quote_char='"', an odd
            # count of these literal, incidental quote characters in one
            # file desyncs the multi-threaded reader's notion of "inside a
            # quoted field" from the file's real row boundaries for
            # everything after the last unmatched one, surfacing as
            # "CSV malformed: expected N rows, actual M rows, in chunk
            # starting at byte offset ..." for the rest of the file.
            # quote_char=None disables quote handling entirely, treating
            # a '"' byte as the plain literal character it always was
            # here; verified against 14 real files pulled live from
            # data.gdeltproject.org (11 that failed this way, 3 that
            # didn't) that every one now parses with a row count matching
            # its own raw newline count exactly.
            "quote_char": None,
        }
        try:
            df = pl.read_csv(csv_path, encoding="utf8", **read_kwargs)
        except pl.exceptions.ComputeError as e:
            # Polars raises this same exception class for essentially any
            # CSV-parsing failure, not only a genuine decode problem, so
            # inspecting the message is what actually distinguishes them;
            # confirmed directly, a real invalid byte always raises with
            # exactly this text regardless of where in the file it sits,
            # while an unrelated structural failure (the quote-desync case
            # quote_char=None above now prevents, or any other malformed-
            # CSV shape) never does. Retrying with a plain encoding fix
            # can't do anything for a non-encoding failure, so treating
            # every ComputeError here as "must be invalid UTF-8" the way
            # this used to (a leftover from porting pandas' own precise
            # except UnicodeDecodeError to polars, whose ComputeError has
            # no such narrow equivalent) wasted a doomed retry and blamed
            # the wrong cause in the log for anything else.
            if "invalid utf-8 sequence" not in str(e).lower():
                raise
            # GKG 2.1's free-text fields (quotations, all-names) routinely
            # carry a handful of non-UTF-8 bytes from non-English source
            # articles GDELT scraped; confirmed against a real 373K-file
            # run where ~6.7% of files hit this. The old behavior caught
            # every exception here, including this one, and returned an
            # empty DataFrame, which process_single_file's `if df.is_
            # empty(): continue` then treated as "nothing to write," so
            # the zip still got marked done with zero output and never
            # showed up in the run's failed count. Retrying with
            # encoding="utf8-lossy" keeps every row that WAS valid UTF-8
            # intact and substitutes U+FFFD only for the genuinely
            # undecodable bytes, instead of silently discarding the whole
            # file. Anything other than a decode error (a truly malformed
            # file, a permissions issue) is not retried here; it
            # propagates to process_single_file's caller, which now
            # correctly counts the zip as failed instead of marking it done.
            logger.warning(
                f"{csv_path}: not valid UTF-8, retrying with byte-level "
                f"replacement (undecodable bytes become U+FFFD)"
            )
            df = pl.read_csv(csv_path, encoding="utf8-lossy", **read_kwargs)

        cast_exprs = [
            pl.col(col).cast(
                pl.Float64 if col in _FLOAT_NUMERIC_COLUMNS else pl.Int64, strict=False
            )
            for col in self.NUMERIC_COLUMNS
            if col in df.columns
        ]
        if cast_exprs:
            df = df.with_columns(cast_exprs)

        # A column skipped above because this vintage's real file doesn't
        # have it (missing_columns) still gets added back as an all-null
        # column when it was actually requested (output_columns unset, or
        # explicitly naming it): a caller comparing a modern and a
        # historical gdelt_event file's schema sees the same columns
        # either way, with the historical row's genuine absence of data
        # represented as null rather than the column not existing at all.
        columns_to_add_back = [
            c for c in missing_columns
            if (self.output_columns is None or c in self.output_columns) and c not in df.columns
        ]
        if columns_to_add_back:
            null_exprs = [pl.lit(None, dtype=pl.Utf8).alias(c) for c in columns_to_add_back]
            df = df.with_columns(null_exprs)

        return df

    # ------------------------------------------------------------
    # SAVE PARQUET  (flat files)
    # ------------------------------------------------------------
    def _save_parquet(self, df: pl.DataFrame, base_name: str) -> Path | None:
        if df.is_empty():
            logger.warning(f"DataFrame empty, skipping parquet: {base_name}")
            return None

        parquet_path = self.parquet_folder / f"{base_name}.parquet"

        try:
            write_parquet_atomic(df, parquet_path, compression=self.compression)
            return parquet_path

        except Exception as e:
            logger.error(f"Error saving parquet {parquet_path}: {e}")
            return None

    # ------------------------------------------------------------
    # WRITE ONE PARTITION FILE
    # ------------------------------------------------------------
    def _write_partition_file(self, df: pl.DataFrame, out_path: Path) -> None:
        """
        Write df to out_path via a temp file plus an atomic rename, so a
        kill or crash mid-write never leaves a truncated file at out_path.
        Shared by _save_historical_parquet (Events' own opt-in yearly/
        monthly partitioning) and process_reduced_file (events-reduced's
        always-partitioned chunked write) instead of each duplicating the
        same tmp-then-rename guarantee.
        """
        tmp_path = out_path.with_name(out_path.name + ".tmp")
        try:
            # self.compression is user config, a plain str at gdeltforge's
            # own boundary; polars' own write_parquet narrows it to a
            # specific Literal set for its own internal type-checking, so
            # an actually-invalid codec name still surfaces as a real
            # error from polars itself at write time, just not one
            # pyright can prove here.
            df.write_parquet(tmp_path, compression=cast(ParquetCompression, self.compression))
            os.replace(tmp_path, out_path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    # ------------------------------------------------------------
    # SAVE HISTORICAL PARQUET  (Hive-partitioned)
    # ------------------------------------------------------------
    def _save_historical_parquet(
        self, df: pl.DataFrame, zip_path: Path, file_type: str
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

        df_clean = df.drop_nulls(subset=by)
        if df_clean.is_empty():
            logger.warning(f"No valid rows after dropping NaN in {by} for {zip_path.name}")
            return []

        # Only called when partitioning is enabled, which requires
        # historical_folder to be set (see __init__).
        assert self.historical_folder is not None

        created: list[Path] = []

        # Polars' own group_by always yields a tuple key, even for a
        # single-column `by` (confirmed directly; pandas' groupby instead
        # returns a bare scalar in that case, which the old code had to
        # normalize with an isinstance check), so no such guard is needed
        # here.
        for key, group in df_clean.group_by(by, maintain_order=True):
            dir_parts = "/".join(f"{col}={int(val)}" for col, val in zip(by, key, strict=True))
            out_dir = self.historical_folder / dir_parts
            out_dir.mkdir(parents=True, exist_ok=True)

            out_path = out_dir / f"{zip_path.stem}.parquet"
            self._write_partition_file(group, out_path)
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
    recover_unzipped: bool = False,
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

    recover_unzipped (CLI: --recover-unzipped) converts leftover CSVs
    sitting in unzipped_data_directory instead of the ZIPs in
    downloaded_data_directory: see GDELTConverter.recover_unzipped_files
    for when this is for.
    """
    output_columns = get_dict(config["converter"], "output_columns").get(dataset)
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
    if recover_unzipped:
        return converter.recover_unzipped_files()
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
