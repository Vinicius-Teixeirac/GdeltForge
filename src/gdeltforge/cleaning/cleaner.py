"""
cleaner.py

The clean stage (`gdeltforge clean`, formerly `filter`): gdeltforge's
data-quality pass over converted Parquet. It decides whether a row is
usable for any analysis; which rows are relevant to a research question
is decided at sampling time (`sample --mode filtered`), never here. See
docs/data-cleaning.md for that boundary and the reasons behind it.

Per converted file, one streaming polars chain drops rows with a null in
any of the configured required columns, optionally projects to a column
subset, narrows chosen float64 columns to float32, and writes
`<stem>_cleaned.parquet` with the configured codec. For example:

clean:
  columns_to_check:
    gdelt_event:
      - Actor1Code
      - Actor2Code
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
(under parquet_historical_directory) is cleaned in addition to the flat daily files.
The Hive directory structure is preserved in cleaned_historical_directory.

Provides:
    - GDELTCleaner: main cleaning class
    - run_cleaner: wrapper that resolves config and calls `clean_all_files`
"""

import glob
import json
import logging
import multiprocessing
import os
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import cast

import polars as pl

# polars genuinely exports this type alias at runtime; it's just under a
# private-looking module name, the same shape of stub gap pyproject.toml's
# reportPrivateImportUsage = false already exists for (pyarrow.dataset's
# Expression/field/Dataset).
from polars._typing import ParquetCompression
from tqdm import tqdm

from gdeltforge import __version__
from gdeltforge.cleaning.steps import (
    ERRATA_VERSION,
    ORIGINAL_COLUMNS,
    Date1920Repair,
    DeriveColumns,
    EventMarkers,
    FileContext,
    NarrowFloat32,
    NormalizeStrings,
    ProjectColumns,
    RequireColumns,
    Step,
    ordered,
)
from gdeltforge.crossref.crossref import warn_if_output_columns_drops_join_key
from gdeltforge.sampling import cameo_codes
from gdeltforge.scraping.scraper import (
    date_parser_for,
    filter_paths_by_date,
    parse_file_date,
    sort_paths_by_date,
)
from gdeltforge.utils.concurrency import WorkerPlan, plan_workers, polars_worker_env
from gdeltforge.utils.config import (
    DATASET_NAMES,
    dataset_is_always_historical,
    dataset_path_key,
    get_dict,
    resolve_max_concurrent_reads,
    validate_max_workers,
)
from gdeltforge.utils.io import (
    CLEAN_MARKER_KEY,
    config_fingerprint,
    delete_done_marker,
    is_marked_done,
    mark_done,
    warn_if_cleaned_files,
    warn_if_delete_source_drops_recoverable_data,
    write_parquet_atomic,
)
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)




@dataclass
class FileReport:
    """What cleaning one file did, for the run audit and the dry-run
    report. `step_counts` holds each step's own counts (values repaired,
    rows dropped), keyed "<step>.<what>"; `unrecognized` counts, per
    CAMEO-coded column in the output, the non-null values that aren't in
    the bundled code tables."""

    source: str
    output: str
    rows_in: int
    rows_out: int
    unrecognized: dict[str, int] = field(default_factory=dict)
    step_counts: dict[str, int] = field(default_factory=dict)
    columns_in: int = 0
    columns_out: int = 0


# The errata settings a dataset may set under clean.errata.<dataset>, and
# the defaults a caller that sets none of them gets: nothing repaired.
_ERRATA_KEYS = ("date_1920", "event_markers", "keep_original")
_EVENT_MARKER_MODES = ("keep", "drop")
_NORMALIZE_KEYS = ("trim_strings", "blank_to_null")
_DERIVE_KEYS = ("event_date", "labels")


class GDELTCleaner:
    """
    Cleans Parquet files: drops rows with a null in the required columns,
    then shapes the output. Handles both flat daily files and
    Hive-partitioned historical files.

    A file already cleaned under the exact same columns_to_check,
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
        max_concurrent_reads: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        date_parser: Callable[[str], tuple[date | None, date | None]] = parse_file_date,
        order: str = "asc",
        output_columns: list[str] | None = None,
        compression: str = "zstd",
        float32_columns: list[str] | None = None,
        delete_source: bool = False,
        verbose: bool = False,
        quiet: bool = False,
        force: bool = False,
        dry_run: bool = False,
        errata: dict | None = None,
        normalize: dict | None = None,
        derive: dict | None = None,
        allow_lossy_delete_source: bool = False,
        report: bool = False,
        runs_folder: str | None = None,
        other_data_folders: list[str] | None = None,
        dataset: str | None = None,
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
        # Off by default: deletes the source (uncleaned, converted)
        # parquet once its filtered output is written and marked done, so
        # a full historical pull doesn't need to hold both the converted
        # and filtered copies at once. A caller explicitly opts in per run
        # (CLI: --delete-source), same shape as start_date/end_date rather
        # than a persistent settings.yaml value, since it's a deliberate
        # one-off choice about this particular run. Also means whatever
        # this run's own columns_to_check/output_columns/float32_columns
        # narrowed away is gone unless an earlier stage is redone, see
        # warn_if_delete_source_drops_recoverable_data in run_cleaner.
        self.delete_source = delete_source
        # Stored as real instance attributes, not just a level flipped
        # here and forgotten: clean_single_file runs inside a
        # ProcessPoolExecutor worker, a genuinely separate process that
        # re-imports this module fresh (get_logger sets INFO again,
        # independent of whatever this process just did), so self.verbose/
        # self.quiet travel across the pickle boundary and
        # clean_single_file re-applies whichever is set itself, rather
        # than relying on a level change made here ever reaching the
        # worker. verbose wins if a caller somehow passes both (argparse's
        # mutually exclusive group already prevents that from the CLI).
        self.verbose = verbose
        self.quiet = quiet
        if verbose:
            logger.setLevel(logging.DEBUG)
        elif quiet:
            logger.setLevel(logging.WARNING)
        # force bypasses the is_marked_done check in clean_all_files, so
        # a file already marked done is reprocessed and its output
        # overwritten. dry_run short-circuits clean_all_files after
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
        # max_workers=None as "use os.cpu_count()" on its own. Anything
        # else must be a positive int, checked eagerly here rather than
        # left to ProcessPoolExecutor's own constructor: that used to
        # raise well after a pre-flight log line had already announced a
        # different, already-resolved worker count for the same run (see
        # validate_max_workers' own docstring for the exact contradiction
        # a falsy-but-invalid max_workers: 0 produced).
        self.max_workers = validate_max_workers(max_workers, "clean.max_workers")
        # io.max_concurrent_reads: each worker reads one file, so this caps
        # the worker count further, for storage that slows down under many
        # concurrent readers (see docs/configuration.md#io).
        self.max_concurrent_reads = validate_max_workers(
            max_concurrent_reads, "io.max_concurrent_reads"
        )
        # GDELTCleaner stays dataset-agnostic (it never sees a dataset name,
        # only already-resolved paths/columns, see run_cleaner below), so
        # the caller resolves which filename convention date_parser needs
        # to understand rather than GDELTCleaner guessing from a dataset it
        # doesn't have.
        self.start_date = start_date
        self.end_date = end_date
        self.date_parser = date_parser
        # "asc" (oldest first, the default) or "desc" (newest first).
        # Only controls submission order into clean_all_files' own
        # ProcessPoolExecutor, not real completion order under
        # concurrency, see sort_paths_by_date.
        self.order = order

        # Determines whether a .done marker from a previous run is still
        # valid: these are exactly the settings a user plausibly iterates
        # on between runs, and each one changes what the filtered output
        # actually contains (which rows survive, which columns are kept,
        # which are narrowed to float32, how it's compressed on disk). A
        # marker written under different values must not cause this run
        # to skip reprocessing that file and silently serve output shaped
        # by the old configuration.
        # Repairs of known GDELT errors (clean.errata.<dataset>, see
        # steps.py and docs/data-cleaning.md#errata). None or {} repairs
        # nothing; run_cleaner passes the bundled default's settings,
        # which repair without losing any value.
        self.errata = self._validated_errata(errata)
        # Optional whitespace normalization (clean.normalize.<dataset>),
        # off by default: it changes GDELT's values without keeping them.
        self.normalize = self._validated_flags(normalize, _NORMALIZE_KEYS, "clean.normalize")
        # Optional derived columns (clean.derive.<dataset>), off by default.
        self.derive = self._validated_derive(derive)
        # With --dry-run: read every file in scope and report what each
        # step would change, instead of only counting files.
        self.report = report

        fingerprint_fields: dict[str, object] = dict(
            columns_to_check=self.columns_to_check,
            output_columns=self.output_columns,
            float32_columns=self.float32_columns,
            compression=self.compression,
        )
        if self.errata:
            # Only when a rule is configured, so a dataset no rule applies
            # to (GKG, Mentions) keeps its pre-0.12 fingerprint and isn't
            # cleaned again for nothing after upgrading. Sorted JSON, since
            # config_fingerprint renders a dict with str(), which depends on
            # key order; the rule-set version makes a run after an errata
            # rule changes clean every file again.
            fingerprint_fields["errata"] = json.dumps(self.errata, sort_keys=True)
            fingerprint_fields["errata_version"] = ERRATA_VERSION
        if any(self.normalize.values()):
            fingerprint_fields["normalize"] = json.dumps(self.normalize, sort_keys=True)
        if self.derive.get("event_date") or self.derive.get("labels"):
            fingerprint_fields["derive"] = json.dumps(self.derive, sort_keys=True)
        self._config_fingerprint = config_fingerprint(**fingerprint_fields)

        # The stage's steps, built once from the settings above and applied
        # to every file in STEP_ORDER, whatever order they were built in.
        # An empty columns_to_check checks nothing, so it isn't a step:
        # listing it would record a lossy null check that never ran.
        steps: list[Step] = []
        if self.columns_to_check:
            steps.append(RequireColumns(tuple(self.columns_to_check)))
        if self.errata.get("date_1920"):
            keep = self.errata.get("keep_original", True)
            left_out = (
                [c for c in ORIGINAL_COLUMNS if c not in self.output_columns]
                if keep and self.output_columns is not None else []
            )
            if left_out:
                logger.warning(
                    f"clean.output_columns leaves out {left_out}, where the date repair "
                    f"keeps GDELT's values, so the repaired files lose them and the repair "
                    f"counts as lossy. List them in output_columns to keep them."
                )
            steps.append(Date1920Repair(keep_original=keep, originals_left_out=bool(left_out)))
        if self.errata.get("event_markers") is not None:
            steps.append(EventMarkers(mode=self.errata["event_markers"]))
        if any(self.normalize.values()):
            steps.append(NormalizeStrings(
                trim=self.normalize.get("trim_strings", False),
                blank_to_null=self.normalize.get("blank_to_null", False),
            ))
        if self.derive.get("event_date") or self.derive.get("labels"):
            label_maps = []
            for column in self.derive.get("labels", []):
                family = cameo_codes.code_family_for_column(column) or {}
                label_maps.append(
                    (column, tuple((code.upper(), label) for code, label in family.items()))
                )
            steps.append(DeriveColumns(
                event_date=self.derive.get("event_date", False),
                label_maps=tuple(label_maps),
            ))
        if self.output_columns is not None:
            steps.append(ProjectColumns(tuple(self.output_columns)))
        if self.float32_columns:
            steps.append(NarrowFloat32(tuple(self.float32_columns)))
        self.steps: list[Step] = ordered(steps)
        # Written into every cleaned file's Parquet metadata (plus the
        # source file's name, per file), so a cleaned file can never pass
        # for GDELT's own data: see utils.io.warn_if_cleaned_files.
        self._marker = {
            "gdeltforge": __version__,
            "fingerprint": self._config_fingerprint,
            "compression": self.compression,
            "steps": [
                {"step": st.name, "lossy": st.lossy, **st.settings()}
                for st in self.steps
            ],
        }

        # --delete-source removes the converted copy, the only way back to
        # what a lossy step discarded. The stage's original steps keep
        # their long-standing warning (run_cleaner); every step added from
        # 0.12.0 on refuses unless the configuration opts in explicitly.
        # A safety switch, so only a real boolean counts: YAML hands a
        # quoted "no" or "false" through as a non-empty string, which
        # bool() would read as true.
        if not isinstance(allow_lossy_delete_source, bool):
            raise ValueError(
                f"clean.allow_lossy_delete_source must be true or false, "
                f"got {allow_lossy_delete_source!r}"
            )
        refused = [st for st in self.steps if st.lossy and st.guarded]
        if self.delete_source and refused and not allow_lossy_delete_source:
            remedies = []
            for st in refused:
                if isinstance(st, EventMarkers):
                    remedies.append("event_markers: keep")
                elif isinstance(st, Date1920Repair):
                    remedies.append(
                        "list the *_original columns in output_columns"
                        if st.originals_left_out else "keep_original: true"
                    )
                elif isinstance(st, NormalizeStrings):
                    remedies.append("turn normalize off")
            raise ValueError(
                f"--delete-source would delete the converted copy of every file, and "
                f"these steps are lossy: {', '.join(self._describe(st) for st in refused)}. "
                f"Without the converted copy, what they discard is gone. Set "
                f"clean.allow_lossy_delete_source: true to accept that, or "
                f"{', '.join(remedies)}."
            )

        self._refuse_output_inside_input()

        # Where each run's audit goes (paths.clean_runs_directory, or the
        # dataset's own key). Never inside a data directory: polars reads
        # every subdirectory of a directory it's given, `_`- and
        # `.`-prefixed ones included, so an audit there would be read as
        # data. The default is a sibling of the cleaned directory.
        output_base = (
            self.output_folder if self.output_folder.name else self.output_folder.resolve()
        )
        self.runs_folder = (
            Path(runs_folder) if runs_folder
            else output_base.with_name(f"{output_base.name}_runs")
        )
        # Every other dataset's Parquet directories (run_cleaner passes
        # them from paths.*): an audit there would be read as that
        # dataset's data just the same.
        self.other_data_folders = [Path(f) for f in other_data_folders or []]
        self._refuse_runs_inside_data()
        # Only names the dataset's own path keys in warnings; nothing the
        # cleaner does depends on it.
        self.dataset = dataset

    # ======================================================================
    # PUBLIC API
    # ======================================================================

    def clean_all_files(self, pattern: str = "*.parquet") -> tuple[int, int]:
        """
        Clean all parquet files in input_folder (flat) and, if configured,
        all parquet files under historical_input_folder (Hive tree).
        """
        flat_files = [Path(p) for p in glob.glob(str(self.input_folder / pattern))]
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

        # Sorted together, not each list independently then concatenated:
        # a per-list sort would only order flat files relative to other
        # flat files (and likewise for historical), not give a true
        # global order across both when their date ranges overlap.
        historical_set = set(historical_files)
        combined = sort_paths_by_date(
            flat_files + historical_files, self.order, date_parser=self.date_parser
        )
        all_files = [(p, p in historical_set) for p in combined]

        if not all_files:
            logger.warning(
                f"No parquet files found in: {self.input_folder}"
                + (f" or {self.historical_input_folder}" if self.historical_input_folder else "")
            )
            return 0, 0

        warn_if_cleaned_files(
            [p for p, _ in all_files], "clean's input directory", logger, dataset=self.dataset
        )

        to_process = []
        for parquet_path, is_historical in all_files:
            if not self.force and is_marked_done(parquet_path, self._config_fingerprint):
                logger.debug(f"Skipping already cleaned: {parquet_path.name}")
                continue
            to_process.append((parquet_path, is_historical))

        if not to_process:
            logger.info("Nothing to clean; all files already processed.")
            return 0, 0

        if self.dry_run:
            flat_preview = sum(1 for _, is_hist in to_process if not is_hist)
            historical_preview = len(to_process) - flat_preview
            logger.info(
                f"[dry run] Would clean {flat_preview} flat file(s) "
                f"and {historical_preview} historical file(s):"
            )
            for parquet_path, _ in to_process:
                logger.debug(f"[dry run]   {parquet_path.name}")
            if self.report:
                self._dry_run_report(to_process)
            return 0, 0

        # Created only now, so a dry run leaves the filesystem as it found
        # it, directories included.
        self.output_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f"Clean output folder ensured: {self.output_folder}")
        if self.historical_output_folder:
            self.historical_output_folder.mkdir(parents=True, exist_ok=True)
            logger.info(
                f"Historical clean output folder ensured: {self.historical_output_folder}"
            )

        flat_to_process = sum(1 for _, is_hist in to_process if not is_hist)
        historical_to_process = len(to_process) - flat_to_process
        worker_plan = self._worker_plan(len(to_process))
        logger.info(
            f"Cleaning {flat_to_process} flat file(s) "
            f"and {historical_to_process} historical file(s) using "
            f"{worker_plan.describe()}..."
        )

        total_rows_before = 0
        total_rows_after  = 0
        files_processed   = 0
        files_failed      = 0
        reports: list[FileReport] = []
        failed_files: list[str] = []
        started_at = datetime.now(timezone.utc)

        # Each file is filtered independently (its own read, own output
        # path), so file-level parallelism across processes is safe:
        # this is CPU-bound (predicate evaluation + parquet write), so
        # ProcessPoolExecutor beats threads here, matching GDELTConverter's
        # identical reasoning for process_all_files.
        #
        # mp_context is forced to spawn rather than left at the platform
        # default: see converter.py's own _process_files for the full
        # mechanism (polars' native thread pool doesn't survive fork() on
        # Linux, hanging the first polars call inside a forked worker
        # permanently once the parent has already used polars for
        # anything, exactly what a real run here always has by this
        # point). spawn starts a genuinely fresh interpreter per worker
        # with nothing inherited, the same mechanism Windows' own
        # ProcessPoolExecutor already relies on by default.
        # polars_worker_env sizes each worker's own polars pools to its
        # share of the machine: see converter.py's _process_files.
        with polars_worker_env(worker_plan), ProcessPoolExecutor(
            max_workers=worker_plan.workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            futures = {
                executor.submit(
                    self._clean_file,
                    parquet_path,
                    self._output_path_for(parquet_path, is_historical),
                ): parquet_path
                for parquet_path, is_historical in to_process
            }

            try:
                # Driven manually (with + explicit update()) rather than
                # iterated directly (for x in tqdm(...)): see converter.py's
                # process_all_files for the full mechanism this avoids (a
                # bare "for x in tqdm(iterable):" builds a second, separate
                # generator via tqdm's own __iter__, which leaks a stray
                # KeyboardInterrupt traceback fragment if interrupted while
                # suspended mid-loop).
                with tqdm(total=len(futures), desc="Cleaning parquet files") as pbar:
                    for future in as_completed(futures):
                        parquet_path = futures[future]
                        try:
                            report = future.result()
                            rows_before, rows_after = report.rows_in, report.rows_out
                            mark_done(parquet_path, self._config_fingerprint)
                            reports.append(report)

                            if self.delete_source:
                                self._delete_source(parquet_path)

                            total_rows_before += rows_before
                            total_rows_after  += rows_after
                            files_processed   += 1

                            rate = (rows_after / rows_before * 100) if rows_before else 0
                            # DEBUG, not INFO: unconditional, once per file, same
                            # rationale as convert's equivalent per-file lines --
                            # see run_cleaner's verbose docstring.
                            logger.debug(
                                f"{parquet_path.name}: "
                                f"{rows_before:,} -> {rows_after:,} rows ({rate:.1f}% kept)"
                            )

                        except Exception as e:
                            files_failed += 1
                            failed_files.append(parquet_path.name)
                            logger.error(f"Failed to clean {parquet_path.name}: {e}")
                        pbar.update(1)
            except KeyboardInterrupt:
                # Same real gap as convert's identical loop (see
                # converter.py's process_all_files for the full
                # measurement): every future was submitted up front, so
                # the executor's own default __exit__ would otherwise
                # drain every one of them, including ones that haven't
                # even started, before actually exiting. cancel_futures
                # cancels every not-yet-started future immediately;
                # wait=False doesn't additionally block here for files
                # already in flight, since the executor's own __exit__
                # still waits for those specific ones as this exception
                # continues propagating.
                still_running = sum(1 for f in futures if f.running())
                logger.warning(
                    f"Interrupted: waiting for {still_running} in-flight file(s) to finish."
                )
                executor.shutdown(wait=False, cancel_futures=True)
                raise

        logger.info("===============================================")
        logger.info("CLEANING SUMMARY")
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

        self._warn_skipped(reports)
        self._write_audit(reports, failed_files, started_at)
        return files_processed, files_failed

    # ======================================================================
    # PER-FILE PROCESSING
    # ======================================================================

    def clean_single_file(
        self,
        parquet_path: str | Path,
        output_path: Path | None = None,
    ) -> tuple[int, int]:
        """Clean one file; return (rows_before, rows_after). See _clean_file."""
        report = self._clean_file(parquet_path, output_path)
        return report.rows_in, report.rows_out

    def _clean_file(
        self,
        parquet_path: str | Path,
        output_path: Path | None = None,
        write: bool = True,
    ) -> FileReport:
        """
        Clean a single parquet file and report what cleaning did.
        Built as a single lazy chain (scan, the steps in STEP_ORDER, sink) rather
        than a hand-rolled batch loop: polars' own streaming engine is
        what keeps peak RAM bounded here, and sink_parquet writes through
        a temp file + atomic rename so a worker process killed mid-write
        leaves nothing at output_path rather than a truncated file,
        matching the pattern already used for converter output.

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
        logger.debug(f"Cleaning file: {file_path.name}")

        lf = pl.scan_parquet(file_path)
        # Metadata-only: confirmed directly (the query plan shows
        # "PROJECT 0/N COLUMNS") that counting rows this way never reads
        # a single column's data, matching pq.ParquetFile(...).metadata.
        # num_rows' own cheapness under the previous pyarrow-based
        # implementation.
        rows_before = lf.select(pl.len()).collect().item()

        if rows_before == 0:
            logger.warning(f"Empty parquet file skipped: {file_path.name}")
            return FileReport(file_path.name, "", 0, 0)

        # The steps, in STEP_ORDER (see steps.py). Planned lazily here, so
        # a step that can't run on this file (RequireColumns with none of
        # its columns present) raises before anything is written. Each
        # step's own counts are planned over its own input frame and
        # collected together with the row count below.
        period_start, period_end = self.date_parser(file_path.name)
        ctx = FileContext(file_path.name, period_start, period_end)
        columns_in = len(lf.collect_schema())
        count_frames: list[pl.LazyFrame] = []
        # Steps that can't read this file, by their index in self.steps,
        # with the columns they lack: they don't run here, and this file's
        # marker says so (see _file_marker).
        skipped: dict[int, list[str]] = {}
        for i, step in enumerate(self.steps):
            missing = step.missing_columns(lf)
            if missing:
                skipped[i] = missing
            exprs = step.counts(lf, ctx)
            if exprs:
                count_frames.append(lf.select([e.alias(k) for k, e in exprs.items()]))
            lf = step.apply(lf, ctx)

        unrecognized_exprs = self._unrecognized_code_exprs(lf)
        final_counts = lf.select(pl.len().alias("__rows_out"), *unrecognized_exprs)
        # One collect_all call, so polars can share the file scan across
        # the count queries; each reads only the columns it needs.
        *step_results, counts_df = pl.collect_all([*count_frames, final_counts])
        step_counts: dict[str, int] = {}
        for result in step_results:
            step_counts.update({k: int(v) for k, v in result.row(0, named=True).items()})
        counts = counts_df.row(0, named=True)
        rows_after = counts.pop("__rows_out")
        columns_out = len(lf.collect_schema())

        if not write:
            return FileReport(
                source=file_path.name, output="", rows_in=rows_before, rows_out=rows_after,
                unrecognized=dict(counts),
                step_counts=step_counts, columns_in=columns_in, columns_out=columns_out,
            )

        if output_path is None:
            output_path = self.output_folder / f"{file_path.stem}_cleaned.parquet"

        output_path.parent.mkdir(parents=True, exist_ok=True)
        # PID-suffixed, matching write_parquet_atomic's own fix for the
        # identical race: two concurrent invocations targeting the same
        # output_path built this same fixed ".tmp" name, so whichever
        # process's os.replace() ran second found its own tmp file already
        # renamed away by the other, failing with a raw FileNotFoundError
        # despite neither run actually doing anything wrong. This can't go
        # through write_parquet_atomic directly: it writes an already-
        # materialized DataFrame, while sink_parquet below streams lf
        # straight to disk to keep peak memory bounded, which is the whole
        # point of scanning rather than collecting above.
        tmp_path = output_path.with_name(f"{output_path.name}.{os.getpid()}.tmp")

        try:
            lf.sink_parquet(
                tmp_path,
                compression=cast(ParquetCompression, self.compression),
                metadata={CLEAN_MARKER_KEY: json.dumps(self._file_marker(file_path.name, skipped))},
            )
            os.replace(tmp_path, output_path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        self._remove_legacy_output(output_path)

        logger.debug(f"Saved cleaned file -> {output_path}")
        return FileReport(
            source=file_path.name,
            output=output_path.name,
            rows_in=rows_before,
            rows_out=rows_after,
            # Zeros kept, so the audit has one column per checked coded
            # column and a zero reads as checked, never as missing.
            unrecognized=dict(counts),
            step_counts=step_counts,
            columns_in=columns_in,
            columns_out=columns_out,
        )

    def _file_marker(self, source: str, skipped: dict[int, list[str]]) -> dict:
        """
        The marker written into one cleaned file: the run's marker, the
        source file's name, and, on each step that couldn't read this file,
        `"skipped": true` with the columns it lacks. Such a step changed
        nothing here, so it is not lossy for this file. Without that entry
        a file the date repair skipped would read as repaired.
        """
        steps = [
            {**entry, "lossy": False, "skipped": True, "missing_columns": skipped[i]}
            if i in skipped else entry
            for i, entry in enumerate(self._marker["steps"])
        ]
        return {**self._marker, "steps": steps, "source": source}

    @staticmethod
    def _unrecognized_code_exprs(lf: pl.LazyFrame) -> list[pl.Expr]:
        """
        For every CAMEO-coded string column in the output, an expression
        counting non-null values missing from the bundled code tables
        (case-insensitively, as cameo_codes.is_recognized_code compares).
        """
        schema = lf.collect_schema()
        exprs = []
        for column in schema.names():
            family = cameo_codes.code_family_for_column(column)
            if family is None or schema[column] != pl.String:
                continue
            known = [code.upper() for code in family]
            exprs.append(
                (pl.col(column).is_not_null() & ~pl.col(column).str.to_uppercase().is_in(known))
                .sum()
                .alias(column)
            )
        return exprs

    def _worker_plan(self, n_files: int) -> WorkerPlan:
        """
        Worker count and per-worker polars threads for n_files, capped by
        clean.max_workers and io.max_concurrent_reads.
        """
        return plan_workers(
            self.max_workers, n_files, max_concurrent_reads=self.max_concurrent_reads
        )

    def _dry_run_report(self, to_process: list[tuple[Path, bool]]) -> list[FileReport]:
        """
        --dry-run --report: run every step over every file in scope without
        writing anything, and log what each would change. Slower than a
        plain --dry-run, which only counts files: this reads the data.
        """
        reports: list[FileReport] = []
        # The same worker plan as a real run: this reads every file too.
        worker_plan = self._worker_plan(len(to_process))
        with polars_worker_env(worker_plan), ProcessPoolExecutor(
            max_workers=worker_plan.workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            futures = {
                executor.submit(
                    self._clean_file, path, self._output_path_for(path, is_historical), False
                ): path
                for path, is_historical in to_process
            }
            try:
                with tqdm(total=len(futures), desc="Measuring (dry run)") as pbar:
                    for future in as_completed(futures):
                        try:
                            reports.append(future.result())
                        except Exception as e:
                            logger.error(f"[dry run] {futures[future].name} would fail: {e}")
                        pbar.update(1)
            except KeyboardInterrupt:
                executor.shutdown(wait=False, cancel_futures=True)
                raise

        rows_in = sum(r.rows_in for r in reports)
        rows_out = sum(r.rows_out for r in reports)
        logger.info(
            f"[dry run] {len(reports)} file(s) read: {rows_in:,} rows in, {rows_out:,} out "
            f"({rows_in - rows_out:,} removed)"
        )
        totals: dict[str, int] = {}
        for r in reports:
            unrecognized = {f"unrecognized.{k}": v for k, v in r.unrecognized.items()}
            for key, value in {**r.step_counts, **unrecognized}.items():
                totals[key] = totals.get(key, 0) + value
        for key in sorted(totals):
            if key.startswith("unrecognized.") and not totals[key]:
                continue
            logger.info(f"[dry run] {key}: {totals[key]:,}")
        if reports:
            logger.info(
                f"[dry run] columns: {max(r.columns_in for r in reports)} in, "
                f"{max(r.columns_out for r in reports)} out"
            )
        lossy = [self._describe(st) for st in self.steps if st.lossy]
        logger.info(f"[dry run] lossy steps: {', '.join(lossy) if lossy else 'none'}")
        self._warn_skipped(reports)
        return reports

    @staticmethod
    def _warn_skipped(reports: list[FileReport]) -> None:
        """
        Warn once per run for every step that couldn't run on some files
        because they lack the columns it reads (a `<step>_skipped_files`
        count): those files come out as GDELT wrote them, without a word
        otherwise, and without the columns the step adds.
        """
        totals: dict[str, int] = {}
        for r in reports:
            for key, value in r.step_counts.items():
                if key.endswith("_skipped_files") and value:
                    totals[key] = totals.get(key, 0) + value
        for key, n in sorted(totals.items()):
            step = key[: -len("_skipped_files")]
            logger.warning(
                f"{step} didn't run on {n} file(s): they lack a column it reads "
                f"({_STEP_COLUMNS.get(step, 'see docs/data-cleaning.md')}), most likely "
                f"pruned by converter.output_columns. Those files are as GDELT wrote them, "
                f"without the columns {step} adds."
            )

    def _write_audit(
        self, reports: list[FileReport], failed: list[str], started_at: datetime
    ) -> Path | None:
        """
        Write this run's audit to <runs_folder>/<UTC start>.parquet: one row
        per cleaned file (source, output, rows in and out, and one
        `unrecognized.<column>` count per coded column), with the run's
        settings, timing and failed files in the file's metadata. One file
        per run, never per data file: per-file sidecars would recreate the
        many-small-files problem. runs_folder sits outside every data
        directory (see _refuse_runs_inside_data).
        """
        if not reports and not failed:
            return None
        rows = [
            {
                "source": r.source, "output": r.output,
                "rows_in": r.rows_in, "rows_out": r.rows_out,
                **r.step_counts,
                **{f"unrecognized.{k}": v for k, v in r.unrecognized.items()},
            }
            for r in reports
        ]
        df = pl.from_dicts(rows, infer_schema_length=None) if rows else pl.DataFrame(
            schema={"source": pl.String, "output": pl.String,
                    "rows_in": pl.Int64, "rows_out": pl.Int64}
        )
        count_cols = [c for c in df.columns if "." in c]
        if count_cols:
            df = df.with_columns(pl.col(count_cols).fill_null(0))
        unrecognized_cols = [c for c in count_cols if c.startswith("unrecognized.")]
        path = self.runs_folder / f"{started_at:%Y%m%dT%H%M%SZ}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        run = {
            **self._marker,
            "started": started_at.isoformat(),
            "finished": datetime.now(timezone.utc).isoformat(),
            "failed": failed,
        }
        write_parquet_atomic(df, path, metadata={CLEAN_MARKER_KEY + "-run": json.dumps(run)})
        for column in count_cols:
            total = int(df[column].sum())
            if not total:
                continue
            if column in unrecognized_cols:
                logger.info(f"Unrecognized codes in {column.split('.', 1)[1]}: {total:,}")
            else:
                logger.info(f"{column}: {total:,}")
        logger.info(f"Run audit written to {path}")
        return path

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
            # read_parquet_schema's own stubs declare a plain dict return
            # type (its real runtime type, polars.schema.Schema, is a
            # dict subclass with a .names() convenience method the stubs
            # don't expose), so this reads it through the documented dict
            # API instead of relying on the unstubbed runtime extra.
            schema_cols = list(pl.read_parquet_schema(sample_path).keys())
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

        Flat daily files  -> output_folder/<stem>_cleaned.parquet
        Historical files  -> historical_output_folder/<relative_partition_path>/
                             <stem>_cleaned.parquet
        """
        if not is_historical:
            return self.output_folder / f"{parquet_path.stem}_cleaned.parquet"

        # Only called with is_historical=True for files collected from
        # historical_input_folder, so both are guaranteed set here.
        assert self.historical_input_folder is not None
        assert self.historical_output_folder is not None

        relative = parquet_path.relative_to(self.historical_input_folder)
        return (
            self.historical_output_folder
            / relative.parent
            / f"{parquet_path.stem}_cleaned.parquet"
        )

    @staticmethod
    def _validated_errata(errata: dict | None) -> dict:
        """clean.errata.<dataset>, checked before anything runs: unknown
        keys and invalid values fail the run up front, naming the key."""
        errata = dict(errata or {})
        unknown = sorted(set(errata) - set(_ERRATA_KEYS))
        if unknown:
            raise ValueError(
                f"clean.errata: unknown setting(s) {unknown}; known: {list(_ERRATA_KEYS)}"
            )
        for key in ("date_1920", "keep_original"):
            if key in errata and not isinstance(errata[key], bool):
                raise ValueError(f"clean.errata.{key} must be true or false, got {errata[key]!r}")
        markers = errata.get("event_markers")
        if markers is not None and markers not in _EVENT_MARKER_MODES:
            raise ValueError(
                f"clean.errata.event_markers must be one of {list(_EVENT_MARKER_MODES)}, "
                f"got {markers!r}"
            )
        return errata

    @staticmethod
    def _validated_flags(settings: dict | None, keys: tuple[str, ...], label: str) -> dict:
        """A section of true/false switches, checked up front like errata."""
        settings = dict(settings or {})
        unknown = sorted(set(settings) - set(keys))
        if unknown:
            raise ValueError(f"{label}: unknown setting(s) {unknown}; known: {list(keys)}")
        for key, value in settings.items():
            if not isinstance(value, bool):
                raise ValueError(f"{label}.{key} must be true or false, got {value!r}")
        return settings

    @staticmethod
    def _validated_derive(derive: dict | None) -> dict:
        """clean.derive.<dataset>: event_date true/false, labels a list of
        CAMEO-coded columns (see `gdeltforge codes`), checked up front."""
        derive = dict(derive or {})
        unknown = sorted(set(derive) - set(_DERIVE_KEYS))
        if unknown:
            raise ValueError(
                f"clean.derive: unknown setting(s) {unknown}; known: {list(_DERIVE_KEYS)}"
            )
        if "event_date" in derive and not isinstance(derive["event_date"], bool):
            raise ValueError(
                f"clean.derive.event_date must be true or false, got {derive['event_date']!r}"
            )
        labels = derive.get("labels") or []
        if not isinstance(labels, list) or not all(isinstance(c, str) for c in labels):
            raise ValueError(f"clean.derive.labels must be a list of column names, got {labels!r}")
        repeated = sorted({c for c in labels if labels.count(c) > 1})
        if repeated:
            raise ValueError(f"clean.derive.labels lists {repeated} more than once")
        uncoded = [c for c in labels if cameo_codes.code_family_for_column(c) is None]
        if uncoded:
            raise ValueError(
                f"clean.derive.labels: {uncoded} aren't CAMEO-coded columns; "
                f"`gdeltforge codes` lists the ones that are."
            )
        derive["labels"] = labels
        return derive

    @staticmethod
    def _describe(step: Step) -> str:
        if isinstance(step, EventMarkers):
            return "errata.event_markers: drop"
        if isinstance(step, Date1920Repair):
            if step.originals_left_out:
                return "errata.date_1920 with its *_original columns left out of output_columns"
            return "errata.date_1920 without keep_original"
        if isinstance(step, NormalizeStrings):
            return " and ".join(
                f"normalize.{key}" for key, on in
                (("trim_strings", step.trim), ("blank_to_null", step.blank_to_null)) if on
            )
        return step.name

    def _refuse_runs_inside_data(self) -> None:
        """
        Raise before any work if the audit directory is, or sits inside, a
        directory holding data (flat or historical, input or output, this
        dataset's or another's): a reader handed that directory would read
        the audits as rows.
        """
        runs = self.runs_folder.resolve()
        data_dirs = [
            self.input_folder, self.historical_input_folder,
            self.output_folder, self.historical_output_folder,
            *self.other_data_folders,
        ]
        for folder in data_dirs:
            if folder is None:
                continue
            resolved = folder.resolve()
            if runs == resolved or resolved in runs.parents:
                raise ValueError(
                    f"The run audit directory {self.runs_folder} is, or sits inside, "
                    f"data directory {folder}, so reading that directory would read "
                    f"the audits as data. Point paths.clean_runs_directory (or the "
                    f"dataset's own key) at a directory outside the data directories."
                )

    def _refuse_output_inside_input(self) -> None:
        """
        Raise before any work if an output directory is an input directory
        or sits inside one (flat and historical, in every combination).
        Cleaned files written there would sit beside the converted files
        they came from: every later run of this stage would pick its own
        output back up as input, and every reader globbing the converted
        directory would count those rows twice. The converted directory is
        the copy to return to (docs/data-cleaning.md), so it must never
        receive cleaned output.
        """
        inputs = [self.input_folder, self.historical_input_folder]
        outputs = [self.output_folder, self.historical_output_folder]
        for out in outputs:
            if out is None:
                continue
            out_resolved = out.resolve()
            for inp in inputs:
                if inp is None:
                    continue
                inp_resolved = inp.resolve()
                if out_resolved == inp_resolved or inp_resolved in out_resolved.parents:
                    raise ValueError(
                        f"The clean stage would write into its own input: output "
                        f"directory {out} is, or sits inside, input directory {inp}. "
                        f"Point paths.cleaned_data_directory (and "
                        f"cleaned_historical_directory) at a directory of their own, "
                        f"outside parquet_data_directory and parquet_historical_directory."
                    )

    @staticmethod
    def _remove_legacy_output(output_path: Path) -> None:
        """
        Remove the same source's pre-0.12 output (`<stem>_filtered.parquet`,
        written by the stage when it was called `filter`) next to the file
        just written. Both would otherwise sit in one directory holding the
        same day's rows, and every reader globs `*.parquet`, so a sample
        would count that day twice. Only ever called after the new file is
        in place, so a failed run never loses the old copy.
        """
        legacy = output_path.with_name(
            output_path.name.replace("_cleaned.parquet", "_filtered.parquet")
        )
        if legacy != output_path and legacy.exists():
            try:
                legacy.unlink()
                logger.debug(f"Removed pre-0.12 output superseded by {output_path.name}")
            except OSError as e:
                logger.warning(f"Could not remove superseded {legacy.name}: {e}")

    def _delete_source(self, parquet_path: Path) -> None:
        """
        Delete the source (uncleaned, converted) parquet once its
        filtered output is confirmed written and marked done. Only
        called from the success branch of clean_all_files, never on a
        failed or in-progress filter, so a killed run can't lose an input
        whose filtered output doesn't actually exist yet. A failure here
        (permissions, the file already gone) is logged and swallowed
        rather than counted as a filter failure: the filtering itself
        already succeeded, this is best-effort cleanup on top of it.

        Also removes the parquet's own .done marker: once it's gone, the
        marker gates nothing (a deleted parquet can never be found by
        this method's own glob on a later run), so leaving it behind
        just accumulates one orphaned file per deleted source in a
        directory this flag's whole point was to shrink.
        """
        try:
            parquet_path.unlink()
            delete_done_marker(parquet_path)
            logger.debug(
                f"Deleted source parquet after successful cleaning: {parquet_path.name}"
            )
        except OSError as e:
            logger.warning(f"Could not delete source parquet {parquet_path.name}: {e}")


# ======================================================================
# RUN WRAPPER (used by main.py)
# ======================================================================

_CLEAN_KEYS = frozenset({
    "max_workers", "columns_to_check", "output_columns", "float32_columns", "compression",
    "errata", "normalize", "derive", "allow_lossy_delete_source",
})
# The columns a step that can be skipped needs, for its warning.
_STEP_COLUMNS = {
    "errata.date_1920": "Day, MonthYear, Year, FractionDate and DATEADDED",
}

# Base names of every paths.* key holding Parquet data, for any dataset
# (each also exists dataset-prefixed): no run audit may land inside one.
_PARQUET_DIRECTORY_KEYS = (
    "parquet_data_directory", "parquet_historical_directory",
    "cleaned_data_directory", "cleaned_historical_directory",
    "aggregated_day_data_directory", "aggregated_month_data_directory",
    "aggregated_year_data_directory",
)
def _empty_block_hint(section: str) -> str:
    if section == "errata":
        return ("Remove the key to keep the defaults, write {} for the same, or give the "
                "settings; to turn a default rule off, say so (date_1920: false).")
    return f"Remove the key, or write the {section} settings you want ({{}} for none)."
# The datasets GDELT's known errors (errata) are in.
_ERRATA_DATASETS = ("gdelt_event", "gdelt_event_15min")


def _validate_clean_section(config: dict, dataset: str) -> None:
    """
    Catch clean settings that would otherwise be ignored without a word or
    fail only once files are being read: an unknown key under clean:, a
    name under errata/normalize/derive that isn't a dataset, a dataset's
    settings that aren't a mapping, errata for a dataset GDELT's known
    errors aren't in, and derive columns the dataset being cleaned doesn't
    declare.
    """
    clean = config["clean"]
    unknown = sorted(set(clean) - _CLEAN_KEYS)
    if unknown:
        logger.warning(
            f"clean: unknown setting(s) {unknown} are ignored; known: {sorted(_CLEAN_KEYS)}"
        )
    for section in ("errata", "normalize", "derive"):
        per_dataset = clean.get(section)
        # A block whose lines are all commented out loads as null. For
        # errata that used to switch the default repair off without a
        # word, while an empty mapping ({}) keeps it.
        if section in clean and per_dataset is None:
            raise ValueError(f"clean.{section} is empty (null). {_empty_block_hint(section)}")
        if per_dataset is None:
            continue
        if not isinstance(per_dataset, dict):
            raise ValueError(
                f"clean.{section} must map dataset names to settings, got {per_dataset!r}"
            )
        for name, settings in per_dataset.items():
            if name not in DATASET_NAMES:
                raise ValueError(
                    f"clean.{section}.{name}: not a dataset; known: {', '.join(DATASET_NAMES)}"
                )
            if settings is None:
                raise ValueError(
                    f"clean.{section}.{name} is empty (null). {_empty_block_hint(section)}"
                )
            if not isinstance(settings, dict):
                raise ValueError(
                    f"clean.{section}.{name} must be a mapping of settings, got {settings!r}"
                )
            if section == "errata" and settings and name not in _ERRATA_DATASETS:
                raise ValueError(
                    f"clean.errata.{name}: the known GDELT errors this stage repairs are "
                    f"in {' and '.join(_ERRATA_DATASETS)} only"
                )

    # Checked against the declared schema of the dataset being cleaned,
    # when the config has one: a derived column needs its source column.
    declared = get_dict(config, "columns").get(dataset) or []
    derive = get_dict(get_dict(clean, "derive"), dataset)
    if declared and derive.get("event_date") and "Day" not in declared:
        raise ValueError(
            f"clean.derive.{dataset}.event_date: {dataset} has no Day column to parse"
        )
    labels = derive.get("labels") or []
    if not isinstance(labels, list) or not all(isinstance(c, str) for c in labels):
        raise ValueError(
            f"clean.derive.{dataset}.labels must be a list of column names, got {labels!r}"
        )
    uncoded = [c for c in labels if cameo_codes.code_family_for_column(c) is None]
    if uncoded:
        raise ValueError(
            f"clean.derive.{dataset}.labels: {uncoded} aren't CAMEO-coded columns; "
            f"`gdeltforge codes` lists the ones that are."
        )
    repeated = sorted({c for c in labels if labels.count(c) > 1})
    if repeated:
        raise ValueError(f"clean.derive.{dataset}.labels lists {repeated} more than once")
    missing = [c for c in labels if isinstance(c, str) and c not in declared]
    if declared and missing:
        raise ValueError(
            f"clean.derive.{dataset}.labels: {missing} aren't columns of {dataset}"
        )


def run_cleaner(
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
    report: bool = False,
) -> tuple[int, int]:
    """
    Convenience wrapper so the CLI can call the clean stage.

    verbose raises this module's own logger to DEBUG, revealing the
    per-file "{name}: rows -> rows"/"Skipping already cleaned"/"Deleted
    source parquet" lines that are DEBUG-level (invisible) by default,
    exactly matching scrape's own already-DEBUG per-attempt detail and
    convert's identical treatment of its own per-file lines. Off by
    default: at GKG 2.1/Mentions scale, those lines unconditionally at
    INFO used to mean hundreds of thousands of terminal lines fighting
    the tqdm progress bar below for the screen. quiet is the inverse:
    raises the logger to WARNING, suppressing even the default setup/
    summary INFO lines for scripted or cron use that only cares about
    problems. Mutually exclusive at the CLI; verbose wins if a caller
    passes both directly. Both passed straight through to GDELTCleaner
    rather than raised here directly: clean_single_file re-applies
    whichever is set independently inside each ProcessPoolExecutor
    worker, since a level change made in this process never reaches
    those.

    force reprocesses files already marked done instead of skipping
    them. dry_run reports what would be filtered without processing
    anything; it sees force's effect on the skip list, since it runs
    after that check.
    """
    _validate_clean_section(config, dataset)
    part_cfg = get_dict(get_dict(config, "converter"), "partitioning")
    historical_input = historical_output = None

    # gdelt_event_reduced has no flat output mode at all (see converter.py's
    # dataset_is_always_historical), so its historical directories must
    # resolve here regardless of converter.partitioning.enabled, the same
    # bypass converter.py and cli.py's own _historical_folder already apply.
    if part_cfg.get("enabled", False) or dataset_is_always_historical(dataset):
        historical_input = config["paths"].get(
            dataset_path_key(dataset, "parquet_historical_directory")
        )
        historical_output = config["paths"].get(
            dataset_path_key(dataset, "cleaned_historical_directory")
        )

    # `or []`, not just the raw config value: an explicit
    # columns_to_check.<dataset>: null (as opposed to the documented [])
    # reaches `for c in self.columns_to_check` a few frames down and
    # crashes with "'NoneType' object is not iterable" on the first file.
    columns_to_check = config["clean"]["columns_to_check"][dataset] or []
    output_columns = get_dict(config["clean"], "output_columns").get(dataset)
    float32_columns = get_dict(config["clean"], "float32_columns").get(dataset)
    errata = get_dict(get_dict(config["clean"], "errata"), dataset)
    normalize = get_dict(get_dict(config["clean"], "normalize"), dataset)
    derive = get_dict(get_dict(config["clean"], "derive"), dataset)
    allow_lossy_delete_source = config["clean"].get("allow_lossy_delete_source")
    if allow_lossy_delete_source is None:
        allow_lossy_delete_source = False
    warn_if_output_columns_drops_join_key(logger, "clean", dataset, output_columns)
    cleaner = GDELTCleaner(
        input_folder=config["paths"][dataset_path_key(dataset, "parquet_data_directory")],
        output_folder=config["paths"][dataset_path_key(dataset, "cleaned_data_directory")],
        columns_to_check=columns_to_check,
        historical_input_folder=historical_input,
        historical_output_folder=historical_output,
        max_workers=config["clean"].get("max_workers"),
        max_concurrent_reads=resolve_max_concurrent_reads(config),
        start_date=start_date,
        end_date=end_date,
        date_parser=date_parser_for(dataset),
        order=order,
        output_columns=output_columns,
        compression=get_dict(config["clean"], "compression").get(dataset, "zstd"),
        float32_columns=float32_columns,
        delete_source=delete_source,
        verbose=verbose,
        quiet=quiet,
        force=force,
        dry_run=dry_run,
        errata=errata,
        normalize=normalize,
        derive=derive,
        allow_lossy_delete_source=allow_lossy_delete_source,
        report=report,
        runs_folder=config["paths"].get(dataset_path_key(dataset, "clean_runs_directory")),
        other_data_folders=[
            value for key, value in get_dict(config, "paths").items()
            if key.endswith(_PARQUET_DIRECTORY_KEYS) and isinstance(value, str) and value
        ],
        dataset=dataset,
    )
    # After GDELTCleaner, which refuses --delete-source under a lossy step
    # added in 0.12.0: a run that won't happen gets no warning about it.
    warn_if_delete_source_drops_recoverable_data(
        logger, "clean", delete_source,
        narrowing=[
            name for name, value in (
                ("columns_to_check", columns_to_check),
                ("output_columns", output_columns),
                ("float32_columns", float32_columns),
                ("errata.event_markers: drop", errata.get("event_markers") == "drop"),
                ("errata.date_1920 without keep_original",
                 errata.get("date_1920") and errata.get("keep_original") is False),
                ("normalize", any(normalize.values())),
            )
            if value
        ],
    )

    return cleaner.clean_all_files()
