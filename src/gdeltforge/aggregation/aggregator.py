"""
aggregator.py

Concatenates a period's worth of 15-minute-cadence GDELT Parquet files
(GKG 2.1, Mentions, events-15min: the only datasets discovered from the
gdeltv2 master file list, published roughly every 96 files/day) into one
larger file per day, month, or year.

Pure concatenation, not a statistical rollup: every row from every
contributing file lands in the aggregated output unchanged, no
deduplication or summing. Every other dataset (events, events-reduced,
gkg-v1, gkg-v1-counts) already publishes at day-or-coarser granularity,
so it has no many-small-files problem for this to solve; see
utils.config.dataset_is_aggregation_eligible.

The point is IndexedSampler's FileIndex (and every other sampler's own
multi-file scan) paying a real per-file metadata-read/scheduling cost
regardless of file size: measured directly against this project's own
real data (see docs/configuration.md's capacity-planning section),
FileIndex construction alone costs roughly 1-1.5ms per file, independent
of that file's size, which adds up to real minutes at GKG 2.1's real
full-archive file count. Aggregating cuts the file count a sampler has
to open by roughly the same ratio as the period chosen (~96x for daily,
more for monthly/yearly), without changing the total bytes a full-
archive scan still has to read: this is an I/O-overhead fix, not a
compression or row-pruning one.

Provides:
    - GDELTAggregator: class responsible for grouping files by period and
      writing each group's concatenated output
    - run_aggregator: wrapper that resolves config and calls
      aggregate_all_periods
"""

from __future__ import annotations

import glob
import logging
import multiprocessing
import os
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import cast

from polars._typing import ParquetCompression
from tqdm import tqdm

from gdeltforge.scraping.scraper import filter_paths_by_date, parse_file_date
from gdeltforge.utils.config import (
    dataset_is_aggregation_eligible,
    dataset_path_key,
    get_dict,
    validate_max_workers,
)
from gdeltforge.utils.io import (
    config_fingerprint,
    delete_done_marker,
    is_marked_done,
    mark_done,
    scan_dataset_reconciled,
    sink_parquet_atomic,
)
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)


class GDELTAggregator:
    """
    Groups a dataset's own Parquet files by the period their filename
    encodes (day/month/year) and writes each period's concatenated rows
    to one output file, resumably.

    Unlike GDELTConverter/GDELTFilter, whose resumability marker is keyed
    one-per-source-file, this is many-sources-in-one-output: the marker
    is keyed to the OUTPUT file, fingerprinted on both the run's own
    settings (compression, source, delete_source) and the sorted set of
    contributing source filenames. That second part matters here
    specifically: a period whose source-file-set later changes (a
    backfilled/delayed 15-minute file, or a still-in-progress day that
    later gets a file added) must be reprocessed, not treated as
    permanently done just because a marker with a matching config exists.
    """

    _PERIOD_PREFIX_LENGTH = {"day": 8, "month": 6, "year": 4}

    def __init__(
        self,
        input_folder: str,
        output_folder: str,
        period: str = "day",
        source: str = "filtered",
        max_workers: int | None = None,
        compression: str = "zstd",
        start_date: date | None = None,
        end_date: date | None = None,
        date_parser: Callable[[str], tuple[date | None, date | None]] = parse_file_date,
        order: str = "asc",
        delete_source: bool = False,
        verbose: bool = False,
        quiet: bool = False,
        force: bool = False,
        dry_run: bool = False,
    ):
        if period not in self._PERIOD_PREFIX_LENGTH:
            raise ValueError(
                f"period must be one of {sorted(self._PERIOD_PREFIX_LENGTH)}, got {period!r}"
            )
        self.input_folder  = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.period = period
        # Recorded purely to fingerprint the output (see _config_fingerprint
        # below), not used to pick a reader: input_folder already IS
        # whichever of converted/filtered the caller resolved.
        self.source = source
        self.max_workers = validate_max_workers(max_workers, "aggregation.max_workers")
        # zstd default, matching converter.compression/filter.compression:
        # no measured downside on real GDELT data, see docs/configuration.md.
        self.compression = compression
        self.start_date = start_date
        self.end_date = end_date
        # date_parser identifies each SOURCE file's own period from its
        # filename (parse_gdeltv2_file_date for the three eligible
        # datasets, via date_parser_for in run_aggregator below), not the
        # generic parse_file_date this class's own default sees when
        # constructed directly, e.g. by tests, against a plain YYYYMMDD-
        # style fixture.
        self.date_parser = date_parser
        # "asc"/"desc": only controls submission order into the
        # ProcessPoolExecutor below, not real completion order under
        # concurrency, same as convert/filter's identical `order`.
        self.order = order
        # Off by default: deletes each contributing source file once its
        # period's aggregated output is written and marked done, so a
        # full pull doesn't need to hold both the per-15-minute files and
        # the aggregated copy at once. Same shape as convert/filter's own
        # --delete-source.
        self.delete_source = delete_source
        self.verbose = verbose
        self.quiet = quiet
        # Stored as real instance attributes, not just a level flipped
        # here and forgotten: aggregate_one_period runs inside a
        # ProcessPoolExecutor worker, a genuinely separate process that
        # re-imports this module fresh (get_logger sets INFO again), so
        # this level change never reaches it on its own; see
        # aggregate_one_period's own re-application of it.
        if verbose:
            logger.setLevel(logging.DEBUG)
        elif quiet:
            logger.setLevel(logging.WARNING)
        self.force = force
        self.dry_run = dry_run

        self.output_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f"Aggregation output folder ensured: {self.output_folder}")

    # ------------------------------------------------------------
    # PERIOD GROUPING
    # ------------------------------------------------------------

    def _period_key(self, file_path: Path) -> str | None:
        """
        The period this file belongs to, e.g. "20200101" (day),
        "202001" (month), "2020" (year), derived from date_parser's own
        parsed start date, never from the file's row contents: matches
        every other stage's own filename-based discovery
        (scrape/convert/filter/sample all narrow/sort by filename-encoded
        date already, never by opening the file). None means the
        filename couldn't be parsed at all.
        """
        start, _ = self.date_parser(file_path.name)
        if start is None:
            return None
        prefix_len = self._PERIOD_PREFIX_LENGTH[self.period]
        return f"{start.year:04d}{start.month:02d}{start.day:02d}"[:prefix_len]

    def _output_path_for(self, period_key: str) -> Path:
        return self.output_folder / f"{period_key}.parquet"

    # ------------------------------------------------------------
    # PUBLIC API
    # ------------------------------------------------------------

    def aggregate_all_periods(self, pattern: str = "*.parquet") -> tuple[int, int]:
        """
        Aggregate every period found in input_folder. Returns
        (periods_processed, periods_failed).
        """
        files = [Path(p) for p in glob.glob(str(self.input_folder / pattern))]
        files = filter_paths_by_date(
            files, self.start_date, self.end_date, date_parser=self.date_parser
        )

        if not files:
            logger.warning(
                f"No parquet files found in {self.input_folder}"
                + (
                    f" within [{self.start_date} - {self.end_date}]"
                    if self.start_date or self.end_date else ""
                )
            )
            return 0, 0

        groups: dict[str, list[Path]] = {}
        n_unparseable = 0
        for f in files:
            key = self._period_key(f)
            if key is None:
                n_unparseable += 1
                continue
            groups.setdefault(key, []).append(f)

        if n_unparseable:
            logger.warning(
                f"{n_unparseable} file(s) with an unparseable filename date "
                f"were skipped (not assigned to any period)."
            )

        if not groups:
            logger.warning(f"No period could be determined for any file in {self.input_folder}.")
            return 0, 0

        # plan: period_key -> (source_files, output_path, fingerprint).
        # Built up front, before any worker is submitted, so dry_run and
        # the is_marked_done skip check both see the exact same plan a
        # real run would execute.
        plan: dict[str, tuple[list[Path], Path, str]] = {}
        for key in sorted(groups, reverse=(self.order == "desc")):
            group_files = sorted(groups[key])
            output_path = self._output_path_for(key)
            fingerprint = config_fingerprint(
                compression=self.compression,
                source=self.source,
                delete_source=self.delete_source,
                sources=[p.name for p in group_files],
            )
            if not self.force and is_marked_done(output_path, fingerprint):
                logger.debug(f"Skipping already aggregated period: {key}")
                continue
            plan[key] = (group_files, output_path, fingerprint)

        if not plan:
            logger.info("Nothing to aggregate; all periods already processed.")
            return 0, 0

        if self.dry_run:
            logger.info(f"[dry run] Would aggregate {len(plan)} {self.period}(s):")
            for key, (group_files, output_path, _) in plan.items():
                logger.debug(
                    f"[dry run]   {key}: {len(group_files)} file(s) -> {output_path.name}"
                )
            return 0, 0

        logger.info(
            f"Aggregating {len(plan)} {self.period}(s) using "
            f"{self.max_workers or os.cpu_count() or '?'} worker process(es)..."
        )

        periods_processed = 0
        periods_failed = 0

        # Each period is aggregated independently (its own source files,
        # own output path), so period-level parallelism across processes
        # is safe. mp_context forced to spawn, matching converter.py's/
        # filter.py's identical fix for polars' Rayon thread pool not
        # surviving fork() on Linux.
        with ProcessPoolExecutor(
            max_workers=self.max_workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            futures = {
                executor.submit(self.aggregate_one_period, group_files, output_path): key
                for key, (group_files, output_path, _) in plan.items()
            }

            try:
                # Driven manually (with + explicit update()), not "for x
                # in tqdm(...)": see converter.py's process_all_files for
                # the full mechanism this avoids (a bare bound iteration
                # leaks a stray KeyboardInterrupt traceback fragment on a
                # real interrupt).
                with tqdm(total=len(futures), desc="Aggregating periods") as pbar:
                    for future in as_completed(futures):
                        key = futures[future]
                        group_files, output_path, fingerprint = plan[key]
                        try:
                            future.result()
                            mark_done(output_path, fingerprint)

                            if self.delete_source:
                                for source_file in group_files:
                                    self._delete_source(source_file)

                            periods_processed += 1
                            logger.debug(
                                f"{key}: aggregated {len(group_files)} file(s) "
                                f"-> {output_path.name}"
                            )
                        except Exception as e:
                            periods_failed += 1
                            logger.error(f"Failed to aggregate period {key}: {e}")
                        pbar.update(1)
            except KeyboardInterrupt:
                # Same real gap as convert/filter's identical loop: every
                # future was submitted up front, so the executor's own
                # default __exit__ would otherwise drain every one of
                # them, including ones that haven't even started.
                still_running = sum(1 for f in futures if f.running())
                logger.warning(
                    f"Interrupted: waiting for {still_running} in-flight period(s) to finish."
                )
                executor.shutdown(wait=False, cancel_futures=True)
                raise

        logger.info(
            f"Aggregation complete. Periods aggregated: {periods_processed}, "
            f"{periods_failed} failed."
        )
        return periods_processed, periods_failed

    # ------------------------------------------------------------
    # PER-PERIOD PROCESSING
    # ------------------------------------------------------------

    def aggregate_one_period(self, group_files: list[Path], output_path: Path) -> None:
        """
        Concatenate group_files into output_path via a lazy, schema-
        reconciled scan streamed straight to disk (sink_parquet_atomic),
        never an eager per-file read + pl.concat + write: a real attempt
        at the eager form during design work aborted the whole process
        with a Rust-level allocator failure on a real multi-GB group, not
        a catchable Python exception (see sink_parquet_atomic's own
        docstring). scan_dataset_reconciled is the same schema-drift-
        tolerant reader CalendarSampler/FilteredSampler/crossref.py
        already share, so a period whose contributing files disagree on
        a column's presence or dtype is handled the same way here.
        """
        # Re-applied here, not just in __init__: this runs inside a
        # ProcessPoolExecutor worker, a genuinely separate process that
        # re-imports this module fresh, so __init__'s own logger.setLevel
        # call, made in the main process, never reaches it.
        if self.verbose:
            logger.setLevel(logging.DEBUG)
        elif self.quiet:
            logger.setLevel(logging.WARNING)

        logger.debug(f"Aggregating {len(group_files)} file(s) -> {output_path.name}")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        lf = scan_dataset_reconciled(group_files)
        sink_parquet_atomic(lf, output_path, compression=cast(ParquetCompression, self.compression))

    def _delete_source(self, source_path: Path) -> None:
        """
        Delete one contributing source file once its period's aggregated
        output is confirmed written and marked done. Only called from the
        success branch of aggregate_all_periods, never on a failed or
        in-progress aggregation, so a killed run can't lose a source
        whose aggregated output doesn't actually exist yet. A failure
        here (permissions, the file already gone) is logged and
        swallowed rather than counted as an aggregation failure: the
        aggregation itself already succeeded, this is best-effort
        cleanup on top of it, matching convert.py's/filter.py's own
        identical _delete_source.
        """
        try:
            source_path.unlink()
            delete_done_marker(source_path)
            logger.debug(f"Deleted source after successful aggregation: {source_path.name}")
        except OSError as e:
            logger.warning(f"Could not delete source {source_path.name}: {e}")


# ------------------------------------------------------------
# RUN WRAPPER
# ------------------------------------------------------------

def run_aggregator(
    config: dict,
    dataset: str = "gdelt_gkg_v2",
    period: str = "day",
    source: str = "filtered",
    start_date: date | None = None,
    end_date: date | None = None,
    order: str = "asc",
    delete_source: bool = False,
    verbose: bool = False,
    quiet: bool = False,
    force: bool = False,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Convenience wrapper so cli.py can call the aggregator cleanly.

    Rejects a dataset outside gdelt_gkg_v2/gdelt_mentions/
    gdelt_event_15min explicitly: those are the only datasets discovered
    from GDELT's 15-minute gdeltv2 master file list (see
    dataset_is_aggregation_eligible), so aggregation has nothing to solve
    for anything else.
    """
    if not dataset_is_aggregation_eligible(dataset):
        raise ValueError(
            f"{dataset!r} doesn't publish at 15-minute cadence, so aggregation has "
            f"nothing to solve for it. Supported datasets: gkg-v2, mentions, events-15min."
        )

    # Late import: scraper.py's date_parser_for is what actually knows
    # each dataset's own filename convention (parse_gdeltv2_file_date for
    # all three eligible datasets today), independent of this module's
    # own default (parse_file_date), which only matters for a caller
    # (tests) constructing GDELTAggregator directly against a plain
    # YYYYMMDD-style fixture.
    from gdeltforge.scraping.scraper import date_parser_for

    input_base_key = (
        "filtered_data_directory" if source == "filtered" else "parquet_data_directory"
    )
    input_folder = config["paths"][dataset_path_key(dataset, input_base_key)]
    output_base_key = f"aggregated_{period}_data_directory"
    output_folder = config["paths"][dataset_path_key(dataset, output_base_key)]

    agg_cfg = get_dict(config, "aggregation")
    compression = get_dict(agg_cfg, "compression").get(dataset, "zstd")

    aggregator = GDELTAggregator(
        input_folder=input_folder,
        output_folder=output_folder,
        period=period,
        source=source,
        max_workers=agg_cfg.get("max_workers"),
        compression=compression,
        start_date=start_date,
        end_date=end_date,
        date_parser=date_parser_for(dataset),
        order=order,
        delete_source=delete_source,
        verbose=verbose,
        quiet=quiet,
        force=force,
        dry_run=dry_run,
    )
    return aggregator.aggregate_all_periods()
