import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

from gdeltforge import __version__
from gdeltforge.conversion.converter import run_converter
from gdeltforge.crossref.crossref import (
    crossref_events_gkg_auto,
    crossref_events_gkg_v1,
    crossref_events_gkg_v2,
)
from gdeltforge.filtering.filter import run_filter

# Samplers
from gdeltforge.sampling import cameo_codes
from gdeltforge.sampling.samplers import (
    CalendarSampler,
    FilteredSampler,
    IndexedSampler,
)

# Pipeline stages
from gdeltforge.scraping.scraper import date_parser_for, run_scraping_pipeline
from gdeltforge.utils.branding import compact_emblem, full_banner, safe_print
from gdeltforge.utils.config import (
    dataset_is_always_historical,
    dataset_path_key,
    get_dict,
    load_config,
)
from gdeltforge.utils.io import (
    ensure_exists,
    read_parquet_path,
    write_dataframe_atomic,
    write_parquet_atomic,
)
from gdeltforge.utils.logging import get_logger

# ======================================================================
# Utilities
# ======================================================================

logger = get_logger(__name__, log_to_file=True)

_TAGLINE = "Global Event Data Pipeline"


class _VersionAction(argparse.Action):
    """Prints the full ASCII banner on a real terminal, or a plain
    "gdeltforge X.Y.Z" line when output is piped/redirected, matching the
    brand system's "never on piped output" rule. Registered as a custom
    Action (not the built-in action="version") specifically so it can make
    that distinction; like the built-in version action, it fires and exits
    as soon as --version is parsed, before argparse's own required-
    subcommand check ever runs.
    """

    def __init__(self, option_strings, dest=argparse.SUPPRESS, default=argparse.SUPPRESS,
                 help="show GdeltForge's version and exit"):
        super().__init__(
            option_strings=option_strings, dest=dest, default=default, nargs=0, help=help
        )

    def __call__(self, parser, namespace, values, option_string=None):
        if sys.stdout.isatty():
            safe_print(full_banner(__version__, _TAGLINE))
        else:
            print(f"gdeltforge {__version__}")
        parser.exit()

# CLI-facing --dataset choices, mapped to the dataset keys used throughout
# config (columns / columns_numeric / filter.columns_to_check / paths.*
# via dataset_path_key). GKG's two format generations (pre-2015 "v1" and
# the current, actively-produced "v2") are different schemas with
# different files, so they're exposed as distinct choices rather than
# one ambiguous "gkg".
_DATASET_CHOICES = [
    "events", "events-15min", "events-reduced",
    "gkg-v1", "gkg-v1-counts", "gkg-v2", "mentions",
]
_DATASET_CLI_TO_CONFIG = {
    "events": "gdelt_event",
    "events-15min": "gdelt_event_15min",
    "events-reduced": "gdelt_event_reduced",
    "gkg-v1": "gdelt_gkg_v1",
    "gkg-v1-counts": "gdelt_gkg_v1_counts",
    "gkg-v2": "gdelt_gkg_v2",
    "mentions": "gdelt_mentions",
}

# Each dataset's own real date column for `sample --mode calendar`'s
# default --date-column: Day for Events (both cadences share the same
# 8-digit YYYYMMDD column), Date for GKG 1.0 (same shape, different
# name), and the two 14-digit YYYYMMDDHHMMSS timestamp columns GKG
# 2.1/Mentions actually carry.
_CALENDAR_DATE_SPECS = {
    "events": "Day",
    "events-15min": "Day",
    "events-reduced": "Date",
    "gkg-v1": "Date",
    "gkg-v1-counts": "Date",
    "gkg-v2": "V2.1DATE",
    "mentions": "MentionTimeDate",
}

# crossref's own choices: a GKG generation to join against, not a dataset
# to read directly (v2 pulls in Mentions internally as the join bridge).
# "auto" attempts every eligible event against both generations instead
# of requiring one version for the whole sample; see
# crossref_events_gkg_auto's docstring and configuration.md.
_CROSSREF_GKG_CHOICES = ["v1", "v1-counts", "v2", "auto"]
_CROSSREF_GKG_TO_CONFIG = {
    "v1": "gdelt_gkg_v1",
    "v1-counts": "gdelt_gkg_v1_counts",
}


def _historical_folder(config: dict, path_key: str, dataset: str) -> str | None:
    """
    Return the historical directory path when partitioning is enabled for
    this dataset, else None.

    gdelt_event_reduced bypasses the partitioning.enabled check entirely:
    it has no flat output mode at all, so its historical directory must
    resolve regardless of that toggle, the same bypass converter.py's own
    historical_folder resolution already applies (see
    dataset_is_always_historical).
    """
    if dataset_is_always_historical(dataset):
        return config["paths"].get(path_key)
    part_cfg = get_dict(get_dict(config, "converter"), "partitioning")
    if not part_cfg.get("enabled", False):
        return None
    return config["paths"].get(path_key)

# ======================================================================
# Subcommand Runners
# ======================================================================

def _parse_date(value: str, arg_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise ValueError(f"Invalid date for {arg_name}: '{value}'. Expected YYYY-MM-DD.") from e


def _parse_date_range(args: argparse.Namespace) -> tuple[date | None, date | None]:
    start_date = _parse_date(args.start_date, "--start-date") if args.start_date else None
    end_date = _parse_date(args.end_date, "--end-date") if args.end_date else None

    if start_date and end_date and start_date > end_date:
        raise ValueError(f"--start-date ({start_date}) must not be after --end-date ({end_date}).")

    return start_date, end_date


def _apply_verbosity(args: argparse.Namespace) -> None:
    """
    Applies --verbose/--quiet to this module's own logger (gdeltforge.cli),
    not just the stage module's. Without this, "Starting X stage..."/"X
    completed." (logged from here, not from scraper.py/converter.py/
    filter.py) would leak through under --quiet, and --quiet's own
    "suppress even the default setup/summary lines" promise would be a
    lie for exactly those two lines. Only called from the three commands
    that actually define these flags (scrape/convert/filter); getattr's
    default keeps it a no-op for any caller whose args lacks them.
    """
    if getattr(args, "verbose", False):
        logger.setLevel(logging.DEBUG)
    elif getattr(args, "quiet", False):
        logger.setLevel(logging.WARNING)


# sample/crossref's --export-format: --out already defaults to *.parquet,
# so this is a no-op unless a caller actually asks for a different format.
_EXPORT_EXTENSIONS = {"parquet": ".parquet", "csv": ".csv"}


def _out_path_for_export_format(out: Path, export_format: str) -> Path:
    """
    --export-format is authoritative over --out's own extension: whatever
    suffix --out was given (or defaulted to) gets replaced with the one
    matching the chosen format, so `--out result.parquet --export-format
    csv` writes result.csv rather than CSV content into a .parquet-named
    file.
    """
    return out.with_suffix(_EXPORT_EXTENSIONS[export_format])


def _write_sample_output(df, out: Path, export_format: str) -> None:
    """
    Writes sample/crossref's finished, already-in-memory result. The
    default (parquet) case calls write_parquet_atomic directly, by that
    exact name, rather than always going through write_dataframe_atomic:
    several existing tests isolate unrelated CLI wiring (which folder a
    sampler reads from, which join function gets called) by monkeypatching
    cli.write_parquet_atomic, and this keeps that working unchanged.
    """
    if export_format == "parquet":
        write_parquet_atomic(df, out)
    else:
        write_dataframe_atomic(df, out, export_format=export_format)


def run_scrape_cmd(config: dict, args: argparse.Namespace) -> None:
    _apply_verbosity(args)
    start_date, end_date = _parse_date_range(args)

    dataset = _DATASET_CLI_TO_CONFIG[args.dataset]
    logger.info("Starting scraping stage...")
    result = run_scraping_pipeline(
        config, start_date=start_date, end_date=end_date, dataset=dataset, order=args.order,
        verbose=args.verbose, quiet=args.quiet,
        force=args.force, dry_run=args.dry_run,
    )
    logger.info("Scraping completed.")

    failed = result["failed"]
    if failed:
        raise RuntimeError(
            f"Scraping finished with {len(failed)} failed download(s): {', '.join(failed)}"
        )


def run_convert_cmd(config: dict, args: argparse.Namespace) -> None:
    _apply_verbosity(args)
    start_date, end_date = _parse_date_range(args)

    dataset = _DATASET_CLI_TO_CONFIG[args.dataset]
    logger.info("Starting conversion stage...")
    outputs, failed = run_converter(
        config, dataset=dataset, start_date=start_date, end_date=end_date, order=args.order,
        delete_source=args.delete_source, verbose=args.verbose, quiet=args.quiet,
        force=args.force, dry_run=args.dry_run, recover_unzipped=args.recover_unzipped,
    )
    logger.info(f"Created {len(outputs)} parquet files.")

    if failed:
        raise RuntimeError(
            f"Conversion finished with {len(failed)} failed file(s): {', '.join(failed)}"
        )


def run_filter_cmd(config: dict, args: argparse.Namespace) -> None:
    _apply_verbosity(args)
    start_date, end_date = _parse_date_range(args)

    dataset = _DATASET_CLI_TO_CONFIG[args.dataset]
    logger.info("Starting filtering stage...")
    files_processed, files_failed = run_filter(
        config, dataset=dataset, start_date=start_date, end_date=end_date, order=args.order,
        delete_source=args.delete_source, verbose=args.verbose, quiet=args.quiet,
        force=args.force, dry_run=args.dry_run,
    )
    logger.info("Filtering completed.")

    if files_failed:
        raise RuntimeError(
            f"Filtering finished with {files_failed} failed file(s) out of "
            f"{files_processed + files_failed}."
        )


def run_sampling_cmd(config: dict, args: argparse.Namespace) -> None:
    start_date, end_date = _parse_date_range(args)
    dataset = _DATASET_CLI_TO_CONFIG[args.dataset]
    date_parser = date_parser_for(dataset)

    if args.mode == "filtered" and (start_date or end_date) and args.filter:
        logger.warning(
            "--start-date/--end-date and --filter are both set: --start-date/--end-date "
            "narrow which files get scanned (by filename period), while --filter narrows "
            "rows within whatever files remain. Both apply independently, so a result "
            "narrower than expected from either alone usually means the other is also active."
        )

    source_key, historical_key = (
        ("filtered_data_directory", "filtered_historical_directory")
        if args.source == "filtered"
        else ("parquet_data_directory", "parquet_historical_directory")
    )
    source_key = dataset_path_key(dataset, source_key)
    historical_key = dataset_path_key(dataset, historical_key)
    source_folder = ensure_exists(config["paths"][source_key], source_key)

    out = _out_path_for_export_format(Path(args.out), args.export_format)

    # Create parent folder if it does not exist
    out.parent.mkdir(parents=True, exist_ok=True)

    hist_folder = _historical_folder(config, historical_key, dataset)
    columns = set(args.columns) if args.columns else None

    # -----------------------------
    # Indexed Sampling
    # -----------------------------
    if args.mode == "indexed":
        sampler = IndexedSampler(
            folder_path=str(source_folder),
            historical_folder=hist_folder,
            random_state=args.seed,
            columns=columns,
            start_date=start_date,
            end_date=end_date,
            date_parser=date_parser,
        )
        df = sampler.get_random_sample(args.n)
        _write_sample_output(df, out, args.export_format)
        logger.info(f"Saved indexed sample ({len(df)} rows) -> {out}")
        return

    # -----------------------------
    # Calendar Sampling
    # -----------------------------
    if args.mode in ("calendar", "daily"):
        if args.mode == "daily":
            if args.period is not None:
                raise ValueError(
                    "--period isn't accepted with the deprecated --mode daily "
                    "(ambiguous about which was meant); use --mode calendar instead"
                )
            logger.warning(
                "--mode daily is deprecated; use --mode calendar (period=day is "
                "the default, so --mode calendar alone behaves the same way)"
            )
            period = "day"
        else:
            period = args.period or "day"

        if args.per_day is not None:
            logger.warning("--per-day is deprecated; use --per-period instead")
            samples_per_period = args.per_day
        else:
            samples_per_period = args.per_period

        date_column = args.date_column or _CALENDAR_DATE_SPECS[args.dataset]

        sampler = CalendarSampler(
            folder_path=str(source_folder),
            historical_folder=hist_folder,
            random_state=args.seed,
            columns=columns,
            date_column=date_column,
            period=period,
            start_date=start_date,
            end_date=end_date,
            date_parser=date_parser,
        )
        df = sampler.get_calendar_samples(samples_per_period=samples_per_period)
        _write_sample_output(df, out, args.export_format)
        logger.info(f"Saved calendar sample ({len(df)} rows, period={period}) -> {out}")
        return

    # -----------------------------
    # Filtered Sampling
    # -----------------------------
    if args.mode == "filtered":
        if args.filter is None:
            raise ValueError(
                "--filter is required when mode == 'filtered' "
                "(must be JSON string)"
            )

        try:
            filter_dict = json.loads(args.filter)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON passed to --filter: {e}") from e

        sampler = FilteredSampler(
            folder_path=str(source_folder),
            gdelt_columns=config["columns"][dataset],
            columns=columns,
            filter_dict=filter_dict,
            random_state=args.seed,
            historical_folder=hist_folder,
            start_date=start_date,
            end_date=end_date,
            date_parser=date_parser,
        )

        if args.stratify:
            if args.n_per_group is None:
                raise ValueError("--n-per-group is required when --stratify is set")
            df = sampler.get_stratified_sample(args.stratify, args.n_per_group)
            _write_sample_output(df, out, args.export_format)
            logger.info(
                f"Saved stratified sample ({len(df)} rows) "
                f"stratified by '{args.stratify}' ({args.n_per_group} per group) -> {out}"
            )
        else:
            df = sampler.get_random_sample(args.n)
            _write_sample_output(df, out, args.export_format)
            logger.info(
                f"Saved filtered sample ({len(df)} rows) "
                f"using filter={filter_dict} -> {out}"
            )
        return

    raise ValueError(f"Unknown sampling mode: {args.mode}")


def run_crossref_cmd(config: dict, args: argparse.Namespace) -> None:
    start_date, end_date = _parse_date_range(args)

    events_df = read_parquet_path(args.events)
    columns = set(args.columns) if args.columns else None
    source_key = (
        "filtered_data_directory" if args.source == "filtered" else "parquet_data_directory"
    )

    out = _out_path_for_export_format(Path(args.out), args.export_format)
    out.parent.mkdir(parents=True, exist_ok=True)

    if args.gkg_version == "auto":
        if columns:
            raise ValueError(
                "--columns isn't supported with --gkg-version auto, since GKG 1.0 and "
                "GKG 2.1 use different column names for equivalent fields, so one column "
                "set can't validly restrict both. Use --gkg-version v1 or v2 directly to "
                "restrict columns, or call crossref_events_gkg_auto with its own "
                "v1_columns/v2_columns to restrict each path independently."
            )
        gkg_v1_folder = ensure_exists(
            config["paths"][dataset_path_key("gdelt_gkg_v1", source_key)],
            "GKG 1.0 directory",
        )
        mentions_folder = ensure_exists(
            config["paths"][dataset_path_key("gdelt_mentions", source_key)],
            "Mentions directory",
        )
        gkg_v2_folder = ensure_exists(
            config["paths"][dataset_path_key("gdelt_gkg_v2", source_key)],
            "GKG 2.1 directory",
        )
        result = crossref_events_gkg_auto(
            events_df,
            str(gkg_v1_folder),
            config["columns"]["gdelt_gkg_v1"],
            str(mentions_folder),
            str(gkg_v2_folder),
            config["columns"]["gdelt_gkg_v2"],
            on_duplicate_document=args.on_duplicate_document,
            dedupe_mentions=args.collapse_duplicate_mentions,
            start_date=start_date,
            end_date=end_date,
        )
    elif args.gkg_version == "v2":
        mentions_folder = ensure_exists(
            config["paths"][dataset_path_key("gdelt_mentions", source_key)],
            "Mentions directory",
        )
        gkg_v2_folder = ensure_exists(
            config["paths"][dataset_path_key("gdelt_gkg_v2", source_key)],
            "GKG 2.1 directory",
        )
        result = crossref_events_gkg_v2(
            events_df,
            str(mentions_folder),
            str(gkg_v2_folder),
            config["columns"]["gdelt_gkg_v2"],
            columns=columns,
            on_duplicate_document=args.on_duplicate_document,
            dedupe_mentions=args.collapse_duplicate_mentions,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        dataset = _CROSSREF_GKG_TO_CONFIG[args.gkg_version]
        gkg_folder = ensure_exists(
            config["paths"][dataset_path_key(dataset, source_key)],
            f"{args.gkg_version} directory",
        )
        result = crossref_events_gkg_v1(
            events_df, str(gkg_folder), config["columns"][dataset], columns=columns,
            start_date=start_date, end_date=end_date,
        )

    _write_sample_output(result, out, args.export_format)
    logger.info(f"Saved cross-referenced sample ({len(result)} rows) -> {out}")


_CAMEO_COLUMN_GROUPS = [
    cameo_codes.CAMEO_ACTOR_COUNTRY_COLUMNS,
    cameo_codes.FIPS_GEO_COLUMNS,
    cameo_codes.CAMEO_ETHNIC_COLUMNS,
    cameo_codes.CAMEO_KNOWN_GROUP_COLUMNS,
    cameo_codes.CAMEO_RELIGION_COLUMNS,
    cameo_codes.CAMEO_TYPE_COLUMNS,
    cameo_codes.CAMEO_EVENT_COLUMNS,
]


def run_codes_cmd(args: argparse.Namespace) -> None:
    if args.column is None:
        print("CAMEO-coded columns with a reference list:\n")
        for group in _CAMEO_COLUMN_GROUPS:
            columns = sorted(group)
            print(f"  {cameo_codes.family_name_for_column(columns[0])}:")
            for c in columns:
                print(f"    {c}")
            print()
        print("Run `gdeltforge codes <column>` to list that column's codes.")
        return

    code_family = cameo_codes.code_family_for_column(args.column)
    if code_family is None:
        known = sorted(c for group in _CAMEO_COLUMN_GROUPS for c in group)
        raise ValueError(
            f"'{args.column}' has no CAMEO code reference list. Known columns: "
            f"{', '.join(known)}"
        )

    entries = sorted(code_family.items())
    if args.search:
        term = args.search.lower()
        entries = [
            (code, name) for code, name in entries
            if term in code.lower() or term in name.lower()
        ]

    if not entries:
        print(f"No codes matching '{args.search}' in {args.column}.")
        return

    width = max(len(code) for code, _ in entries)
    for code, name in entries:
        print(f"  {code.ljust(width)}  {name}")
    print(f"\n{len(entries)} code(s).")


# ======================================================================
# Argument Parser
# ======================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="GdeltForge: a data pipeline for the GDELT Events Database"
    )
    parser.add_argument("--version", action=_VersionAction)
    parser.add_argument(
        "--config",
        metavar="PATH",
        help="Path to settings.yaml. Defaults to the GDELTFORGE_CONFIG "
             "environment variable, then ./config/settings.yaml.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # ----------------------------------------------------
    # scrape
    # ----------------------------------------------------
    scrape = subparsers.add_parser("scrape", help="Download and extract raw GDELT data")
    scrape.add_argument(
        "--dataset",
        choices=_DATASET_CHOICES,
        required=True,
        help="Which GDELT dataset to scrape (required)"
    )
    scrape.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        help="Only download files whose period starts on or after this date",
    )
    scrape.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        help="Only download files whose period ends on or before this date",
    )
    scrape.add_argument(
        "--order",
        choices=["asc", "desc"],
        default="asc",
        help="Processing order: asc (oldest first, the default) or desc (newest first). "
             "Only controls which files are submitted to the download pool first, not real "
             "completion order under concurrency"
    )
    scrape_verbosity = scrape.add_mutually_exclusive_group()
    scrape_verbosity.add_argument(
        "--verbose",
        action="store_true",
        help="Show per-attempt download detail (filename, attempt N/M) instead of just the "
             "progress bar and summary. Off by default"
    )
    scrape_verbosity.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress even the default setup/summary lines, leaving only warnings and "
             "errors. Off by default"
    )
    scrape.add_argument(
        "--force",
        action="store_true",
        help="Re-download files that already exist locally instead of skipping them. "
             "Off by default"
    )
    scrape.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many files would be downloaded and skipped without downloading "
             "anything. Off by default"
    )

    # ----------------------------------------------------
    # convert
    # ----------------------------------------------------
    convert = subparsers.add_parser("convert", help="Convert raw data to parquet")
    convert.add_argument(
        "--dataset",
        choices=_DATASET_CHOICES,
        required=True,
        help="Which GDELT dataset to convert (required)"
    )
    convert.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        help="Only convert files whose period starts on or after this date",
    )
    convert.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        help="Only convert files whose period ends on or before this date",
    )
    convert.add_argument(
        "--order",
        choices=["asc", "desc"],
        default="asc",
        help="Processing order: asc (oldest first, the default) or desc (newest first). "
             "Only controls which files are submitted to the worker pool first, not real "
             "completion order under concurrency"
    )
    convert.add_argument(
        "--delete-source",
        action="store_true",
        help="Delete each source zip once its parquet output is written and confirmed done. "
             "Off by default. Only the zip; the intermediate extracted CSV is already "
             "removed unless converter.keep_unzipped is set. Combined with output_columns, "
             "the dropped columns can't be recovered without re-scraping"
    )
    convert.add_argument(
        "--recover-unzipped",
        action="store_true",
        help="Convert leftover .csv files sitting in unzipped_data_directory instead of the "
             "zips in downloaded_data_directory. For a CSV whose own conversion failed, or "
             "one kept by converter.keep_unzipped, once its source zip is no longer around "
             "to retry from normally (e.g. --delete-source already removed it). Off by "
             "default; not supported for --dataset events-reduced"
    )
    convert_verbosity = convert.add_mutually_exclusive_group()
    convert_verbosity.add_argument(
        "--verbose",
        action="store_true",
        help="Show per-file conversion detail (which zip is being processed, which are "
             "skipped as already done) instead of just the progress bar and summary. Off by "
             "default: at GKG 2.1/Mentions scale this is hundreds of thousands of lines"
    )
    convert_verbosity.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress even the default setup/summary lines, leaving only warnings and "
             "errors. Off by default"
    )
    convert.add_argument(
        "--force",
        action="store_true",
        help="Reprocess zips already marked done instead of skipping them, overwriting "
             "their parquet output. Off by default"
    )
    convert.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many zips would be converted without converting anything. "
             "Off by default"
    )

    # ----------------------------------------------------
    # filter
    # ----------------------------------------------------
    filter_ = subparsers.add_parser("filter", help="Filter parquet files")
    filter_.add_argument(
        "--dataset",
        choices=_DATASET_CHOICES,
        required=True,
        help="Which GDELT dataset to filter (required)"
    )
    filter_.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        help="Only filter files whose period starts on or after this date",
    )
    filter_.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        help="Only filter files whose period ends on or before this date",
    )
    filter_.add_argument(
        "--order",
        choices=["asc", "desc"],
        default="asc",
        help="Processing order: asc (oldest first, the default) or desc (newest first). "
             "Only controls which files are submitted to the worker pool first, not real "
             "completion order under concurrency"
    )
    filter_.add_argument(
        "--delete-source",
        action="store_true",
        help="Delete each source (unfiltered, converted) parquet once its filtered output "
             "is written and confirmed done. Off by default. Combined with "
             "columns_to_check/output_columns/float32_columns, whatever those narrowed away "
             "can't be recovered without re-converting; also removes the option to later "
             "`sample --source converted` against the unfiltered data"
    )
    filter_verbosity = filter_.add_mutually_exclusive_group()
    filter_verbosity.add_argument(
        "--verbose",
        action="store_true",
        help="Show per-file filter detail (rows kept per file, which are skipped as already "
             "done) instead of just the progress bar and summary. Off by default: at GKG "
             "2.1/Mentions scale this is hundreds of thousands of lines"
    )
    filter_verbosity.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress even the default setup/summary lines, leaving only warnings and "
             "errors. Off by default"
    )
    filter_.add_argument(
        "--force",
        action="store_true",
        help="Reprocess files already marked done instead of skipping them, overwriting "
             "their filtered output. Off by default"
    )
    filter_.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many files would be filtered without filtering anything. "
             "Off by default"
    )

    # ----------------------------------------------------
    # sample
    # ----------------------------------------------------
    sample = subparsers.add_parser(
        "sample", help="Sampling utilities (indexed, filtered, calendar)"
    )

    sample.add_argument(
        "--dataset",
        choices=_DATASET_CHOICES,
        required=True,
        help="Which GDELT dataset to sample from (required)"
    )
    sample.add_argument(
        "--mode",
        required=True,
        choices=["indexed", "filtered", "calendar", "daily"],
        help="Sampling strategy. 'daily' is a deprecated alias for "
             "'calendar' (period=day)"
    )
    sample.add_argument(
        "--source",
        choices=["filtered", "converted"],
        default="filtered",
        help="Which stage's output to sample from: 'filtered' (default, "
             "after the filter command) or 'converted' (raw parquet, "
             "before filtering)"
    )
    sample.add_argument(
        "-n", type=int, default=1000,
        help="Number of rows to sample"
    )
    sample.add_argument(
        "--seed", type=int, default=42,
        help="RNG seed"
    )
    sample.add_argument(
        "--per-period", type=int, default=10,
        help="Rows per calendar period (calendar mode only, default 10)"
    )
    sample.add_argument(
        "--per-day", type=int, default=None,
        help="Deprecated alias for --per-period"
    )
    sample.add_argument(
        "--period",
        choices=["day", "month", "year"],
        default=None,
        help="Calendar period to group by (calendar mode only, default 'day'). "
             "Not accepted alongside the deprecated --mode daily"
    )
    sample.add_argument(
        "--date-column",
        default=None,
        help="Date column to group by in calendar mode (default depends on "
             "--dataset: Day for events/events-15min, Date for gkg-v1/"
             "gkg-v1-counts, V2.1DATE for gkg-v2, MentionTimeDate for mentions)"
    )
    sample.add_argument(
        "--filter",
        help="JSON dictionary for filtered sampling "
             "(e.g. '{\"QuadClass\": [1,2]}')"
    )
    sample.add_argument(
        "--columns",
        nargs="*",
        help="Restrict output to these columns (all modes); cuts both I/O "
             "and memory use on the full archive"
    )
    sample.add_argument(
        "--stratify",
        metavar="COLUMN",
        help="Column to stratify by (filtered mode only); requires --n-per-group"
    )
    sample.add_argument(
        "--n-per-group",
        type=int,
        metavar="N",
        help="Rows per stratum when --stratify is set"
    )
    sample.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        help="Only sample from files whose period starts on or after this date. "
             "Applies to all three modes alike, narrowing the file list each one "
             "reads before it does anything else; in filtered mode this stacks "
             "with --filter's own row-level conditions rather than replacing them"
    )
    sample.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        help="Only sample from files whose period ends on or before this date. "
             "Same file-level narrowing as --start-date above"
    )
    sample.add_argument(
        "--out",
        default="sample.parquet",
        help="Output parquet file"
    )
    sample.add_argument(
        "--export-format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Output file format. csv rewrites --out's extension to .csv. "
             "Off (parquet) by default. csv writes a <name>.csv.schema.json "
             "sidecar alongside it; read the file back with gdeltforge's own "
             "read_csv_export(path), not a bare pl.read_csv/pd.read_csv, or "
             "zero-padded string codes like EventCode lose their leading zero"
    )

    # ----------------------------------------------------
    # crossref
    # ----------------------------------------------------
    crossref = subparsers.add_parser(
        "crossref", help="Cross-reference a sampled Events output against GKG"
    )
    crossref.add_argument(
        "--events",
        required=True,
        metavar="PATH",
        help="Parquet file of Events rows to enrich, e.g. the output of `gdeltforge sample`. "
             "A directory of parquet files (e.g. convert/filter output) also works; .done "
             "resumability markers in it are ignored"
    )
    crossref.add_argument(
        "--gkg-version",
        required=True,
        choices=_CROSSREF_GKG_CHOICES,
        help="Which GKG generation to join against: v1 (direct join on EventIds, main GKG "
             "1.0 file), v1-counts (same join, GKG 1.0's separate Counts file), v2 "
             "(two-hop join through Mentions, GKG 2.1), or auto (attempts every eligible "
             "event against both generations, e.g. for a sample spanning the 2013-2015 "
             "window where only GKG 1.0 exists; --columns isn't supported with auto)"
    )
    crossref.add_argument(
        "--source",
        choices=["filtered", "converted"],
        default="filtered",
        help="Which stage's GKG/Mentions output to read from (default: filtered)"
    )
    crossref.add_argument(
        "--columns",
        nargs="*",
        help="Restrict GKG-side output to these columns; cuts I/O and memory. The join key "
             "column is always included regardless. Not supported with --gkg-version auto"
    )
    crossref.add_argument(
        "--on-duplicate-document",
        choices=["latest", "earliest", "all"],
        default="all",
        help="When GKG 2.1 carries more than one record for the same article URL (a page "
             "crawled more than once): keep all of them, one row per record (default), "
             "or narrow to just the most recent or the earliest record. Only affects "
             "--gkg-version v2/auto; v1 has no equivalent duplicate-document step."
    )
    crossref.add_argument(
        "--collapse-duplicate-mentions",
        action="store_true",
        help="Collapse per-sentence duplicate mentions of the same event in the same "
             "article into one row with an explicit Mention_Count column, instead of "
             "keeping every raw Mentions row (the default). Only affects "
             "--gkg-version v2/auto."
    )
    crossref.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        help="Only join against GKG/Mentions files whose period starts on or after this "
             "date. Narrows the configured directories being read, not --events, which is "
             "unaffected either way"
    )
    crossref.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        help="Only join against GKG/Mentions files whose period ends on or before this "
             "date. Narrows the configured directories being read, not --events, which is "
             "unaffected either way"
    )
    crossref.add_argument(
        "--out",
        default="crossref.parquet",
        help="Output parquet file"
    )
    crossref.add_argument(
        "--export-format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Output file format. csv rewrites --out's extension to .csv. "
             "Off (parquet) by default. csv writes a <name>.csv.schema.json "
             "sidecar alongside it; read the file back with gdeltforge's own "
             "read_csv_export(path), not a bare pl.read_csv/pd.read_csv, or "
             "zero-padded string codes like EventCode lose their leading zero"
    )

    # ----------------------------------------------------
    # codes
    # ----------------------------------------------------
    codes = subparsers.add_parser(
        "codes", help="Look up valid GDELT country codes for filter values"
    )
    codes.add_argument(
        "column",
        nargs="?",
        help="A country-code column (e.g. ActionGeo_CountryCode). "
             "Omit to list which columns have a reference list.",
    )
    codes.add_argument(
        "--search",
        metavar="TERM",
        help="Filter to codes/names containing this substring (case-insensitive)",
    )

    return parser


# ======================================================================
# Entrypoint
# ======================================================================

def main() -> None:
    parser = build_parser()

    # Printed before parse_args, not after: argparse's own -h/--help
    # handling (and a parse error, e.g. a missing required --dataset)
    # exits from inside parse_args itself, before anything past it ever
    # runs. That left --help, almost always the very first thing a
    # freshly-installed user actually runs, showing zero branding at
    # all. --version is skipped here since VersionAction below already
    # prints the fuller full_banner; showing this one-liner first too
    # would just be redundant right above it.
    if sys.stdout.isatty() and "--version" not in sys.argv:
        safe_print(compact_emblem(__version__))

    args = parser.parse_args()

    try:
        if args.command == "codes":
            run_codes_cmd(args)
            return

        config = load_config(args.config)

        logger.info(f"Running command: {args.command}")

        if args.command == "scrape":
            run_scrape_cmd(config, args)

        elif args.command == "convert":
            run_convert_cmd(config, args)

        elif args.command == "filter":
            run_filter_cmd(config, args)

        elif args.command == "sample":
            run_sampling_cmd(config, args)

        elif args.command == "crossref":
            run_crossref_cmd(config, args)

    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
