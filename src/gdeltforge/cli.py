import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

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
    DailySampler,
    FilteredSampler,
    IndexedSampler,
)

# Pipeline stages
from gdeltforge.scraping.scraper import run_scraping_pipeline
from gdeltforge.utils.config import dataset_path_key, load_config
from gdeltforge.utils.io import ensure_exists, read_parquet_path, write_parquet_atomic
from gdeltforge.utils.logging import get_logger

# ======================================================================
# Utilities
# ======================================================================

logger = get_logger(__name__, log_to_file=True)

# CLI-facing --dataset choices, mapped to the dataset keys used throughout
# config (columns / columns_numeric / filter.columns_to_check / paths.*
# via dataset_path_key). GKG's two format generations (pre-2015 "v1" and
# the current, actively-produced "v2") are different schemas with
# different files, so they're exposed as distinct choices rather than
# one ambiguous "gkg".
_DATASET_CHOICES = ["events", "gkg-v1", "gkg-v1-counts", "gkg-v2", "mentions"]
_DATASET_CLI_TO_CONFIG = {
    "events": "gdelt_event",
    "gkg-v1": "gdelt_gkg_v1",
    "gkg-v1-counts": "gdelt_gkg_v1_counts",
    "gkg-v2": "gdelt_gkg_v2",
    "mentions": "gdelt_mentions",
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


def _historical_folder(config: dict, path_key: str) -> str | None:
    """Return the historical directory path when partitioning is enabled, else None."""
    part_cfg = config.get("converter", {}).get("partitioning", {})
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


def run_scrape_cmd(config: dict, args: argparse.Namespace) -> None:
    _apply_verbosity(args)
    start_date = _parse_date(args.start_date, "--start-date") if args.start_date else None
    end_date = _parse_date(args.end_date, "--end-date") if args.end_date else None

    if start_date and end_date and start_date > end_date:
        raise ValueError(f"--start-date ({start_date}) must not be after --end-date ({end_date}).")

    dataset = _DATASET_CLI_TO_CONFIG[args.dataset]
    logger.info("Starting scraping stage...")
    result = run_scraping_pipeline(
        config, start_date=start_date, end_date=end_date, dataset=dataset,
        verbose=args.verbose, quiet=args.quiet, force=args.force,
    )
    logger.info("Scraping completed.")

    failed = result["failed"]
    if failed:
        raise RuntimeError(
            f"Scraping finished with {len(failed)} failed download(s): {', '.join(failed)}"
        )


def run_convert_cmd(config: dict, args: argparse.Namespace) -> None:
    _apply_verbosity(args)
    start_date = _parse_date(args.start_date, "--start-date") if args.start_date else None
    end_date = _parse_date(args.end_date, "--end-date") if args.end_date else None

    if start_date and end_date and start_date > end_date:
        raise ValueError(f"--start-date ({start_date}) must not be after --end-date ({end_date}).")

    dataset = _DATASET_CLI_TO_CONFIG[args.dataset]
    logger.info("Starting conversion stage...")
    outputs, failed = run_converter(
        config, dataset=dataset, start_date=start_date, end_date=end_date,
        delete_source=args.delete_source, verbose=args.verbose, quiet=args.quiet,
        force=args.force,
    )
    logger.info(f"Created {len(outputs)} parquet files.")

    if failed:
        raise RuntimeError(
            f"Conversion finished with {len(failed)} failed file(s): {', '.join(failed)}"
        )


def run_filter_cmd(config: dict, args: argparse.Namespace) -> None:
    _apply_verbosity(args)
    start_date = _parse_date(args.start_date, "--start-date") if args.start_date else None
    end_date = _parse_date(args.end_date, "--end-date") if args.end_date else None

    if start_date and end_date and start_date > end_date:
        raise ValueError(f"--start-date ({start_date}) must not be after --end-date ({end_date}).")

    dataset = _DATASET_CLI_TO_CONFIG[args.dataset]
    logger.info("Starting filtering stage...")
    files_processed, files_failed = run_filter(
        config, dataset=dataset, start_date=start_date, end_date=end_date,
        delete_source=args.delete_source, verbose=args.verbose, quiet=args.quiet,
        force=args.force,
    )
    logger.info("Filtering completed.")

    if files_failed:
        raise RuntimeError(
            f"Filtering finished with {files_failed} failed file(s) out of "
            f"{files_processed + files_failed}."
        )


def run_sampling_cmd(config: dict, args: argparse.Namespace) -> None:
    dataset = _DATASET_CLI_TO_CONFIG[args.dataset]
    source_key, historical_key = (
        ("filtered_data_directory", "filtered_historical_directory")
        if args.source == "filtered"
        else ("parquet_data_directory", "parquet_historical_directory")
    )
    source_key = dataset_path_key(dataset, source_key)
    historical_key = dataset_path_key(dataset, historical_key)
    source_folder = ensure_exists(config["paths"][source_key], source_key)

    out = Path(args.out)

    # Create parent folder if it does not exist
    out.parent.mkdir(parents=True, exist_ok=True)

    hist_folder = _historical_folder(config, historical_key)
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
        )
        df = sampler.get_random_sample(args.n)
        write_parquet_atomic(df, out)
        logger.info(f"Saved indexed sample ({len(df)} rows) -> {out}")
        return

    # -----------------------------
    # Daily Sampling
    # -----------------------------
    if args.mode == "daily":
        sampler = DailySampler(
            folder_path=str(source_folder),
            historical_folder=hist_folder,
            random_state=args.seed,
            columns=columns,
        )
        df = sampler.get_daily_samples(samples_per_day=args.per_day)
        write_parquet_atomic(df, out)
        logger.info(f"Saved daily sample ({len(df)} rows) -> {out}")
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
        )

        if args.stratify:
            if args.n_per_group is None:
                raise ValueError("--n-per-group is required when --stratify is set")
            df = sampler.get_stratified_sample(args.stratify, args.n_per_group)
            write_parquet_atomic(df, out)
            logger.info(
                f"Saved stratified sample ({len(df)} rows) "
                f"stratified by '{args.stratify}' ({args.n_per_group} per group) -> {out}"
            )
        else:
            df = sampler.get_random_sample(args.n)
            write_parquet_atomic(df, out)
            logger.info(
                f"Saved filtered sample ({len(df)} rows) "
                f"using filter={filter_dict} -> {out}"
            )
        return

    raise ValueError(f"Unknown sampling mode: {args.mode}")


def run_crossref_cmd(config: dict, args: argparse.Namespace) -> None:
    start_date = _parse_date(args.start_date, "--start-date") if args.start_date else None
    end_date = _parse_date(args.end_date, "--end-date") if args.end_date else None

    if start_date and end_date and start_date > end_date:
        raise ValueError(f"--start-date ({start_date}) must not be after --end-date ({end_date}).")

    events_df = read_parquet_path(args.events)
    columns = set(args.columns) if args.columns else None
    source_key = (
        "filtered_data_directory" if args.source == "filtered" else "parquet_data_directory"
    )

    out = Path(args.out)
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

    write_parquet_atomic(result, out)
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
        description="GdeltForge: a data pipeline for the GDELT 2.0 Events Database"
    )
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
        default="events",
        help="Which GDELT dataset to scrape (default: events)"
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

    # ----------------------------------------------------
    # convert
    # ----------------------------------------------------
    convert = subparsers.add_parser("convert", help="Convert raw data to parquet")
    convert.add_argument(
        "--dataset",
        choices=_DATASET_CHOICES,
        default="events",
        help="Which GDELT dataset to convert (default: events)"
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
        "--delete-source",
        action="store_true",
        help="Delete each source zip once its parquet output is written and confirmed done. "
             "Off by default. Only the zip; the intermediate extracted CSV is already "
             "removed unless converter.keep_unzipped is set. Combined with output_columns, "
             "the dropped columns can't be recovered without re-scraping"
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

    # ----------------------------------------------------
    # filter
    # ----------------------------------------------------
    filter_ = subparsers.add_parser("filter", help="Filter parquet files")
    filter_.add_argument(
        "--dataset",
        choices=_DATASET_CHOICES,
        default="events",
        help="Which GDELT dataset to filter (default: events)"
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

    # ----------------------------------------------------
    # sample
    # ----------------------------------------------------
    sample = subparsers.add_parser(
        "sample", help="Sampling utilities (indexed, filtered, daily)"
    )

    sample.add_argument(
        "--dataset",
        choices=_DATASET_CHOICES,
        default="events",
        help="Which GDELT dataset to sample from (default: events)"
    )
    sample.add_argument(
        "--mode",
        required=True,
        choices=["indexed", "filtered", "daily"],
        help="Sampling strategy"
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
        "--per-day", type=int, default=10,
        help="Rows per day (daily mode only)"
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
        "--out",
        default="sample.parquet",
        help="Output parquet file"
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
