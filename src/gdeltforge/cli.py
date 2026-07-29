import argparse
import json
import sys
from datetime import date
from pathlib import Path

from gdeltforge.conversion.converter import run_converter
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
from gdeltforge.utils.config import load_config
from gdeltforge.utils.io import ensure_exists, write_parquet_atomic
from gdeltforge.utils.logging import get_logger

# ======================================================================
# Utilities
# ======================================================================

logger = get_logger(__name__, log_to_file=True)


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


def run_scrape_cmd(config: dict, args: argparse.Namespace) -> None:
    start_date = _parse_date(args.start_date, "--start-date") if args.start_date else None
    end_date = _parse_date(args.end_date, "--end-date") if args.end_date else None

    if start_date and end_date and start_date > end_date:
        raise ValueError(f"--start-date ({start_date}) must not be after --end-date ({end_date}).")

    logger.info("Starting scraping stage...")
    result = run_scraping_pipeline(config, start_date=start_date, end_date=end_date)
    logger.info("Scraping completed.")

    failed = result["failed"]
    if failed:
        raise RuntimeError(
            f"Scraping finished with {len(failed)} failed download(s): {', '.join(failed)}"
        )


def run_convert_cmd(config: dict) -> None:
    logger.info("Starting conversion stage...")
    outputs, failed = run_converter(config)
    logger.info(f"Created {len(outputs)} parquet files.")

    if failed:
        raise RuntimeError(
            f"Conversion finished with {len(failed)} failed file(s): {', '.join(failed)}"
        )


def run_filter_cmd(config: dict) -> None:
    logger.info("Starting filtering stage...")
    files_processed, files_failed = run_filter(config)
    logger.info("Filtering completed.")

    if files_failed:
        raise RuntimeError(
            f"Filtering finished with {files_failed} failed file(s) out of "
            f"{files_processed + files_failed}."
        )


def run_sampling_cmd(config: dict, args: argparse.Namespace) -> None:
    source_key, historical_key = (
        ("filtered_data_directory", "filtered_historical_directory")
        if args.source == "filtered"
        else ("parquet_data_directory", "parquet_historical_directory")
    )
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
            gdelt_columns=config["columns"]["gdelt_event"],
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


_CAMEO_COLUMN_GROUPS = [
    cameo_codes.CAMEO_ACTOR_COUNTRY_COLUMNS,
    cameo_codes.FIPS_GEO_COLUMNS,
    cameo_codes.CAMEO_ETHNIC_COLUMNS,
    cameo_codes.CAMEO_KNOWN_GROUP_COLUMNS,
    cameo_codes.CAMEO_RELIGION_COLUMNS,
    cameo_codes.CAMEO_TYPE_COLUMNS,
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
        "--start-date",
        metavar="YYYY-MM-DD",
        help="Only download files whose period starts on or after this date",
    )
    scrape.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        help="Only download files whose period ends on or before this date",
    )

    # ----------------------------------------------------
    # convert
    # ----------------------------------------------------
    subparsers.add_parser("convert", help="Convert raw data to parquet")

    # ----------------------------------------------------
    # filter
    # ----------------------------------------------------
    subparsers.add_parser("filter", help="Filter parquet files")

    # ----------------------------------------------------
    # sample
    # ----------------------------------------------------
    sample = subparsers.add_parser(
        "sample", help="Sampling utilities (indexed, filtered, daily)"
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
            run_convert_cmd(config)

        elif args.command == "filter":
            run_filter_cmd(config)

        elif args.command == "sample":
            run_sampling_cmd(config, args)

    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
