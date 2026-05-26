import argparse
import json
from datetime import date
from pathlib import Path

from utils.config import load_config
from utils.io import ensure_exists
from utils.logging import get_logger

# Pipeline stages
from scraping.scraper import run_scraping_pipeline
from conversion.converter import run_converter
from filtering.filter import run_filter

# Samplers
from sampling.samplers import (
    IndexedSampler,
    DailySampler,
    FilteredSampler,
)


# ======================================================================
# Utilities
# ======================================================================

logger = get_logger(__name__, log_to_file=True)

# ======================================================================
# Subcommand Runners
# ======================================================================

def _parse_date(value: str, arg_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"Invalid date for {arg_name}: '{value}'. Expected YYYY-MM-DD.")


def run_scrape_cmd(config: dict, args: argparse.Namespace) -> None:
    start_date = _parse_date(args.start_date, "--start-date") if args.start_date else None
    end_date = _parse_date(args.end_date, "--end-date") if args.end_date else None

    if start_date and end_date and start_date > end_date:
        raise ValueError(f"--start-date ({start_date}) must not be after --end-date ({end_date}).")

    logger.info("Starting scraping stage...")
    run_scraping_pipeline(config, start_date=start_date, end_date=end_date)
    logger.info("Scraping completed.")


def run_convert_cmd(config: dict) -> None:
    logger.info("Starting conversion stage...")
    outputs = run_converter(config)
    logger.info(f"Created {len(outputs)} parquet files.")


def run_filter_cmd(config: dict) -> None:
    logger.info("Starting filtering stage...")
    run_filter(config)
    logger.info("Filtering completed.")


def run_sampling_cmd(config: dict, args: argparse.Namespace) -> None:
    filtered_folder = ensure_exists(
        config["paths"]["filtered_data_directory"],
        "filtered_data_directory"
    )

    out = Path(args.out)

    # Create parent folder if it does not exist
    out.parent.mkdir(parents=True, exist_ok=True)

    # -----------------------------
    # Indexed Sampling
    # -----------------------------
    if args.mode == "indexed":
        sampler = IndexedSampler(folder_path=str(filtered_folder),
                                 random_state=args.seed)
        df = sampler.get_random_sample(args.n)
        df.to_parquet(out)
        logger.info(f"Saved indexed sample ({len(df)} rows) -> {out}")
        return

    # -----------------------------
    # Daily Sampling
    # -----------------------------
    if args.mode == "daily":
        sampler = DailySampler(folder_path=str(filtered_folder),
                               random_state=args.seed)
        df = sampler.get_daily_samples(samples_per_day=args.per_day)
        df.to_parquet(out)
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
            raise ValueError(f"Invalid JSON passed to --filter: {e}")

        sampler = FilteredSampler(
            folder_path=str(filtered_folder),
            gdelt_columns=config["columns"]["gdelt_event"],
            columns=set(args.columns) if args.columns else None,
            filter_dict=filter_dict,
            random_state=args.seed
        )

        df = sampler.get_random_sample(args.n)
        df.to_parquet(out)
        logger.info(
            f"Saved filtered sample ({len(df)} rows) "
            f"using filter={filter_dict} -> {out}"
        )
        return

    raise ValueError(f"Unknown sampling mode: {args.mode}")


# ======================================================================
# Argument Parser
# ======================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="GDELT Pipeline Orchestrator"
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
        help="Columns to include (filtered sampler)"
    )
    sample.add_argument(
        "--out",
        default="sample.parquet",
        help="Output parquet file"
    )

    return parser


# ======================================================================
# Entrypoint
# ======================================================================

def main() -> None:
    config = load_config()

    parser = build_parser()
    args = parser.parse_args()

    logger.info(f"Running command: {args.command}")

    if args.command == "scrape":
        run_scrape_cmd(config, args)

    elif args.command == "convert":
        run_convert_cmd(config)

    elif args.command == "filter":
        run_filter_cmd(config)

    elif args.command == "sample":
        run_sampling_cmd(config, args)


if __name__ == "__main__":
    main()
