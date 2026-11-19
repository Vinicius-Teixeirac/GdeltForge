"""
launcher.py

The gdeltforge command's entry point, for both the installed console
script and `python -m gdeltforge`. It runs before polars is imported:
polars reads POLARS_MAX_THREADS once, when it loads, so this is the
last moment that variable can still size this process's thread pool.

sample and crossref scan many files in this process.
POLARS_MAX_CONCURRENT_SCANS (io.max_concurrent_reads, applied in
cli.main) bounds their data reads, but polars fetches the files'
footers on its thread pool, whatever that variable says. With
io.max_concurrent_reads set, those two commands therefore start polars
with at most that many threads, which bounds their footer reads the
way POLARS_MAX_CONCURRENT_SCANS bounds their data reads. A
POLARS_MAX_THREADS the user exported is left as it is; cli.main warns
when it exceeds the cap.

polars also reads POLARS_FORCE_ASYNC only when it loads, and with it
set reads Parquet without memory-mapping the files (io.memory_map, see
gdeltforge.utils.storage). Every command that reads Parquet is started
with it when the config says not to map; the worker processes clean
and aggregate spawn inherit it.
"""

import os
import sys
from collections.abc import Mapping
from pathlib import Path

import yaml

from gdeltforge.utils.config import CONFIG_ENV_VAR, DEFAULT_CONFIG_PATH, peek_config
from gdeltforge.utils.storage import POLARS_FORCE_ASYNC, memory_map_decision

# The commands that read many files in the CLI's own process.
_SCANNING_COMMANDS = frozenset({"sample", "crossref"})
# The commands that read Parquet, in this process or in their workers.
_PARQUET_READING_COMMANDS = frozenset({"clean", "filter", "aggregate", "sample", "crossref"})


def _command_and_config(argv: list[str]) -> tuple[str | None, str | None]:
    """The subcommand and the --config value of a command line, read
    without argparse (whose parser lives in gdeltforge.cli, which imports
    polars). Options before the subcommand are skipped."""
    config_path = None
    args = iter(argv)
    for arg in args:
        if arg == "--config":
            config_path = next(args, None)
        elif arg.startswith("--config="):
            config_path = arg.split("=", 1)[1]
        elif not arg.startswith("-"):
            return arg, config_path
    return None, config_path


def force_async_reads(argv: list[str], environ: Mapping[str, str]) -> bool:
    """
    Whether to start this command line with POLARS_FORCE_ASYNC=1, so that
    polars reads Parquet without memory-mapping it. Only for the commands
    that read Parquet, and never over a value the user exported. The
    config is read as load_config would read it (bundled defaults filled
    in), without its side effects; anything unexpected (no file, invalid
    YAML, an invalid io.memory_map) returns False and is left for the
    command's own config validation to report.
    """
    command, config_path = _command_and_config(argv)
    if command not in _PARQUET_READING_COMMANDS or POLARS_FORCE_ASYNC in environ:
        return False
    config = peek_config(config_path, environ)
    if config is None:
        return False
    try:
        mapped, _ = memory_map_decision(config, environ)
    except ValueError:
        return False
    return not mapped


def scan_thread_cap(argv: list[str], environ: Mapping[str, str]) -> int | None:
    """
    The polars thread count to start this command line with, or None to
    leave polars' default. Reads only io.max_concurrent_reads, from the
    same file load_config will use (--config, then GDELTFORGE_CONFIG,
    then ./config/settings.yaml). Anything unexpected (no file, invalid
    YAML, an invalid value) returns None and is left for load_config's
    own validation to report once the command runs.
    """
    if "POLARS_MAX_THREADS" in environ:
        return None
    command, config_path = _command_and_config(argv)
    if command not in _SCANNING_COMMANDS:
        return None
    path = Path(config_path or environ.get(CONFIG_ENV_VAR) or DEFAULT_CONFIG_PATH)
    try:
        with open(path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except (OSError, UnicodeDecodeError, yaml.YAMLError):
        return None
    io = config.get("io") if isinstance(config, dict) else None
    cap = io.get("max_concurrent_reads") if isinstance(io, dict) else None
    if isinstance(cap, bool) or not isinstance(cap, int) or cap < 1:
        return None
    return cap


def main() -> None:
    cap = scan_thread_cap(sys.argv[1:], os.environ)
    if cap is not None:
        os.environ["POLARS_MAX_THREADS"] = str(cap)
    if force_async_reads(sys.argv[1:], os.environ):
        os.environ[POLARS_FORCE_ASYNC] = "1"
    # Imported only now: gdeltforge.cli imports polars.
    from gdeltforge.cli import main as cli_main

    cli_main()
