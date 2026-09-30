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
"""

import os
import sys
from collections.abc import Mapping
from pathlib import Path

import yaml

from gdeltforge.utils.config import CONFIG_ENV_VAR, DEFAULT_CONFIG_PATH

# The commands that read many files in the CLI's own process.
_SCANNING_COMMANDS = frozenset({"sample", "crossref"})


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
    config_path = None
    command = None
    args = iter(argv)
    for arg in args:
        if arg == "--config":
            config_path = next(args, None)
        elif arg.startswith("--config="):
            config_path = arg.split("=", 1)[1]
        elif not arg.startswith("-"):
            command = arg
            break
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
    # Imported only now: gdeltforge.cli imports polars.
    from gdeltforge.cli import main as cli_main

    cli_main()
