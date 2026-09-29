import os
from importlib.resources import files
from pathlib import Path

import yaml

from gdeltforge.utils.logging import get_logger

# get_logger, not a bare logging.getLogger(__name__): every other module
# in this codebase goes through it, which eagerly attaches a formatted
# StreamHandler (timestamp, level, logger name) at import time. Skipping
# it here meant this module's warnings only ever reached the terminal
# via Python's own unconfigured-logger fallback (logging.lastResort),
# which does print to stderr, so nothing was silently lost, but with no
# formatting at all: no "WARNING" label, no timestamp, indistinguishable
# from ordinary output and inconsistent with every other warning this
# tool emits.
logger = get_logger(__name__)

CONFIG_ENV_VAR = "GDELTFORGE_CONFIG"
DEFAULT_CONFIG_PATH = "config/settings.yaml"

# The package's own bundled fallback, read via importlib.resources so it
# works identically whether gdeltforge is running from an editable clone
# or a wheel installed by pip (e.g. `pip install gdeltforge` in a fresh
# Colab cell, which drops nothing into the working directory the way a
# git clone's config/settings.example.yaml does). Deliberately a
# different, more conservative file than settings.example.yaml, not the
# same content under a new name; see that file's own header for why.
_BUNDLED_DEFAULT_RESOURCE = files("gdeltforge").joinpath("config/default_settings.yaml")

# Maps each dataset's config key (under columns / columns_numeric) to the
# prefix its paths.* keys use. Events keeps its original, unprefixed keys
# (e.g. "downloaded_data_directory") for backward compatibility; other
# datasets get a prefixed sibling key (e.g. "gkg_v2_downloaded_data_directory").
_DATASET_PATH_PREFIXES = {
    "gdelt_event": "",
    "gdelt_event_15min": "event_15min_",
    "gdelt_event_reduced": "event_reduced_",
    "gdelt_gkg_v1": "gkg_v1_",
    "gdelt_gkg_v1_counts": "gkg_v1_counts_",
    "gdelt_gkg_v2": "gkg_v2_",
    "gdelt_mentions": "mentions_",
}

# Every dataset's config name, for settings keyed by dataset.
DATASET_NAMES = tuple(_DATASET_PATH_PREFIXES)

# Datasets whose converted output is always Hive-partitioned, never flat:
# unlike Events' pre-2013 yearly/monthly archives (opt-in via
# converter.partitioning.enabled, alongside its own flat daily files),
# gdelt_event_reduced has no per-day source files at all, it's one static
# file, and Year (derived from its own Date column, not present in the
# raw file) is its only meaningful partition key. clean/sample must
# resolve this dataset's historical directory unconditionally, independent
# of the global converter.partitioning.enabled toggle, which only controls
# Events' own opt-in split.
_ALWAYS_HISTORICAL_DATASETS = frozenset({"gdelt_event_reduced"})


def dataset_is_always_historical(dataset: str) -> bool:
    return dataset in _ALWAYS_HISTORICAL_DATASETS


# The three datasets discovered from GDELT's 15-minute gdeltv2 master file
# list (see scraper.py's _GDELT_V2_SUFFIXES): the only ones where a single
# calendar day is routinely split across ~96 separate files, which is what
# `aggregate` (concatenating a period's worth of them into one larger file)
# and `sample --source aggregated` exist to address. Every other dataset
# already publishes at day-or-coarser granularity, so there's no many-
# small-files problem for aggregation to solve for it.
_AGGREGATION_ELIGIBLE_DATASETS = frozenset({
    "gdelt_gkg_v2", "gdelt_mentions", "gdelt_event_15min",
})


def dataset_is_aggregation_eligible(dataset: str) -> bool:
    return dataset in _AGGREGATION_ELIGIBLE_DATASETS


def dataset_path_key(dataset: str, base_key: str) -> str:
    """
    Map a dataset name and a base paths.* key (e.g. "downloaded_data_directory")
    to that dataset's actual config key, e.g.
    dataset_path_key("gdelt_gkg_v2", "downloaded_data_directory")
    -> "gkg_v2_downloaded_data_directory".
    """
    try:
        prefix = _DATASET_PATH_PREFIXES[dataset]
    except KeyError:
        raise ValueError(
            f"Unknown dataset {dataset!r}. Known datasets: "
            f"{', '.join(_DATASET_PATH_PREFIXES)}"
        ) from None
    return f"{prefix}{base_key}"


def validate_max_workers(value: int | None, label: str) -> int | None:
    """
    Check a resolved converter.max_workers/clean.max_workers value
    before it's ever logged or handed to ProcessPoolExecutor.

    None means "let ProcessPoolExecutor pick os.cpu_count() on its own",
    a real, valid, and by far the most common configuration (the default,
    unset value); it's returned unchanged. Anything else must be a
    positive int. This used to be checked only implicitly, by
    ProcessPoolExecutor's own constructor, well after a pre-flight log
    line had already announced a *different*, already-resolved worker
    count for the same run: max_workers: 0 is falsy in Python, so a
    "config_value or cpu_count()"-style fallback silently took the same
    branch a genuinely unset value would, logging the real CPU count
    convert/clean would use, e.g. 32, one line before ProcessPoolExecutor
    raised its own "max_workers must be greater than 0" against the
    original, still-0 value. Checking explicitly here, before either the
    log line or the executor ever see the value, makes 0 (and any other
    non-positive value) fail immediately with one clear error instead of
    two contradictory statements about the same run.

    The value comes straight from YAML, so it can be any scalar. true is
    an int in Python and would run as one worker, logged as "True worker
    process(es)"; "4" failed the comparison below with a bare TypeError;
    2.5 was logged and then failed inside the pool. Only an int is a
    worker count.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"{label} must be a whole number greater than 0, or null; got {value!r}"
        )
    if value <= 0:
        raise ValueError(f"{label} must be greater than 0, got {value}")
    return value


def resolve_max_concurrent_reads(config: dict) -> int | None:
    """
    io.max_concurrent_reads: how many files one clean/aggregate/sample/
    crossref command may read at once, or None (the default) for no cap
    beyond each stage's own worker count. Validated the same way as
    max_workers, since 0 is just as falsy and just as meaningless here.
    """
    return validate_max_workers(
        get_dict(config, "io").get("max_concurrent_reads"), "io.max_concurrent_reads"
    )


def get_dict(section: dict, key: str) -> dict:
    """
    section.get(key, {}), except an explicit `key: null` in the YAML is
    also treated as "use {}" instead of returned as None. dict.get's own
    default only applies when key is missing entirely; every optional,
    dict-valued config subsection this guards (converter.output_columns,
    converter.compression, converter.max_workers_by_dataset,
    converter.partitioning, clean.output_columns, clean.float32_columns,
    clean.compression) is routinely left blank while a user is still
    filling the config in, and YAML parses that as None, not {}. Every
    caller immediately chains a second .get(...) onto the result, which
    crashes with "'NoneType' object has no attribute 'get'" the instant
    that happens -- found in a real production log, once for the section
    a converter:/clean: line mid-edit produces (see
    _normalize_top_level_sections above), and independently again here,
    one level deeper, for the same reason on a subsection instead of a
    top-level section.
    """
    value = section.get(key)
    return value if value is not None else {}


def _normalize_top_level_sections(config: dict) -> dict:
    """
    A top-level section key present in the YAML but with nothing indented
    under it (e.g. `converter:` immediately followed by the next key, or
    an explicit `converter: null`) parses to None, not {}. Every
    downstream `config["converter"].get(...)`-style call (scraper.py,
    converter.py, cleaner.py, cli.py) assumes a dict, so touching such a
    section used to crash with a bare "'NoneType' object has no attribute
    'get'", with no mention of which section or file was at fault.
    Replacing a None-valued top-level key with {} here makes every one of
    those calls fall through to its own .get(key, default) exactly as if
    the section had been omitted entirely, which is what an empty section
    actually means.
    """
    return {key: ({} if value is None else value) for key, value in config.items()}


def _warn_about_empty_sections(config: dict, source: Path) -> None:
    """
    An empty (null) top-level section loads as if it were absent: every
    setting in it is the bundled default. That is rarely what an edited
    file means (settings lost to an indentation slip, or all commented
    out), and for paths it points every stage at ./data under whatever
    directory gdeltforge runs in, which on a shared machine can hold
    someone else's data. Say so once per section.
    """
    for key, value in config.items():
        if value is not None:
            continue
        where = (
            " Its default data directories are relative to the directory gdeltforge "
            "runs in."
            if key == "paths" else ""
        )
        logger.warning(
            f"{key} is empty (null) in {source}, so every {key} setting is the bundled "
            f"default.{where} Remove the key if that is what you want, or indent the "
            f"settings under it."
        )


# Pre-0.12 names of the clean stage, when it was called `filter`: the
# top-level config section and the path keys' base names. Translated on
# load with a warning, so an existing settings.yaml keeps working through
# the 0.12.x series.
_DEPRECATED_SECTION_NAMES = {"filter": "clean"}
_DEPRECATED_PATH_SUFFIXES = {
    "filtered_data_directory": "cleaned_data_directory",
    "filtered_historical_directory": "cleaned_historical_directory",
}


def _migrate_deprecated_names(config: dict, source: Path) -> dict:
    """
    Translate the clean stage's pre-0.12 config names to the current ones:
    the `filter:` section to `clean:`, and every `*filtered_data_directory`/
    `*filtered_historical_directory` path key (dataset-prefixed ones
    included) to its `*cleaned_*` equivalent. Each translation logs a
    deprecation warning naming the file. A config that sets both the old
    and the new name for the same thing is ambiguous and raises, naming
    both, since silently preferring either could point the stage at the
    wrong directory.
    """
    migrated = dict(config)
    for old, new in _DEPRECATED_SECTION_NAMES.items():
        if old not in migrated:
            continue
        if new in migrated:
            raise ValueError(
                f"{source} sets both `{old}:` and `{new}:`. `{old}:` is the pre-0.12 "
                f"name of `{new}:`; keep only `{new}:`."
            )
        migrated[new] = migrated.pop(old)
        logger.warning(
            f"{source}: the `{old}:` section is deprecated, read as `{new}:`. "
            f"Rename it; the old name will stop working in a future release."
        )

    paths = migrated.get("paths")
    if isinstance(paths, dict):
        paths = dict(paths)
        for key in list(paths):
            for old_suffix, new_suffix in _DEPRECATED_PATH_SUFFIXES.items():
                if not key.endswith(old_suffix):
                    continue
                new_key = key[: -len(old_suffix)] + new_suffix
                if new_key in paths:
                    raise ValueError(
                        f"{source} sets both paths.{key} and paths.{new_key}. "
                        f"paths.{key} is the pre-0.12 name; keep only paths.{new_key}."
                    )
                paths[new_key] = paths.pop(key)
                logger.warning(
                    f"{source}: paths.{key} is deprecated, read as paths.{new_key}. "
                    f"Rename it; the old name will stop working in a future release."
                )
        migrated["paths"] = paths
    return migrated


def _warn_about_moved_default_directories(config: dict, user_paths: dict) -> None:
    """
    0.12.0 moved the bundled default's clean-stage directories from
    data/<dataset>/filtered to data/<dataset>/cleaned. A config that leaves
    those paths to the default, run from a directory holding output from an
    earlier version, no longer reads that output: clean skips every file
    already cleaned under unchanged settings (every dataset without an
    errata rule), so nothing is written to the new location, and sample
    and aggregate read only what was cleaned after the upgrade. Warn,
    naming both directories, for as long as the old one holds Parquet
    files. Whether the new one exists says nothing: any clean run creates
    it, and it can hold part of the data. The check stops at the first
    file found, so it costs a directory stat and one entry even on a
    large network share.
    """
    suffixes = tuple(_DEPRECATED_PATH_SUFFIXES.values())
    for key, value in get_dict(config, "paths").items():
        if key in user_paths or not key.endswith(suffixes) or not isinstance(value, str):
            continue
        new = Path(value)
        if not new.name.startswith("cleaned"):
            continue
        old = new.with_name("filtered" + new.name[len("cleaned"):])
        if not old.is_dir() or next(old.rglob("*.parquet"), None) is None:
            continue
        fix = f"move its files into {new}" if new.exists() else f"rename it to {new}"
        logger.warning(
            f"{old} holds clean-stage output from before 0.12.0, but "
            f"paths.{key} now defaults to {new}: {fix}, or set paths.{key}: {old}. "
            f"Until then sample and aggregate don't read those files, and clean "
            f"doesn't rewrite files it already cleaned under the same settings."
        )


def _bundled_default_dict() -> dict:
    """
    Parse GdeltForge's own bundled default config (see
    _BUNDLED_DEFAULT_RESOURCE above), normalized the same way a real config
    file is, with none of _load_bundled_default's file-write/warning side
    effects. Used both as the tier-4 fallback itself and as the fallback
    layer merged under a real, user-supplied config (see
    _deep_merge_defaults), so both paths read the exact same source.
    """
    text = _BUNDLED_DEFAULT_RESOURCE.read_text(encoding="utf-8")
    return _normalize_top_level_sections(yaml.safe_load(text))


def _deep_merge_defaults(config: dict, defaults: dict) -> dict:
    """
    Fill in any key missing from `config` with the equivalent value from
    `defaults`, recursing into nested dicts so a hand-written settings.yaml
    only needs to specify what it actually wants to change.

    A real user config that omits a whole section entirely (columns,
    columns_numeric) or a nested one (clean.columns_to_check,
    converter.output_columns, a specific dataset under paths) used to crash
    deep inside converter.py/cleaner.py/cli.py with a bare KeyError naming
    just the missing key: every one of those reads its section with a
    direct config["..."][...] access, not .get(), on the assumption the
    section is always present the way the bundled default always has it.
    _normalize_top_level_sections already covers a section being present
    but null; this covers it being absent, arguably the more natural
    mistake for someone writing a small, targeted config instead of
    starting from the full settings.example.yaml.

    A key already present in `config` is never touched, regardless of its
    type: the user's own value always wins over the default. Only a dict
    value recurses; a list or scalar default is used as-is when the key is
    missing, never merged element-by-element (so a user's own, possibly
    empty, columns_to_check list for one dataset is never padded with the
    default's entries for that same dataset).
    """
    merged = dict(config)
    for key, default_value in defaults.items():
        if key not in merged:
            merged[key] = default_value
        elif isinstance(merged[key], dict) and isinstance(default_value, dict):
            merged[key] = _deep_merge_defaults(merged[key], default_value)
    return merged


def _load_bundled_default(path: Path) -> dict:
    """
    Read GdeltForge's own built-in fallback config (bundled inside the
    installed package, see _BUNDLED_DEFAULT_RESOURCE above) and try to
    materialize it at `path`, so it becomes a normal, editable file for
    the rest of this session instead of a config that's only ever read
    from memory. The write is best-effort: a read-only working directory
    (some sandboxes, some CI setups) still gets a working, in-memory-only
    config rather than failing outright, just without the "now edit
    config/settings.yaml" convenience.
    """
    config = _bundled_default_dict()

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # The raw resource text, not a re-serialization of `config`: writing
        # the parsed-and-rebuilt dict would drop every comment in the
        # bundled file, and dict ordering isn't guaranteed to round-trip
        # through YAML the same way.
        path.write_text(_BUNDLED_DEFAULT_RESOURCE.read_text(encoding="utf-8"), encoding="utf-8")
        logger.warning(
            f"No config found (no --config, no {CONFIG_ENV_VAR}, nothing at {path}); "
            f"using GdeltForge's built-in default and writing it to {path}, so it's a "
            f"normal file you can edit for the rest of this session. It's deliberately "
            f"conservative (no row/column filtering); see settings.example.yaml for a "
            f"heavily-annotated starting point, or docs/configuration.md for what every "
            f"key does."
        )
    except OSError as e:
        logger.warning(
            f"No config found (no --config, no {CONFIG_ENV_VAR}, nothing at {path}); "
            f"using GdeltForge's built-in default in memory only, since it could not be "
            f"written to {path} ({e}). See docs/configuration.md."
        )

    return config


def load_config(config_path: str | None = None) -> dict:
    """
    Resolve and load the pipeline config.

    Resolution order:
      1. `config_path` argument (e.g. from --config)
      2. GDELTFORGE_CONFIG environment variable
      3. ./config/settings.yaml relative to the current working directory
      4. GdeltForge's own bundled default (see _load_bundled_default),
         only when neither 1 nor 2 was given at all

    This lets the installed `gdeltforge` command be pointed at a config
    file anywhere on disk, not just when run from inside the repo. Tier
    4 only activates when the caller gave no explicit signal (no
    --config, no GDELTFORGE_CONFIG) and the default path is also empty:
    a real `pip install gdeltforge` (e.g. a fresh Colab session, which
    loses any locally-created file the moment the session resets, with
    no config/settings.example.yaml to copy from since pip installs
    don't put repo files in the working directory) previously hit a hard
    FileNotFoundError here on every single run. An explicit --config or
    GDELTFORGE_CONFIG pointing at a path that turns out to be missing
    still raises: that's almost always a typo, not "please use the
    built-in default instead", so it isn't silently substituted.
    """
    explicit = config_path is not None or CONFIG_ENV_VAR in os.environ
    if config_path is None:
        config_path = os.environ.get(CONFIG_ENV_VAR, DEFAULT_CONFIG_PATH)

    path = Path(config_path)
    # Checked ahead of path.exists() below, which is true for a directory
    # too: open(path) on one raises a raw, unformatted OSError straight
    # from the filesystem ("[Errno 21] Is a directory: '...'"), unlike
    # every other malformed-config case here (missing file, empty file,
    # invalid YAML), which all get a clear, crafted message.
    if path.is_dir():
        raise IsADirectoryError(
            f"Config path is a directory, not a file: {path}. Point --config "
            f"or {CONFIG_ENV_VAR} at the settings.yaml file itself."
        )
    if path.exists():
        with open(path) as f:
            try:
                config = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise ValueError(
                    f"Config file at {path} contains invalid YAML: {e}"
                ) from e
        if not config:
            raise ValueError(
                f"Config file is empty: {path}. Copy config/settings.example.yaml as a "
                f"starting point, or see docs/configuration.md."
            )
        _warn_about_empty_sections(config, path)
        config = _migrate_deprecated_names(_normalize_top_level_sections(config), path)
        user_paths = get_dict(config, "paths")
        config = _deep_merge_defaults(config, _bundled_default_dict())
        _warn_about_moved_default_directories(config, user_paths)
        return config

    if not explicit:
        config = _load_bundled_default(path)
        _warn_about_moved_default_directories(config, {})
        return config

    example_url = (
        "https://github.com/Vinicius-Teixeirac/GdeltForge/blob/main/"
        "config/settings.example.yaml"
    )
    raise FileNotFoundError(
        f"Config file not found: {path}. "
        f"Copy config/settings.example.yaml to config/settings.yaml and adjust the paths "
        f"(no local clone? download it from {example_url}), "
        f"or point to an existing config via --config or the {CONFIG_ENV_VAR} "
        f"environment variable. Omit both to use GdeltForge's built-in default instead."
    )
