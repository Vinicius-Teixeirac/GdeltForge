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
    "gdelt_gkg_v1": "gkg_v1_",
    "gdelt_gkg_v1_counts": "gkg_v1_counts_",
    "gdelt_gkg_v2": "gkg_v2_",
    "gdelt_mentions": "mentions_",
}


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


def get_dict(section: dict, key: str) -> dict:
    """
    section.get(key, {}), except an explicit `key: null` in the YAML is
    also treated as "use {}" instead of returned as None. dict.get's own
    default only applies when key is missing entirely; every optional,
    dict-valued config subsection this guards (converter.output_columns,
    converter.compression, converter.max_workers_by_dataset,
    converter.partitioning, filter.output_columns, filter.float32_columns,
    filter.compression) is routinely left blank while a user is still
    filling the config in, and YAML parses that as None, not {}. Every
    caller immediately chains a second .get(...) onto the result, which
    crashes with "'NoneType' object has no attribute 'get'" the instant
    that happens -- found in a real production log, once for the section
    a converter:/filter: line mid-edit produces (see
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
    converter.py, filter.py, cli.py) assumes a dict, so touching such a
    section used to crash with a bare "'NoneType' object has no attribute
    'get'", with no mention of which section or file was at fault.
    Replacing a None-valued top-level key with {} here makes every one of
    those calls fall through to its own .get(key, default) exactly as if
    the section had been omitted entirely, which is what an empty section
    actually means.
    """
    return {key: ({} if value is None else value) for key, value in config.items()}


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
    text = _BUNDLED_DEFAULT_RESOURCE.read_text(encoding="utf-8")
    config = _normalize_top_level_sections(yaml.safe_load(text))

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
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
    if path.exists():
        with open(path) as f:
            config = yaml.safe_load(f)
        if not config:
            raise ValueError(
                f"Config file is empty: {path}. Copy config/settings.example.yaml as a "
                f"starting point, or see docs/configuration.md."
            )
        return _normalize_top_level_sections(config)

    if not explicit:
        return _load_bundled_default(path)

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
