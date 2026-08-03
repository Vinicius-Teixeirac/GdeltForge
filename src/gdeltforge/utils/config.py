import os
from pathlib import Path

import yaml

CONFIG_ENV_VAR = "GDELTFORGE_CONFIG"
DEFAULT_CONFIG_PATH = "config/settings.yaml"

# Maps each dataset's config key (under columns / columns_numeric) to the
# prefix its paths.* keys use. Events keeps its original, unprefixed keys
# (e.g. "downloaded_data_directory") for backward compatibility; other
# datasets get a prefixed sibling key (e.g. "gkg_v2_downloaded_data_directory").
_DATASET_PATH_PREFIXES = {
    "gdelt_event": "",
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


def load_config(config_path: str | None = None) -> dict:
    """
    Resolve and load the pipeline config.

    Resolution order:
      1. `config_path` argument (e.g. from --config)
      2. GDELTFORGE_CONFIG environment variable
      3. ./config/settings.yaml relative to the current working directory

    This lets the installed `gdeltforge` command be pointed at a config
    file anywhere on disk, not just when run from inside the repo.
    """
    if config_path is None:
        config_path = os.environ.get(CONFIG_ENV_VAR, DEFAULT_CONFIG_PATH)

    path = Path(config_path)
    if not path.exists():
        example_url = (
            "https://github.com/Vinicius-Teixeirac/GdeltForge/blob/main/"
            "config/settings.example.yaml"
        )
        raise FileNotFoundError(
            f"Config file not found: {path}. "
            f"Copy config/settings.example.yaml to config/settings.yaml and adjust the paths "
            f"(no local clone? download it from {example_url}), "
            f"or point to an existing config via --config or the {CONFIG_ENV_VAR} "
            f"environment variable."
        )
    with open(path) as f:
        return yaml.safe_load(f)
