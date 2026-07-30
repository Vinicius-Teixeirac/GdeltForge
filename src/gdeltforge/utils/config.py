import os
from pathlib import Path

import yaml

CONFIG_ENV_VAR = "GDELTFORGE_CONFIG"
DEFAULT_CONFIG_PATH = "config/settings.yaml"


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
