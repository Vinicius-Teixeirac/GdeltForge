from pathlib import Path

import yaml


def load_config(config_path: str = "config/settings.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: {path}. "
            f"Copy config/settings.example.yaml to config/settings.yaml and adjust the paths."
        )
    with open(path, "r") as f:
        return yaml.safe_load(f)