"""docs/api.md lists gdeltforge's supported Python names; each must exist,
so the page can't drift from the code."""

import importlib
import inspect
import re
from pathlib import Path

import pytest

from gdeltforge import cli

API_DOC = Path(__file__).resolve().parent.parent / "docs" / "api.md"


def _section(title: str) -> str:
    text = API_DOC.read_text(encoding="utf-8")
    start = text.index(f"## {title}\n")
    end = text.find("\n## ", start + 1)
    return text[start:end if end != -1 else None]


def _listed_names() -> list[tuple[str, str]]:
    rows = re.findall(r"^\| `(gdeltforge\.[\w.]+)` \| (.+) \|$", _section("Python names"), re.M)
    return [(module, name) for module, names in rows for name in re.findall(r"`(\w+)`", names)]


def test_the_page_lists_names():
    assert len(_listed_names()) >= 20


@pytest.mark.parametrize("module, name", _listed_names())
def test_every_listed_name_imports_from_its_module(module, name):
    obj = getattr(importlib.import_module(module), name)
    assert callable(obj)


def test_every_listed_method_exists():
    methods = re.findall(r"`([A-Z]\w+)\.([a-z]\w+)`", _section("Python names"))
    assert methods
    listed = {name: module for module, name in _listed_names()}
    for cls, method in methods:
        obj = getattr(importlib.import_module(listed[cls]), cls)
        assert inspect.isfunction(getattr(obj, method, None)), f"{cls}.{method}"


def test_the_dataset_names_are_the_config_names():
    section = _section("Python names")
    line = next(part for part in section.split("- **") if part.startswith("Datasets"))
    assert set(re.findall(r"`(gdelt_\w+)`", line)) == set(cli._DATASET_CLI_TO_CONFIG.values())
