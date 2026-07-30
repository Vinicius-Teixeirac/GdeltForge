# Getting Started

## Installation

GdeltForge isn't on PyPI yet, so install it from a clone using [uv](https://docs.astral.sh/uv/):

```
# Install uv (if not already installed)
pip install uv

git clone https://github.com/Vinicius-Teixeirac/GdeltForge.git
cd GdeltForge

# Create the virtual environment, install dependencies, and install
# GdeltForge itself (editable): this is what makes the `gdeltforge`
# command available.
uv sync

# Activate the virtual environment
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# Verify the CLI is installed
gdeltforge --help
```

`python main.py <command>` also works as a backward-compatible alias for the `gdeltforge` command, in case you have existing scripts built around it.

## Configuration

Copy the example config and adjust the paths for your machine:

```
cp config/settings.example.yaml config/settings.yaml
```

At minimum, review `paths:` (where downloaded/converted/filtered data will live) and `filter.columns_to_check` (which columns must be non-null for a row to survive filtering). See the full [Configuration](configuration.md) reference for every option.

## Your first pipeline run

Each stage is a separate command, run in order. A small, date-restricted run is the fastest way to confirm everything is wired up correctly:

```
gdeltforge scrape --start-date 2024-01-01 --end-date 2024-01-07
gdeltforge convert
gdeltforge filter
gdeltforge sample --mode indexed -n 1000 --out sample.parquet
```

This downloads one week of daily GDELT files, converts them to Parquet, drops rows missing your configured columns, and writes a 1,000-row random sample to `sample.parquet`.

Once that works, drop the date flags to work with the full archive (1979-present); see [CLI Reference](cli-reference.md) for every mode and flag.

## Running the test suite

The test suite is pure unit tests: no network access, no browser, no real GDELT data required.

```
uv sync --group dev
uv run pytest
```

## Building the docs locally

This site is built with [MkDocs](https://www.mkdocs.org/) + the [Material theme](https://squidfunk.github.io/mkdocs-material/).

```
uv sync --group docs
uv run mkdocs serve
```

Then open `http://127.0.0.1:8000/`.
