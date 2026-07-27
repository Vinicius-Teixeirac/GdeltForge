<p align="center">
  <img src="docs/assets/emblem.png" alt="GdeltForge emblem: a world map globe resting on an anvil" width="360">
</p>

# GdeltForge

### Forges the raw GDELT 2.0 Events archive into clean, reproducibly-sampled Parquet

📖 **[Full documentation](https://vinicius-teixeirac.github.io/GdeltForge/)**

GdeltForge is a lightweight but scalable data pipeline to extract, transform, and load the entire **GDELT 2.0 Events Database**.
It is designed for research workflows requiring:

- Large-scale event data  
- Efficient historical storage (**Parquet**)  
- Reproducible sampling  
- Transparent and modular data lineage  

The architecture emphasizes **simplicity**, **efficiency**, and **explicit execution**: each stage can be run independently or reused in larger workflows.

---

# Stack

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![uv](https://img.shields.io/badge/uv-package_manager-DE5FE9?style=for-the-badge)](https://docs.astral.sh/uv/)
[![PyArrow](https://img.shields.io/badge/PyArrow-parquet_%26_datasets-FF6F00?style=for-the-badge)](https://arrow.apache.org/docs/python/)
[![pandas](https://img.shields.io/badge/pandas-dataframes-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![NumPy](https://img.shields.io/badge/NumPy-vectorized_sampling-013243?style=for-the-badge&logo=numpy&logoColor=white)](https://numpy.org/)
[![Requests](https://img.shields.io/badge/Requests-web_scraping-000000?style=for-the-badge&logo=python&logoColor=white)](https://requests.readthedocs.io/)
[![Selenium](https://img.shields.io/badge/Selenium-optional_fallback-43B02A?style=for-the-badge&logo=selenium&logoColor=white)](https://selenium-python.readthedocs.io/)

# 1. Challenges with Official Access Methods

The GDELT Events Database is extremely rich, but extracting the full archive through official methods is difficult. This pipeline solves the following issues:

## 1.1 GDELT API Limitations
The standard GDELT APIs are optimized for *recent* and *small* queries. They impose strict limits on:

- time windows (often only the last 3 months)
- maximum rows per query (≈ 250)
- request rate limits

These constraints make it effectively impossible to retrieve the **full historical dataset**.

## 1.2 Google BigQuery Mirror Constraints
While BigQuery provides full access, it suffers from practical issues:

- free-tier quotas (1TB/month query, 10GB storage, 1GB egress) are far too small for the ~hundreds of GB required
- full-table scans are expensive without billing
- some researchers prefer *local* offline workflows

## 1.3 Raw Bulk Downloads Are Available, But Hard to Use  
The [raw GDELT 2.0 archives](https://data.gdeltproject.org/events/) contain **thousands of files**.  
Processing them requires:

- automated downloading  
- streaming or chunked processing  
- efficient columnar storage (Parquet)  
- memory-safe filtering and sampling  

This pipeline automates these steps end-to-end.

---

# 2. Pipeline Design Principles

The pipeline follows a **single-responsibility, single-stage execution model**:

Each command performs *exactly one* transformation.

### Stages
scrape: download raw GDELT CSV files
convert: transform CSV -> Parquet
filter: remove rows with missing values
sample: reproducibly sample from Parquet files


This design is intentionally **transparent and low-magic**:

- every operation is explicit  
- no hidden steps  
- each module is individually testable  
- any stage can be re-run without affecting others  

### High-Level Architecture

```
┌─────────────┐      ┌────────────┐      ┌─────────────┐      ┌─────────────┐

│   Scraper   │ ---> │ Converter  │ ---> │   Filter    │ ---> │   Sampler   │

└─────────────┘      └────────────┘      └─────────────┘      └─────────────┘
      CSV               Parquet            Cleaned data         Sampled data

```

Each stage consumes the output of the previous one, enabling:

- incremental execution  
- streaming-friendly operations  
- memory-efficient processing  
- simple debugging  
- reusable intermediate data  

---

# 3. Installation

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. `uv sync` builds and installs GdeltForge itself (editable), so the `gdeltforge` command becomes available inside the virtual environment.

```
# Install uv (if not already installed)
pip install uv

# Create virtual environment, install dependencies, and install GdeltForge itself
uv sync

# Activate the virtual environment
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# Verify the CLI is installed
gdeltforge --help
```

Then copy the example config and adjust paths:

```
cp config/settings.example.yaml config/settings.yaml
```

### 3.1 Running Tests

The test suite is pure unit tests (no network access, no browser, no real GDELT data required) covering the scraping and conversion logic.

```
uv sync --group dev
uv run pytest
```

---

# 4. Project Structure

GdeltForge follows a standard `src/` package layout, installable via `uv sync` / `pip install`:

```
project_root/
├── config/
│ └── settings.yaml # Global configuration for all pipeline stages
│
├── src/gdeltforge/
│ ├── cli.py # Argument parsing + subcommand dispatch (the gdeltforge entry point)
│ │
│ ├── conversion/
│ │ └── converter.py # CSV -> Parquet conversion logic
│ │
│ ├── filtering/
│ │ └── filter.py # Filtering logic (drop invalid rows)
│ │
│ ├── sampling/
│ │ ├── indexer.py # File indexing for reproducible sampling
│ │ ├── rng.py # Random number generation helpers
│ │ └── samplers.py # Indexed, daily, and filtered sampling
│ │
│ ├── scraping/
│ │ └── scraper.py # Downloader for raw GDELT event files
│ │
│ └── utils/
│   ├── config.py # Config resolution (--config / env var / CWD) and YAML loading
│   ├── io.py # File and chunked-IO helpers
│   └── logging.py # Central logging system
│
├── tests/ # pytest suite (unit tests, no network/browser required)
│
├── main.py # Backward-compatible shim: `python main.py <command>` still works
├── pyproject.toml # Package metadata, build backend, gdeltforge console-script entry point
└── README.md
```

---

# 5. Command Line Interface (CLI)

Run pipeline stages using the installed console script:

```
gdeltforge <command> [options]
```

`python main.py <command> [options]` is kept as a backward-compatible equivalent, so existing scripts keep working unchanged.

Available commands:

| Command | Description |
|---------|-------------|
| scrape  | Download raw GDELT data (ZIP -> CSV) |
| convert | Convert downloaded CSV files to Parquet |
| filter  | Apply row-column filtering to Parquet files |
| sample  | Efficient, reproducible sampling |

The CLI intentionally does not chain stages automatically.
You run each stage explicitly to maintain full control.

By default the config is read from `./config/settings.yaml` (relative to wherever you run the command from). To point at a config file anywhere else on disk, use `--config /path/to/settings.yaml` or set the `GDELTFORGE_CONFIG` environment variable -- useful once GdeltForge is installed and invoked from outside the repo checkout.

---

# 6. Usage Examples

Below are practical, beginner-friendly examples covering all common workflows.

## 6.1 Scrape Raw Data

Download the entire archive:
```
gdeltforge scrape
```

Download only files within a date range (any combination of bounds is valid):
```
gdeltforge scrape --start-date 2020-01-01 --end-date 2023-12-31
gdeltforge scrape --start-date 2022-01-01          # from date onward
gdeltforge scrape --end-date   2015-12-31          # up to date
```

The filter applies to all three file types the GDELT archive provides:

| File type | Example filename | Included when |
|-----------|-----------------|---------------|
| Daily | `20200315.export.CSV.zip` | day falls within range |
| Monthly | `202003.zip` | month overlaps range |
| Yearly | `2020.zip` | year overlaps range |

Files already present in the download directory are skipped regardless of the date filter.

Downloads run concurrently (`scraping.max_workers` in `config/settings.yaml`, default `8`) and are checksum-verified: the GDELT index publishes an MD5 per file, which the scraper captures and checks against each download after it completes. A mismatch is treated the same as a network failure: the file is discarded and retried up to `scraping.retries` times before being reported as failed, so a corrupted or truncated download never silently ends up in the dataset.

### 6.1.1 Link Collection Method: `requests` vs `selenium`

The scraper needs to read the file index at `data.gdeltproject.org/events/` before it can download anything. That index turns out to be a plain, server-rendered HTML directory listing (not JS-rendered), so a headless browser is unnecessary overhead. `scraping.method` in `config/settings.yaml` controls which collector is used:

| | `requests` (default) | `selenium` |
|---|---|---|
| How it works | Plain HTTP GET + regex over the HTML | Launches headless Chrome, waits for the DOM, reads `<a>` tags |
| Dependencies | Just `requests` (already required) | Chrome install + a version-matched ChromeDriver + `selenium`/`webdriver-manager` (`uv pip install '.[selenium]'`) |
| Measured speed | ~0.4s | ~16s (~40x slower) |
| Failure modes | None specific to this site | Breaks whenever Chrome auto-updates past the pinned ChromeDriver version (the original reason this option was added) |
| When to use | Always, unless the page below stops being static | Fallback only, in case GDELT ever switches this index page to a JS-rendered listing |

Both methods were verified to return an identical set of URLs (4,953 files). Timing was a single wall-clock run of each method's link-collection call only (`_collect_gdelt_links_requests` / `_collect_gdelt_links_selenium`, timed with `time.perf_counter()`), back-to-back against the live site on the same machine/network: it does *not* include the subsequent file downloads, which are identical for both methods. It's a one-off measurement, not an average over multiple runs, so treat the ~40x figure as indicative of the order of magnitude rather than a precise benchmark; the gap is structural (Chrome process startup + page-render wait vs. a single HTTP GET), so it should hold up under repeated measurement.

The site's TLS certificate doesn't match its hostname (it's served off a GCS bucket cert), so both methods intentionally skip certificate verification for this one connection; the actual file downloads happen over plain `http://`, which sidesteps the issue entirely.

## 6.2 Convert CSV -> Parquet

```
gdeltforge convert
```
Extracts all CSV files from the downloaded ZIP archives and converts them to Parquet files. Each ZIP is processed independently, so conversion runs across a pool of worker processes (`converter.max_workers` in `config/settings.yaml`; `null`, the default, uses all available CPU cores).

### 6.2.1 Optional: Hive Partitioning for Historical Data

The GDELT archive distributes pre-2013 data in yearly and monthly ZIPs (e.g. `1979.zip`, `200601.zip`) rather than daily files. When you are working with the full archive (1971-2025), keeping those as flat Parquet files means every query scans thousands of files. Enabling Hive partitioning routes those files into a structured directory tree, so filters on `Year` or `MonthYear` skip irrelevant files entirely.

**This feature is off by default.** To enable it, add the following to `settings.yaml`:

```yaml
paths:
  # existing paths ...
  parquet_historical_directory: "./data/parquet_historical"
  filtered_historical_directory: "./data/filtered_historical"

converter:
  partitioning:
    enabled: true
    rules:
      - file_type: yearly    # e.g. 1979.zip
        by: ["Year"]
      - file_type: monthly   # e.g. 200601.zip
        by: ["Year", "MonthYear"]
```

With partitioning enabled, running `gdeltforge convert` produces two separate output areas:

```
data/
├── parquet/                        # daily files (2013-present), unchanged
│   ├── 20130401.export.parquet
│   └── ...
└── parquet_historical/             # yearly/monthly files, Hive-organized
    ├── Year=1979/
    │   └── 1979.parquet
    ├── Year=2006/
    │   └── MonthYear=200601/
    │       └── 200601.parquet
    └── ...
```

Daily ZIPs (2013-present) always go to `parquet_data_directory` as flat files. Yearly and monthly ZIPs go to `parquet_historical_directory` under the directory structure defined by their rule.

Historical ZIPs that have already been converted are tracked with `.done` marker files, so re-running `convert` skips them safely.

All downstream stages (`filter`, `sample`) detect the historical directory automatically from the config and include its data without any extra flags.

## 6.3 Filter the Parquet Dataset
```
gdeltforge filter
```
Drops rows with missing values in the columns defined in settings.yaml.

## 6.4 Sampling

All sampling modes read from the filtered directory.

> Disclaimer: It's easy to adjust this behavior (look for run_sampling_cmd() first line), but I observe that the sampling methods are already as memory friendly as possible in the current setup and, despite that, they still demand lots of RAM, due to the huge data's volume. They would demand much more without the filtering step.

### 6.4.1 Indexed Sampling (Uniform Random)
```
gdeltforge sample --mode indexed -n 10000 --seed 123 --out sample.parquet
```
This samples 10000 instances considering the entire data.

### 6.4.2 Daily Sampling (N Rows Per Day)
```
gdeltforge sample --mode daily --per-day 20 --out daily.parquet
```
This samples 20 instances per day from the entire period (1971 - 20xx)

### 6.4.3 Filtered Sampling (Using JSON Filters)

* Example: 5000 events whose QuadClass is in {1,2}:

  ```
  gdeltforge sample \
      --mode filtered \
      --filter '{"QuadClass": [1, 2]}' \
      -n 5000 \
      --out qc12.parquet
  ```

* Example: 2000 "Verbal Cooperation" events that happened in "USA":

  ```
  gdeltforge sample \
      --mode filtered \
      --filter '{"ActionGeo_CountryCode": ["USA"], "QuadClass": [1]}' \
      -n 2000
  ```

* Example: You can select specific columns of interest, which is a memory friendly practice:

  ```
  gdeltforge sample \
      --mode filtered \
      --filter '{"ActionGeo_CountryCode": ["USA"], "QuadClass": [1]}' \
      --columns GlobalEventID Year Actor1Code \
      -n 1000
  ```
  It outputs 1000 instances following the same rule as before, but this time with only three columns.

### 6.4.4 Stratified Sampling (Fixed N Per Group)

Combines a filter with stratified reservoir sampling: draws exactly `--n-per-group` rows for each distinct value of a chosen column.

```
gdeltforge sample \
    --mode filtered \
    --filter '{"ActionGeo_CountryCode": ["USA"]}' \
    --stratify QuadClass \
    --n-per-group 500 \
    --out stratified.parquet
```

This produces a balanced dataset with 500 USA events per QuadClass value, regardless of the natural class distribution.

`--stratify` requires `--n-per-group`. The `-n` flag is ignored when `--stratify` is set.

## 6.5 Full Pipeline Examples
### 6.5.1 Full pipeline: sample 10,000 rows
```
gdeltforge scrape
gdeltforge convert
gdeltforge filter
gdeltforge sample --mode indexed -n 10000
```
### 6.5.2 Reproducible sampling
```
gdeltforge scrape
gdeltforge convert
gdeltforge filter
gdeltforge sample --mode indexed -n 5000 --seed 42
```
### 6.5.3 USA-only events
```
gdeltforge scrape
gdeltforge convert
gdeltforge filter
gdeltforge sample \
    --mode filtered \
    --filter '{"ActionGeo_CountryCode": ["USA"]}' \
    -n 3000
```

### 6.5.4 30 Events Per Day
```
gdeltforge scrape
gdeltforge convert
gdeltforge filter
gdeltforge sample --mode daily --per-day 30
```

### 6.5.5 Bash One-Liner
```
gdeltforge scrape && \
gdeltforge convert && \
gdeltforge filter && \
gdeltforge sample --mode indexed -n 10000
```

### 6.5.6 PowerShell Loop
```
foreach ($c in "scrape", "convert", "filter") {
    gdeltforge $c
}
gdeltforge sample --mode indexed -n 10000
```

### 6.5.7 Date-restricted pipeline
```
gdeltforge scrape --start-date 2020-01-01 --end-date 2023-12-31
gdeltforge convert
gdeltforge filter
gdeltforge sample --mode indexed -n 10000
```
The date flags apply only to `scrape`. The subsequent stages operate on whatever files are already on disk.

### 6.6 Bonus
The complete filtered-sampling syntax reference and a full set of runnable recipes are in the [documentation](https://vinicius-teixeirac.github.io/GdeltForge/filtered-sampling/): see [Filtered Sampling](https://vinicius-teixeirac.github.io/GdeltForge/filtered-sampling/) and [Recipes](https://vinicius-teixeirac.github.io/GdeltForge/recipes/).

# 7. Logging System

Logging is enabled through a shared logger helper:

```
from gdeltforge.utils.logging import get_logger
logger = get_logger(__name__)
```

Logging to a file:

```
logger = get_logger(__name__, log_to_file=True)
```

# 8. Limitations & Roadmap

GdeltForge is intentionally simple: one pipeline stage per command (no automatic chaining or dependency resolution), CSV -> Parquet only, and `scrape`/`convert`/`filter` now exit non-zero if any individual file fails.

The full, current list of limitations and the roadmap live in one place, the docs site, rather than duplicated here where they'd inevitably drift: see [Limitations & Roadmap](https://vinicius-teixeirac.github.io/GdeltForge/limitations-and-roadmap/).

# 9. Contributing

Contributions are welcome. See [CONTRIBUTING.md](.github/CONTRIBUTING.md) for dev setup, the branch/commit conventions this repo follows, and how to propose a change. Please also read the [Code of Conduct](.github/CODE_OF_CONDUCT.md).

See [CHANGELOG.md](CHANGELOG.md) for a history of notable changes.

Found a security issue? See [SECURITY.md](.github/SECURITY.md) rather than opening a public issue.

# 10. License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.
