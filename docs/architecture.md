# Architecture

## Design principles

GdeltForge follows a **single-responsibility, single-stage execution model**: each command performs exactly one transformation.

| Stage | Does |
|-------|------|
| `scrape` | download raw GDELT CSV files |
| `convert` | transform CSV -> Parquet |
| `filter` | remove rows with missing values |
| `sample` | reproducibly sample from Parquet files |

This is intentionally transparent and low-magic:

- every operation is explicit
- no hidden steps
- each module is individually testable
- any stage can be re-run without affecting the others

```
┌─────────────┐      ┌────────────┐      ┌─────────────┐      ┌─────────────┐
│   Scraper   │ ---> │ Converter  │ ---> │   Filter    │ ---> │   Sampler   │
└─────────────┘      └────────────┘      └─────────────┘      └─────────────┘
      CSV               Parquet            Cleaned data         Sampled data
```

Each stage consumes the previous stage's output, which gives you:

- incremental execution (re-run only the stage you need)
- streaming-friendly operations (batched/chunked reads, not full loads)
- memory-efficient processing on datasets much larger than RAM
- simple debugging (inspect intermediate Parquet at any point)
- reusable intermediate data (multiple sampling runs off one filtered dataset)

## Project structure

GdeltForge is a standard installable `src/` package:

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

Each package under `src/gdeltforge/` corresponds to one pipeline stage, plus `utils/` for shared, stage-agnostic helpers (config loading, logging, I/O).

## Logging

Logging goes through a shared helper used consistently across every module:

```python
from gdeltforge.utils.logging import get_logger
logger = get_logger(__name__)
```

To also log to a file (used by the CLI entrypoint itself):

```python
logger = get_logger(__name__, log_to_file=True)
```

File logs land in `logs/pipeline.log`, relative to the current working directory.
