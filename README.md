# GDELT 2.0 EVENT DATABASE Pipeline  
### A Simple and Modular Data Engineering Framework

This project implements a lightweight but scalable data pipeline to extract, transform, and load the entire **GDELT 2.0 Events Database**.  
It is designed for research workflows requiring:

- Large-scale event data  
- Efficient historical storage (**Parquet**)  
- Reproducible sampling  
- Transparent and modular data lineage  

The architecture emphasizes **simplicity**, **efficiency**, and **explicit execution**: each stage can be run independently or reused in larger workflows.

---

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

## 1.3 Raw Bulk Downloads Are Available — But Hard to Use  
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
scrape → download raw GDELT CSV files
convert → transform CSV → Parquet
filter → remove rows with missing values
sample → reproducibly sample from Parquet files


This design is intentionally **transparent and low-magic**:

- every operation is explicit  
- no hidden steps  
- each module is individually testable  
- any stage can be re-run without affecting others  

### High-Level Architecture

```
┌─────────────┐      ┌────────────┐      ┌─────────────┐      ┌─────────────┐

│   Scraper   │ ---> │ Converter  │ --->v│   Filter    │ ---> │   Sampler   │

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

# 3. Project Structure

```
project_root/
├── config/
│ └── settings.yaml # Global configuration for all pipeline stages
│
├── conversion/
│ └── converter.py # CSV → Parquet conversion logic
│
├── filtering/
│ └── filter.py # Filtering logic (drop invalid rows)
│
├── sampling/
│ ├── indexer.py # File indexing for reproducible sampling
│ ├── rng.py # Random number generation helpers
│ └── samplers.py # Indexed, daily, and filtered sampling
│
├── scraping/
│ └── scraper.py # Downloader for raw GDELT event files
│
├── utils/
│ ├── config.py # YAML loader and configuration utilities
│ ├── io.py # File and chunked-IO helpers
│ └── logging.py # Central logging system
│
├── main.py # Pipeline entrypoint
└── README.md
```

---

# 4. Command Line Interface (CLI)

Run pipeline stages using:

```
python main.py <command> [options]
```

Available commands:

| Command | Description |
|---------|-------------|
| scrape  | Download raw GDELT data (ZIP → CSV) |
| convert | Convert downloaded CSV files to Parquet |
| filter  | Apply row-column filtering to Parquet files |
| sample  | Efficient, reproducible sampling |

The CLI intentionally does not chain stages automatically.
You run each stage explicitly to maintain full control.

---

# 5. Usage Examples

Below are practical, beginner-friendly examples covering all common workflows.

## 5.1 Scrape Raw Data
```
python main.py scrape
```
Downloads and extracts all missing GDELT raw files.

## 5.2 Convert CSV → Parquet

```
python main.py convert
```
Reads all CSV files and writes partitioned Parquet files.

## 5.3 Filter the Parquet Dataset
```
python main.py filter
```
Drops rows with missing values in the columns defined in settings.yaml.

## 5.4 Sampling

All sampling modes read from the filtered directory.

> Disclaimer: It's easy to adjust this behavior (look for run_sampling_cmd() first line), but we observe that the sampling methods are already as memory friendly as possible in the current setup and, despite that, they still demand lots of RAM, due to the huge data'ss volume. They would demand much more without the filtering step.

### 5.4.1 Indexed Sampling (Uniform Random)
```
python main.py sample --mode indexed -n 10000 --seed 123 --out sample.parquet
```
This samples 10000 instances considering the entire data.

### 5.4.2 Daily Sampling (N Rows Per Day)
```
python main.py sample --mode daily --per-day 20 --out daily.parquet
```
This samples 20 instances per day from the entire period (1971 - 20xx)

### 5.4.3 Filtered Sampling (Using JSON Filters)

* Example — 5000 events whose QuadClass is in {1,2}:

  ```
  python main.py sample \
      --mode filtered \
      --filter '{"QuadClass": [1, 2]}' \
      -n 5000 \
      --out qc12.parquet
  ```

* Example — 2000 "Verbal Cooperation" events that happened in "USA":

  ```
  python main.py sample \
      --mode filtered \
      --filter '{"ActionGeo_CountryCode": ["USA"], "QuadClass": [1]}' \
      -n 2000
  ```

* Example — You can select specific columns of interest, which is a memory friendly practice:

  ```
  python main.py sample \
      --mode filtered \
      --filter '{"ActionGeo_CountryCode": ["USA"], "QuadClass": [1]}' \
      --columns GLOBALEVENTID Year Actor1Code \
      -n 1000
  ```
  It outputs 1000 instances following the same rule as before, but this time with only three columns.

## 5.5 Full Pipeline Examples
### 5.5.1 Full pipeline — sample 10,000 rows
```
python main.py scrape
python main.py convert
python main.py filter
python main.py sample --mode indexed -n 10000
```
### 5.5.2 Reproducible sampling
```
python main.py scrape
python main.py convert
python main.py filter
python main.py sample --mode indexed -n 5000 --seed 42
```
### 5.5.3 USA-only events
```
python main.py scrape
python main.py convert
python main.py filter
python main.py sample \
    --mode filtered \
    --filter '{"ActionGeo_CountryCode": ["USA"]}' \
    -n 3000
```

### 5.5.4 30 Events Per Day
```
python main.py scrape
python main.py convert
python main.py filter
python main.py sample --mode daily --per-day 30
```

### 5.5.5 Bash One-Liner
```
python main.py scrape && \
python main.py convert && \
python main.py filter && \
python main.py sample --mode indexed -n 10000
```

### 5.5.6 PowerShell Loop
```
foreach ($c in "scrape", "convert", "filter") {
    python main.py $c
}
python main.py sample --mode indexed -n 10000
```

### 5.6 Bônus
A extensive guide of filtered sampling is available in filtered_sampling_guide.md

# 6. Logging System

Logging is enabled through a shared logger helper:

```
from utils.logger import get_logger
logger = get_logger(__name__)
```

Logging to a file:

```
logger = get_logger(__name__, log_to_file=True)
```

# 7. Known Limitations (Current Version)

The pipeline is intentionally simple and transparent.
Current limitations include:

- Execution Model

Only one pipeline stage per command. No automatic chaining. No dependency resolution. Example of unsupported:

```
python main.py scrape convert sample
```

> Disclaimer: Users can run multiple stages at once by .sh (.cmd, .ps, ...) files. Take a look on multi_sample_.example.cmd or multi_sample.example.sh.

- Data Ingestion

Scrapes all available GDELT files not already downloaded. No date-range filtering during download

> Disclaimer: Users can create year/month/country subsets later using FilteredSampler.

- Format Limitations

Only CSV → Parquet is supported. Schema is preserved without additional transformations

- Sampling Limitations

Supports ony: indexed random sampling, daily sampling and a filtered sampling.Large samples (>20M rows) require significant disk I/O.

> Disclaimer: Data is intentionally partitioned into many files to avoid extreme RAM usage.

# 8. Future Work (Roadmap)

- Date-range scraping
- Parallel executation of operations
- CLI pipelines (e.g., python main.py run all)
- Partition Parquet by year/month
- Parallel execution of scraping, conversion, filtering, sampling
- GPU-aware sampling (cuDF / RAPIDS)

# 9. License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
