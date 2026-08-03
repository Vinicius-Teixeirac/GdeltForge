# Configuration

GdeltForge reads a single YAML file, resolved in this order:

1. `--config PATH` passed to the CLI
2. the `GDELTFORGE_CONFIG` environment variable
3. `./config/settings.yaml`, relative to the current working directory (the default)

Start from the template:

```
cp config/settings.example.yaml config/settings.yaml
```

## Datasets and `--dataset`

`convert`, `filter`, and `sample` (and, for GKG 2.1/Mentions, `scrape` too) accept `--dataset {events,gkg-v1,gkg-v2,mentions}`, defaulting to `events`. This selects which set of `columns`/`columns_numeric`/`filter.columns_to_check`/`paths.*` keys a command reads; see below for exactly how each section is namespaced per dataset.

| `--dataset` | Config key | Status |
|---|---|---|
| `events` | `gdelt_event` | Full support |
| `gkg-v2` | `gdelt_gkg_v2` | Full support (GKG 2.1, the current format, live since Feb 2015) |
| `mentions` | `gdelt_mentions` | Full support (the bridge table between Events and GKG; see [Comparison](comparison.md) for why) |
| `gkg-v1` | `gdelt_gkg_v1` | Reserved, not yet implemented (legacy pre-2015 format) |

## `columns`

`columns.<dataset>` lists every column in that dataset's schema, in file order. It's used to name the otherwise-headerless columns when reading the raw CSVs, so it should match the official field list for that dataset unless you know you're working with a modified schema: [Events](https://www.gdeltproject.org/data/lookups/CSV.header.fieldids.xlsx), [GKG 2.1](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf).

`columns_numeric.<dataset>` lists which of those columns should be coerced to numeric types (via `pd.to_numeric`, invalid values become `NaN`) rather than kept as strings.

GKG 2.1's own repeated/structured sub-fields (themes, persons, GCAM scores, and similar list-valued columns) are stored as their raw, still-delimited strings in the Parquet output. Parsing those into their own structured columns is a separate concern this version doesn't attempt.

## `paths`

All directories the pipeline reads from or writes to. Absolute or relative paths both work. Events keeps its original, unprefixed keys; other datasets use a prefixed sibling key (`gkg_v2_*`, `mentions_*`) for the same four stages, since mixing different datasets' files in one directory would be a real correctness hazard, not just an organizational one.

| Key (Events) | Key (GKG 2.1 / Mentions) | Used by | Purpose |
|-----|-----|---------|---------|
| `downloaded_data_directory` | `gkg_v2_downloaded_data_directory` / `mentions_downloaded_data_directory` | scrape | Where ZIP files land |
| `unzipped_data_directory` | `gkg_v2_unzipped_data_directory` / `mentions_unzipped_data_directory` | convert | Scratch space for extracted CSVs (cleaned up automatically unless `converter.keep_unzipped` is true) |
| `parquet_data_directory` | `gkg_v2_parquet_data_directory` / `mentions_parquet_data_directory` | convert, filter, sample | Flat Parquet output |
| `filtered_data_directory` | `gkg_v2_filtered_data_directory` / `mentions_filtered_data_directory` | filter, sample | Flat filtered Parquet output |
| `parquet_historical_directory` | *(not applicable)* | convert, filter, sample | Hive-partitioned Parquet for yearly/monthly source files. Events-only; only used when `converter.partitioning.enabled` is true. GKG 2.1/Mentions have no pre-2013 archive to partition. |
| `filtered_historical_directory` | *(not applicable)* | filter, sample | Filtered version of the historical Hive dataset. Events-only, same condition as above. |

## `scraping`

| Key | Default | Description |
|-----|---------|--------------|
| `retries` | `3` | Retry attempts per file before giving up |
| `timeout` | `30` | Per-request timeout, in seconds |
| `method` | `requests` | Link-collection method: `requests` or `selenium` (see below) |
| `chromedriver_path` | `null` | Only used when `method: selenium`. Absolute path to `chromedriver.exe`, for when automatic download is blocked by a firewall |
| `max_workers` | `8` | Concurrent download workers |

### GKG 2.1 / Mentions discovery is a different mechanism entirely

`method`/`chromedriver_path` below only apply to `--dataset events`. GKG 2.1 and Mentions publish at 15-minute granularity (not daily) under `data.gdeltproject.org/gdeltv2/`, discovered via a single master file list rather than an HTML page to scrape: there's no `requests`-vs-`selenium` choice for them. Because of that granularity, a multi-year `--dataset gkg-v2`/`mentions` scrape can easily imply hundreds of thousands of small files; GdeltForge logs a warning (not a hard stop) before starting a scrape that large, and `max_workers` below is worth raising for that many-small-files workload.

### `requests` vs `selenium` (Events only)

The scraper needs to read the file index at `data.gdeltproject.org/events/` before it can download anything. That index is a plain, server-rendered HTML directory listing, not JS-rendered, so a headless browser is unnecessary overhead:

| | `requests` (default) | `selenium` |
|---|---|---|
| How it works | Plain HTTP GET + regex over the HTML | Launches headless Chrome, waits for the DOM, reads `<a>` tags |
| Dependencies | Just `requests` (already required) | Chrome install + a version-matched ChromeDriver + `selenium`/`webdriver-manager` (`uv pip install '.[selenium]'`) |
| Measured speed | ~0.4s | ~16s (~40x slower) |
| Failure modes | None specific to this site | Breaks whenever Chrome auto-updates past the pinned ChromeDriver version |
| When to use | Always, unless the page ever stops being static | Fallback only, in case GDELT ever switches this index page to a JS-rendered listing |

Both methods return an identical set of URLs. `selenium` is kept purely as a fallback; installing it is optional (`selenium`/`webdriver-manager` are not in the default dependency set).

### Concurrency and checksum verification

Downloads run through a bounded thread pool (`max_workers`) since they're I/O-bound. Each download is also checksum-verified: the GDELT index publishes an MD5 per file, which the scraper captures and checks after each download completes. A mismatch is treated the same as a network failure: the file is discarded and retried up to `retries` times before being reported as failed, so a corrupted or truncated download never silently ends up in the dataset.

## `converter`

| Key | Default | Description |
|-----|---------|--------------|
| `keep_unzipped` | `false` | Keep extracted CSVs after conversion instead of deleting them |
| `file_pattern` | `"*.zip"` | Glob pattern for which files in `downloaded_data_directory` to convert |
| `max_workers` | `null` | Worker processes for conversion. `null` uses `os.cpu_count()` |
| `partitioning` | see below | Optional Hive partitioning for historical (pre-daily) files |

Conversion is CPU-bound (CSV parsing + Parquet writing), and each ZIP is independent, so it runs across a `ProcessPoolExecutor`.

### Hive partitioning for historical data

The GDELT archive distributes pre-2013 data in yearly and monthly ZIPs (e.g. `1979.zip`, `200601.zip`) rather than daily files. Keeping those as flat Parquet files means every query scans thousands of files. Enabling partitioning routes them into a structured directory tree instead, so filters on `Year` or `MonthYear` skip irrelevant files entirely.

**Off by default.** To enable it:

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

With partitioning enabled, `gdeltforge convert` produces two separate output areas:

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

Daily ZIPs (2013-present) always go to `parquet_data_directory` as flat files, unaffected by this setting. Historical ZIPs that have already been converted are tracked with `.done` marker files, so re-running `convert` skips them safely. `filter` and `sample` detect the historical directory automatically from the config and include its data without any extra flags.

## `filter`

| Key | Description |
|-----|-------------|
| `columns_to_check.<dataset>` | Rows with a `NaN`/null value in any of these columns are dropped. Nested under the dataset name (mirroring `columns`/`columns_numeric`), one list per dataset |

This is the one section you should always customize: the example values are illustrative, not a recommendation. Pick the columns that matter for your analysis, e.g. if you don't need geocoding, don't require `Actor1Geo_Lat`/`Actor1Geo_Long` to be non-null, since that drops any event GDELT couldn't geolocate.
