# Configuration

GdeltForge reads a single YAML file, resolved in this order:

1. `--config PATH` passed to the CLI
2. the `GDELTFORGE_CONFIG` environment variable
3. `./config/settings.yaml`, relative to the current working directory (the default)

Start from the template:

```
cp config/settings.example.yaml config/settings.yaml
```

## `columns`

`columns.gdelt_event` lists every column in the GDELT Events schema, in file order. It's used to name the otherwise-headerless columns when reading the raw CSVs, so it should match the [official GDELT 2.0 field list](https://www.gdeltproject.org/data/lookups/CSV.header.fieldids.xlsx) unless you know you're working with a modified schema.

`columns_numeric` lists which of those columns should be coerced to numeric types (via `pd.to_numeric`, invalid values become `NaN`) rather than kept as strings.

## `paths`

All directories the pipeline reads from or writes to. Absolute or relative paths both work.

| Key | Used by | Purpose |
|-----|---------|---------|
| `downloaded_data_directory` | scrape | Where ZIP files land |
| `unzipped_data_directory` | convert | Scratch space for extracted CSVs (cleaned up automatically unless `converter.keep_unzipped` is true) |
| `parquet_data_directory` | convert, filter, sample | Flat (daily) Parquet output |
| `filtered_data_directory` | filter, sample | Flat filtered Parquet output |
| `parquet_historical_directory` | convert, filter, sample | Hive-partitioned Parquet for yearly/monthly source files. Only used when `converter.partitioning.enabled` is true. |
| `filtered_historical_directory` | filter, sample | Filtered version of the historical Hive dataset. Only used when `converter.partitioning.enabled` is true. |

## `scraping`

| Key | Default | Description |
|-----|---------|--------------|
| `retries` | `3` | Retry attempts per file before giving up |
| `timeout` | `30` | Per-request timeout, in seconds |
| `method` | `requests` | Link-collection method: `requests` or `selenium` (see below) |
| `chromedriver_path` | `null` | Only used when `method: selenium`. Absolute path to `chromedriver.exe`, for when automatic download is blocked by a firewall |
| `max_workers` | `8` | Concurrent download workers |

### `requests` vs `selenium`

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

Downloads run through a bounded thread pool (`max_workers`) since they're I/O-bound. Each download is also checksum-verified: the GDELT index publishes an MD5 per file, which the scraper captures and checks after each download completes. A mismatch is treated the same as a network failure -- the file is discarded and retried up to `retries` times before being reported as failed, so a corrupted or truncated download never silently ends up in the dataset.

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
| `columns_to_check` | Rows with a `NaN`/null value in any of these columns are dropped |

This is the one section you should always customize: the example values are illustrative, not a recommendation. Pick the columns that matter for your analysis -- e.g. if you don't need geocoding, don't require `Actor1Geo_Lat`/`Actor1Geo_Long` to be non-null, since that drops any event GDELT couldn't geolocate.
