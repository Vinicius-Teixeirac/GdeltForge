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

`convert`, `filter`, `sample`, and `scrape` all accept `--dataset {events,gkg-v1,gkg-v1-counts,gkg-v2,mentions}`, defaulting to `events`. This selects which set of `columns`/`columns_numeric`/`filter.columns_to_check`/`paths.*` keys a command reads; see below for exactly how each section is namespaced per dataset.

| `--dataset` | Config key | Status |
|---|---|---|
| `events` | `gdelt_event` | Full support |
| `gkg-v2` | `gdelt_gkg_v2` | Full support (GKG 2.1, the current format, live since Feb 2015) |
| `mentions` | `gdelt_mentions` | Full support (the bridge table between Events and GKG; see [Comparison](comparison.md) for why) |
| `gkg-v1` | `gdelt_gkg_v1` | Full support (legacy format, April 2013 through February 2015 as the primary feed, still published daily since) |
| `gkg-v1-counts` | `gdelt_gkg_v1_counts` | Full support (GKG 1.0's separate, narrower "Counts" file, one row per count mention rather than per document) |

## `columns`

`columns.<dataset>` lists every column in that dataset's schema, in file order. It's used to name the otherwise-headerless columns when reading the raw CSVs, so it should match the official field list for that dataset unless you know you're working with a modified schema: [Events](https://www.gdeltproject.org/data/lookups/CSV.header.fieldids.xlsx), [GKG 2.1](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf), [GKG 1.0](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook.pdf) (covers both `gkg-v1` and `gkg-v1-counts`).

`columns_numeric.<dataset>` lists which of those columns should be coerced to numeric types (via `pd.to_numeric`, invalid values become `NaN`) rather than kept as strings.

GKG's own repeated/structured sub-fields (themes, persons, GCAM scores, `EventIds`, and similar list-valued columns, in both GKG 2.1 and GKG 1.0) are stored as their raw, still-delimited strings in the Parquet output. Parsing those into their own structured columns is a separate concern this version doesn't attempt.

## `paths`

All directories the pipeline reads from or writes to. Absolute or relative paths both work. Events keeps its original, unprefixed keys; every other dataset uses a prefixed sibling key for the same four stages, since mixing different datasets' files in one directory would be a real correctness hazard, not just an organizational one. The actual config key is `<prefix><base key>`, e.g. `gkg_v1_counts_` + `downloaded_data_directory` = `gkg_v1_counts_downloaded_data_directory`.

The key names don't nest, but the example paths do: `settings.example.yaml` points every stage at `data/<dataset>/<stage>` (`data/events/raw`, `data/gkg_v2/parquet`, `data/mentions/filtered`, ...) rather than a flat `data/<dataset>_<stage>`, so the five datasets stay easy to tell apart on disk even though nothing requires following that convention if you'd rather lay it out differently.

| `--dataset` | Path prefix |
|---|---|
| `events` | *(none, unprefixed)* |
| `gkg-v2` | `gkg_v2_` |
| `mentions` | `mentions_` |
| `gkg-v1` | `gkg_v1_` |
| `gkg-v1-counts` | `gkg_v1_counts_` |

| Base key | Used by | Purpose |
|-----|---------|---------|
| `downloaded_data_directory` | scrape | Where ZIP files land |
| `unzipped_data_directory` | convert | Scratch space for extracted CSVs (cleaned up automatically unless `converter.keep_unzipped` is true) |
| `parquet_data_directory` | convert, filter, sample | Flat Parquet output |
| `filtered_data_directory` | filter, sample | Flat filtered Parquet output |

Two further keys exist for Events only: `parquet_historical_directory` and `filtered_historical_directory` (Hive-partitioned Parquet for yearly/monthly source files, only used when `converter.partitioning.enabled` is true). None of GKG 2.1, Mentions, or GKG 1.0/Counts have a pre-2013 yearly/monthly archive to partition, so they have no historical variant.

## `scraping`

| Key | Default | Description |
|-----|---------|--------------|
| `retries` | `3` | Retry attempts per file before giving up |
| `timeout` | `30` | Per-request timeout, in seconds |
| `method` | `requests` | Link-collection method: `requests` or `selenium` (see below) |
| `chromedriver_path` | `null` | Only used when `method: selenium`. Absolute path to `chromedriver.exe`, for when automatic download is blocked by a firewall |
| `max_workers` | `8` | Concurrent download workers |

### GKG 2.1 / Mentions discovery is a different mechanism entirely

`method`/`chromedriver_path` below only apply to `--dataset events` (and, per below, `gkg-v1`/`gkg-v1-counts`). GKG 2.1 and Mentions publish at 15-minute granularity (not daily) under `data.gdeltproject.org/gdeltv2/`, discovered via a single master file list rather than an HTML page to scrape: there's no `requests`-vs-`selenium` choice for them. Because of that granularity, a multi-year `--dataset gkg-v2`/`mentions` scrape can easily imply hundreds of thousands of small files; GdeltForge logs a warning (not a hard stop) before starting a scrape that large, and `max_workers` below is worth raising for that many-small-files workload.

### GKG 1.0 uses Events' HTML-listing mechanism, at a different URL

`gkg-v1` and `gkg-v1-counts` are daily files, like Events, not the 15-minute batches GKG 2.1/Mentions publish. Discovery scrapes an HTML directory listing at `data.gdeltproject.org/gkg/` the same way Events scrapes `data.gdeltproject.org/events/`, just with a different base URL and filename shape (`YYYYMMDD.gkg.csv.zip` / `YYYYMMDD.gkgcounts.csv.zip`). That reuses the same parsing logic as the `requests` method above, but the exact markup of GKG 1.0's index page hasn't been directly confirmed against a live response (this environment can't reach `data.gdeltproject.org` at all); it's inferred from both paths sharing the same TLS certificate mismatch, i.e. the same underlying GCS bucket. Worth a small-scale test scrape before relying on it for a large historical pull.

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
| `max_workers_by_dataset.<dataset>` | none | Overrides `max_workers` for one dataset. See "Capacity planning" below: a worker count safe for one dataset isn't necessarily safe for another, since it depends on peak per-worker memory |
| `output_columns.<dataset>` | none | Restricts CSV parsing to just these columns instead of every column `columns.<dataset>` defines. `names` is still passed in full to `pandas.read_csv` (it's what maps each raw position to a name on files with no header row), but pandas skips allocating/decoding whatever isn't in `output_columns`. See "`output_columns` and `crossref`" below before pruning a dataset you plan to `crossref` later |
| `compression.<dataset>` | `zstd` | Parquet codec for converter's own output (`parquet_data_directory`), independent of `filter.compression` below for the filtered output that follows it. pyarrow already ships `zstd`, `gzip`, `brotli`, and `lz4`, so this needs no new dependency |
| `partitioning` | see below | Optional Hive partitioning for historical (pre-daily) files |

Conversion is CPU-bound (CSV parsing + Parquet writing), and each ZIP is independent, so it runs across a `ProcessPoolExecutor`.

`output_columns` is worth setting for GKG 2.1 in particular: most of its columns are free-text fields (quotations, all-names, GCAM, extras XML, image/video embeds) that a themes/tone/persons/orgs crossref never reads. See "Capacity planning" below for what dropping them and raising `max_workers_by_dataset` measurably bought on real data.

### Hive partitioning for historical data

The GDELT archive distributes pre-2013 data in yearly and monthly ZIPs (e.g. `1979.zip`, `200601.zip`) rather than daily files. Keeping those as flat Parquet files means every query scans thousands of files. Enabling partitioning routes them into a structured directory tree instead, so filters on `Year` or `MonthYear` skip irrelevant files entirely.

**Off by default.** To enable it:

```yaml
paths:
  # existing paths ...
  parquet_historical_directory: "./data/events/historical"
  filtered_historical_directory: "./data/events/filtered_historical"

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
| `max_workers` | Worker processes for filtering, same tradeoffs as `converter.max_workers`. `null` (default) uses `os.cpu_count()` |
| `columns_to_check.<dataset>` | Rows with a `NaN`/null value in any of these columns are dropped. Nested under the dataset name (mirroring `columns`/`columns_numeric`), one list per dataset |
| `output_columns.<dataset>` | Projects the filtered output down to this column subset, independent of `columns_to_check` (row-filtering still runs against the full row first). Unset keeps every column, same as before this existed. See "`output_columns` and `crossref`" below before pruning a dataset you plan to `crossref` later |
| `compression.<dataset>` | Parquet codec for the filtered output. Unset defaults to `zstd`. pyarrow already ships `zstd`, `gzip`, `brotli`, and `lz4`, so this needs no new dependency |
| `float32_columns.<dataset>` | Narrows these float64 columns to float32 on write. Unset keeps every float column at full float64 precision. See "Capacity planning" below before using this: it's a real precision change, not free compression |

This is the one section you should always customize: the example values are illustrative, not a recommendation. Pick the columns that matter for your analysis, e.g. if you don't need geocoding, don't require `Actor1Geo_Lat`/`Actor1Geo_Long` to be non-null, since that drops any event GDELT couldn't geolocate.

### `output_columns` and `crossref`: four columns you can't prune away

Both `converter.output_columns` and `filter.output_columns` share this same hazard: if you plan to run `gdeltforge crossref` on a dataset later, whichever stage you prune it in must keep the column the join actually runs on, no matter how aggressively you trim everything else:

| Dataset | Required column | Used by |
|---------|------------------|---------|
| `gdelt_event` | `GlobalEventID` | Both join paths |
| `gdelt_gkg_v1` / `gdelt_gkg_v1_counts` | `EventIds` | Direct join (`crossref --gkg-version v1` / `v1-counts`) |
| `gdelt_gkg_v2` | `V2DOCUMENTIDENTIFIER` | Two-hop join (`crossref --gkg-version v2`)[^gdelt2] |
| `gdelt_mentions` | `GLOBALEVENTID`, `MentionIdentifier` | The bridge hop itself, needed only for the `v2` path[^gdelt2] |

[^gdelt2]: The `v2` path has nothing to join before 2015-02-18: Mentions and GKG 2.1 didn't exist until GDELT 2.0 launched that day. `v1`/`v1-counts` reaches back further, to April 2013.

Note that `SOURCEURL` is *not* on this list: the two-hop join to GKG 2.1 goes through Mentions' `MentionIdentifier` (which captures every article that mentioned an event), not through Events' own `SOURCEURL` (which only ever holds one representative article). Pruning `SOURCEURL` doesn't affect crossref at all.

Dropping one of the required columns above doesn't corrupt anything: `crossref` checks for it explicitly and raises a clear error (`"... must include a 'GlobalEventID' column"` or similar) rather than silently returning wrong or empty results. The problem is *when* that error shows up: potentially after `convert`, `filter`, and a `sample` run have already completed on the pruned data, discovering the missing column only once you actually try to enrich it. Both `run_converter` and `run_filter` warn proactively instead, at the point `output_columns` is configured for either stage, against a single `REQUIRED_JOIN_COLUMNS` mapping shared with `crossref.py` itself so the two can't drift apart.

Scraping has no equivalent warning, and can't: `scrape` downloads whole files, it never parses or selects individual columns, so there's no column-level decision to warn about at that stage. The closest real analog at the scrape stage is a coarser, dataset-level one, not choosing a column: `crossref --gkg-version v2` needs Mentions data to exist locally at all, so scraping GKG 2.1 without ever also scraping Mentions produces the same downstream failure for a different reason. Nothing currently warns about that either.

### The other way `crossref` can find nothing: sampling from before a GKG generation existed

Even with every required column intact, `crossref` finds nothing for an event dated before the target GKG generation's coverage actually starts[^gdelt2], since there are no rows to match against, not a configuration problem. Both `crossref_events_gkg_v1` and `crossref_events_gkg_v2` now warn about this too, checked against each sampled event's `DATEADDED` (not `Day`, deliberately: `Day` is when an event is reported to have occurred, which can be far in the past for retrospective reporting, e.g. a 2003 event appearing in a 2013 daily file, while `DATEADDED` is when GDELT actually processed the record and matches the daily file's own date by construction, which is what actually determines whether a corresponding GKG/Mentions record could exist). The warning is a diagnostic, not a filter: events within coverage in the same sample still join normally, and the check silently skips if `DATEADDED` isn't in the sample at all (e.g. pruned out via `--columns`).

For a sample that genuinely spans both eras, `--gkg-version auto` (`crossref_events_gkg_auto`) is worth reaching for instead of picking one version and accepting the gap: it routes each event to `v1` or `v2` individually by its own `DATEADDED`, so events before 2015-02-18 still get GKG 1.0 enrichment rather than nothing, while events from that date on get GKG 2.1's richer per-article fields instead of GKG 1.0's coarser ones. See the "GKG-Enriched Events Across the 2013-2015 Boundary" recipe in [Recipes](recipes.md) for a full worked example, including why the two generations' output columns are concatenated rather than unified (they don't share a single field name in common).

Dropping one of the columns above doesn't corrupt anything: `crossref` checks for it explicitly and raises a clear error (`"... must include a 'GlobalEventID' column"` or similar) rather than silently returning wrong or empty results. The problem is *when* that error shows up: potentially after `filter` and a `sample` run have already completed on the pruned data, discovering the missing column only once you actually try to enrich it. `filter` now warns proactively instead, at the point where `output_columns` is configured, if it detects a dataset's join key isn't in the kept column list, so you find out before those later steps run rather than after.

## Capacity planning: real measured numbers

Everything below was measured against real GDELT data (not synthetic benchmarks), on GKG 2.1, since it's the dataset these knobs matter most for: mostly free-text fields, and 15-minute-interval files means a multi-year pull is hundreds of thousands of files. Treat these as a starting point for sizing your own pull, not a guarantee: your mix of news volume, disk, and CPU will shift the numbers.

**Compression codec**, one real day (120,728 rows, all 27 columns, previously `snappy`-only):

| Codec | Size | Write time | vs. snappy |
|-------|------|------------|------------|
| `snappy` (previous default) | 744.7 MB | 11.0s | baseline |
| `gzip` | 422.3 MB | 514.0s | 1.8x smaller, ~47x slower to write |
| `zstd` (recommended) | 410.8 MB | 14.3s | 1.8x smaller, same order of write time |
| `brotli` | 284.2 MB | 87.7s | 2.6x smaller, ~8x slower to write |
| `zstd`, level 19 | 232.6 MB | slow | 3.2x smaller, not worth it once columns are pruned (below) |

`gzip` and `zstd` level 19 both cost far more write time than they're worth here; plain `zstd` is the pick, which is what `filter.compression` defaults `gdelt_gkg_v2` to.

**Column pruning**, same day, `output_columns` set to the join key plus themes/tone/persons/orgs (10 of 27 columns):

| Variant | Size | vs. snappy/all-columns |
|---------|------|------------------------|
| 10 columns, `snappy` | 97.1 MB | 7.0x smaller |
| 10 columns, `zstd` | 54.7 MB | 12.4x smaller |

Column pruning did most of the work; the codec switch on top was a smaller, roughly 1.8x bonus. A second real day (2023-06-01, a heavier news day at 200,740 rows) landed at 94 MB pruned+`zstd`, consistent with the same ratio, so expect day-to-day variance of roughly 55-95 MB/day for GKG 2.1 rather than a single fixed number.

**Conversion worker count**, same 96-file day, `output_columns` active:

| `max_workers` | Time | Rate | Notes |
|----------------|------|------|-------|
| 4, no pruning | 30.2s | 3.1 files/s | previous default, previous behavior |
| 4, pruned | 21.4s | 4.5 files/s | pruning alone, same worker count |
| 8, pruned | 10.7s | 9.0 files/s | new `max_workers_by_dataset` default for `gdelt_gkg_v2` |
| 12, pruned | 10.1s | 9.5 files/s | marginal gain over 8, less headroom |
| 20, pruned | 11.4s | 8.5 files/s | previously crashed unpruned at this count; completed cleanly pruned, but no faster |

8 workers was picked over 12: nearly identical throughput with more memory headroom, since the original crash at `os.cpu_count()` was never root-caused, only reproduced and then avoided by pruning.

**Putting it together**: projecting to the full ~385,728-file GKG 2.1 archive (15-minute files, 2015-present) at these measured rates, with `mentions` (needed for the crossref join) excluded since it is small enough not to move these numbers much:

| Scope | Files | Wall-clock (scrape + convert + filter) | Disk |
|-------|-------|------------------------------------------|------|
| Previous approach (no pruning, 4 workers, `snappy`) | 385,728 | ~103 hours (~4.3 days) | ~2.9 TB |
| Pruned + `zstd` + 8 workers | 385,728 | ~46 hours (~1.9 days) | ~220-380 GB |

Scrape throughput (~4.2 files/s) is network-bound against `data.gdeltproject.org` and unaffected by any of the above; convert is where pruning and worker count actually move the number, from the previous bottleneck (~71 hours) down to roughly 12 hours.

### Dtype narrowing: where it does and doesn't pay off

A natural next question after the above is whether narrowing individual column types saves more. The answer depends entirely on whether the column is low-cardinality or genuinely continuous, and the two cases point in opposite directions.

**Low-cardinality integers (`Actor1Geo_Type`, `QuadClass`, ...): narrowing barely helps.** `Actor1Geo_Type`/`Actor2Geo_Type`/`ActionGeo_Type` have exactly 6 distinct values in practice (0-5, confirmed against 8.3M real Events rows), and `QuadClass` has 4. The intuitive expectation is that declaring these as `uint8` instead of `int64` should save close to 8x. It doesn't: Parquet already dictionary-encodes low-cardinality integer columns by default regardless of the declared Arrow type, so a 6-value column gets stored as small bit-packed dictionary indices either way. Measured on a real 5M-row sample, `int64` to `uint8` gave a 1.00-1.01x change (noise level) for every one of these columns. `IsRootEvent` to `bool` was the one exception, a real 17% reduction on that column specifically, since Parquet's native boolean bit-packing beats even dictionary-encoded `int64` for a 2-value column, but that column is too small a slice of a full row (these five candidate columns combined are only ~2% of a full Events file) for it to be worth a config option on its own. This was investigated but not implemented for that reason.

**Continuous floats (`AvgTone`, lat/long, ...): narrowing to float32 saves real space, but is not lossless.** Dictionary encoding can't compress a column with hundreds of thousands of distinct values the way it compresses a 6-value one, so `float64` to `float32` does save real space here: `AvgTone` measured 1.30-1.33x smaller, lat/long columns 1.02-1.03x.

The first pass at this reasoned "float32's ~7 significant digits should be plenty for a tone score" without checking GDELT's actual emitted precision. That assumption was wrong. A real downloaded Events file (`20130401.export.CSV.zip`) shows `AvgTone` values with up to 16 decimal places and 15 significant figures in source data, e.g. `0.0284010224368077`, and a direct round-trip test (cast to float32, back to float64, compare to the original) against 6.5M real rows confirms the practical effect: the value changes on 31% of rows for `GoldsteinScale`, 96% for `AvgTone`, and literally 100% for `FractionDate`. Each individual change is tiny (on the order of float32's ~1.19e-7 relative precision floor), but it is a genuine, measurable change to the value, not just a smaller encoding of the same one.

That's exactly why `filter.float32_columns` exists as an explicit, per-dataset, per-column opt-in rather than a blanket setting or a new default: it's available for anyone who has decided that tradeoff is acceptable for their use case, but nothing is cast to float32 unless a column is named there.

### Why compression defaults to zstd now, for every dataset

The GKG 2.1 codec numbers earlier in this page don't automatically transfer to Events, since it's a different content mix (mostly short codes, names, and dates rather than GKG's free text). Measured directly on 5.8M real Events rows, all 58 columns:

| Codec | Size | bytes/row | Write time |
|-------|------|-----------|------------|
| `snappy` (previous default) | 471.6 MB | 81.1 | 64.0s |
| `zstd` (current default) | 330.4 MB | 56.8 | 50.5s |

Roughly 30% smaller, and faster to write, not slower. Since `zstd` is lossless, this isn't a tradeoff to weigh the way `float32_columns` is: there's no case where `snappy` is the better default. `filter.compression` defaults to `zstd` for every dataset as of 2026-08-07; `compression.<dataset>` remains available to override to a specific codec if one is ever needed.

`converter.compression` defaults to `zstd` too, for the same reason: it wasn't independently re-measured against converter's own (unfiltered, wider-row-count) output, but a lossless codec with no measured downside on real GDELT data has no case for defaulting to `snappy` there either. It was previously hardcoded to `snappy` with no way to change it; it's now a normal per-dataset setting, same shape as `filter.compression`.
