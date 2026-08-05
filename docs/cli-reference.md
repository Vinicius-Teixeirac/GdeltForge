# CLI Reference

```
gdeltforge <command> [options]
```

The CLI intentionally does not chain stages automatically: you run each one explicitly to maintain full control. `python main.py <command>` is kept as a backward-compatible alias.

| Command | Description |
|---------|-------------|
| `scrape`  | Download raw GDELT data (ZIP -> CSV) |
| `convert` | Convert downloaded CSV files to Parquet |
| `filter`  | Apply row-column filtering to Parquet files |
| `sample`  | Efficient, reproducible sampling |
| `crossref` | Enrich a sampled Events output with GKG (themes, tone, people, organizations) |
| `codes`   | Look up valid CAMEO/FIPS codes for filter values |

`scrape`, `convert`, and `filter` all exit non-zero if any individual file failed, even though the ones that succeeded are kept, so a partial failure never gets missed in a `&&`-chained or scripted run. The failed filenames are included in the error message; the per-file reason is in the log output above it.

Any command that fails prints `Error: <message>` to stderr and exits with status 1, rather than a raw Python traceback; interrupting a command with Ctrl+C prints `Interrupted.` and exits with status 130.

## Global options

`--config PATH`

: Path to `settings.yaml`. Defaults to the `GDELTFORGE_CONFIG` environment variable, then `./config/settings.yaml` relative to the current working directory. Use this (or the env var) to run `gdeltforge` from outside the repo checkout, pointing at a config file anywhere on disk.

## `gdeltforge scrape`

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

| Flag | Description |
|------|-------------|
| `--dataset {events,gkg-v1,gkg-v1-counts,gkg-v2,mentions}` | Which GDELT dataset to scrape (default `events`; see [`--dataset`](#-dataset) below) |
| `--start-date YYYY-MM-DD` | Only download files whose period starts on or after this date |
| `--end-date YYYY-MM-DD` | Only download files whose period ends on or before this date |

`--dataset gkg-v2`/`mentions` publish every 15 minutes rather than daily, so a wide date range can imply far more files than the equivalent Events scrape; see [`--dataset`](#-dataset) below. `gkg-v1`/`gkg-v1-counts` are daily, like Events, so this doesn't apply to them.

The date filter applies to all three file types the GDELT archive provides:

| File type | Example filename | Included when |
|-----------|-----------------|---------------|
| Daily | `20200315.export.CSV.zip` | day falls within range |
| Monthly | `202003.zip` | month overlaps range |
| Yearly | `2020.zip` | year overlaps range |

Files already present in the download directory are skipped regardless of the date filter, so re-running `scrape` is safe and incremental.

Downloads run concurrently (`scraping.max_workers`, default `8`) and are checksum-verified against the MD5 GDELT publishes for each file: a mismatch is treated like a network failure and retried, so a corrupted or truncated download never silently ends up in the dataset. See [Configuration](configuration.md#scraping) for the `requests` vs `selenium` link-collection method and the full list of scraping settings.

## `gdeltforge convert`

```
gdeltforge convert
```

Extracts all CSV files from the downloaded ZIP archives and converts them to Parquet. Each ZIP is processed independently, so conversion runs across a pool of worker processes (`converter.max_workers`; `null`, the default, uses all available CPU cores).

Accepts the same `--start-date`/`--end-date` as `scrape` (see above), narrowing which already-downloaded ZIPs get converted instead of which get downloaded:

```
gdeltforge convert --start-date 2020-01-01 --end-date 2020-12-31
```

See [Configuration](configuration.md#hive-partitioning-for-historical-data) for the optional Hive-partitioning feature for pre-2013 yearly/monthly source files.

## `gdeltforge filter`

```
gdeltforge filter
```

Drops rows with missing values in the columns defined under `filter.columns_to_check.<dataset>` in `settings.yaml`. Each file is filtered independently, so filtering runs across a pool of worker processes too (`filter.max_workers`; `null`, the default, uses all available CPU cores).

Also accepts `--start-date`/`--end-date`, narrowing which already-converted Parquet files get read. This restricts which *files* get filtered, not the rows within them: filtering itself drops rows with missing values, a concern unrelated to date.

## `--dataset`

`scrape`, `convert`, `filter`, and `sample` all accept `--dataset {events,gkg-v1,gkg-v1-counts,gkg-v2,mentions}` (default `events`).

- `events`: the daily/monthly/yearly Events archive, as always.
- `gkg-v2`: GKG 2.1, the current, actively-produced Global Knowledge Graph (themes, tone, GCAM, people, organizations; see [Configuration](configuration.md#datasets-and-dataset)). Discovered and downloaded differently from Events under the hood (a 15-minute-interval master file list, not a directory listing); see [Configuration](configuration.md#gkg-21-mentions-discovery-is-a-different-mechanism-entirely).
- `mentions`: every re-report of an Event by a different article over time, the bridge table a real Events<->GKG join goes through, since GKG 2.1 itself carries no event ID (see [Comparison](comparison.md)).
- `gkg-v1`: the legacy GKG format (April 2013 through February 2015 as the primary feed, still published daily since). Unlike GKG 2.1, each row carries `EventIds` directly, so it joins to Events without the two-hop trip through Mentions. Daily files, discovered the same way as Events but from a different URL (see [Configuration](configuration.md#gkg-10-uses-events-html-listing-mechanism-at-a-different-url)).
- `gkg-v1-counts`: GKG 1.0's separate "Counts" file, one row per individual count mention (e.g. one row per "12 killed" statement) rather than one row per document.

## `gdeltforge sample`

All sampling modes read from the filtered directory by default; pass `--source converted` to sample from raw converted Parquet instead, before the `filter` stage's NaN-dropping.

| Flag | Applies to | Description |
|------|-----------|-------------|
| `--dataset {events,gkg-v1,gkg-v1-counts,gkg-v2,mentions}` | all | Which GDELT dataset to sample from (default `events`; see `--dataset` above) |
| `--mode {indexed,daily,filtered}` | all | Sampling strategy (required) |
| `--source {filtered,converted}` | all | Which stage's output to read from (default `filtered`) |
| `-n N` | indexed, filtered | Number of rows to sample (default 1000) |
| `--seed N` | all | RNG seed (default 42) |
| `--per-day N` | daily | Rows per day (default 10) |
| `--filter JSON` | filtered | JSON filter dict, e.g. `'{"QuadClass": [1,2]}'` |
| `--columns COL [COL ...]` | all | Restrict output to these columns; cuts I/O and memory on the full archive |
| `--stratify COLUMN` | filtered | Stratify by this column; requires `--n-per-group` |
| `--n-per-group N` | filtered | Rows per stratum when `--stratify` is set |
| `--out PATH` | all | Output parquet file (default `sample.parquet`) |

### Indexed sampling (uniform random)

```
gdeltforge sample --mode indexed -n 10000 --seed 123 --out sample.parquet
```

Samples 10,000 rows uniformly across the entire dataset.

### Daily sampling (N rows per day)

```
gdeltforge sample --mode daily --per-day 20 --out daily.parquet
```

Samples 20 rows per day across the entire period covered by your downloaded data.

### Filtered sampling (JSON filters)

5,000 events whose `QuadClass` is in `{1, 2}`:

```
gdeltforge sample \
    --mode filtered \
    --filter '{"QuadClass": [1, 2]}' \
    -n 5000 \
    --out qc12.parquet
```

2,000 "Verbal Cooperation" events that happened in the USA:

```
gdeltforge sample \
    --mode filtered \
    --filter '{"ActionGeo_CountryCode": ["US"], "QuadClass": [1]}' \
    -n 2000
```

Selecting specific columns keeps memory use down:

```
gdeltforge sample \
    --mode filtered \
    --filter '{"ActionGeo_CountryCode": ["US"], "QuadClass": [1]}' \
    --columns GlobalEventID Year Actor1Code \
    -n 1000
```

Filters support nested `AND`/`OR` blocks; see the example pipelines below for an `OR` example across multiple columns.

GDELT has two distinct country-code schemes that are easy to mix up: `Actor1CountryCode`/`Actor2CountryCode` use 3-letter CAMEO codes (`USA`), while `ActionGeo_CountryCode`, `Actor1Geo_CountryCode`, and `Actor2Geo_CountryCode` use 2-letter FIPS 10-4 codes (`US`). A value that doesn't match the right scheme for its column logs a warning rather than failing outright (FIPS 10-4 was retired in 2008 and can lag newer countries), but it also means the filter silently matches nothing. Run `gdeltforge codes` to check.

### Stratified sampling (fixed N per group)

Combines a filter with stratified reservoir sampling: draws exactly `--n-per-group` rows for each distinct value of a chosen column, producing a class-balanced dataset regardless of the natural distribution.

```
gdeltforge sample \
    --mode filtered \
    --filter '{"ActionGeo_CountryCode": ["US"]}' \
    --stratify QuadClass \
    --n-per-group 500 \
    --out stratified.parquet
```

This produces 500 USA events per `QuadClass` value. `--stratify` requires `--n-per-group`; `-n` is ignored when `--stratify` is set.

## `gdeltforge crossref`

Enriches a sampled Events output with GKG: themes, tone, people, organizations extracted from the news coverage of each event. Takes a Parquet file (the output of `gdeltforge sample`) rather than the full archive, since joining against the entire Events dataset by default would be a much heavier operation than enriching a bounded sample.

```
gdeltforge crossref --events sample.parquet --gkg-version v2 --out enriched.parquet
```

| Flag | Description |
|------|-------------|
| `--events PATH` | Parquet file of Events rows to enrich (required) |
| `--gkg-version {v1,v1-counts,v2}` | Which GKG generation to join against (required, see below) |
| `--source {filtered,converted}` | Which stage's GKG/Mentions output to read from (default: `filtered`) |
| `--columns COL [COL ...]` | Restrict GKG-side output to these columns; the join key column is always included regardless |
| `--out PATH` | Output parquet file (default `crossref.parquet`) |

GKG's two format generations relate to Events differently, so `--gkg-version` picks a genuinely different join strategy, not just a different data source (see [Comparison](comparison.md) for why a direct Events<->GKG 2.1 join isn't possible):

- **`v1`** and **`v1-counts`**: GKG 1.0 (and its separate Counts file) carry `EventIds` directly on each row, a comma-delimited list, so this is a direct join.
- **`v2`**: GKG 2.1 carries no event id at all, only the source article's URL, so this is a two-hop join through Mentions: Events -> Mentions (on `GlobalEventID`) -> GKG 2.1 (on that URL).

Both preserve the underlying many-to-many structure rather than collapsing it: one event can produce several output rows (several articles covered it, each contributing its own GKG data), and one article covering several events contributes one row per event, not one merged row. An event with no GKG match contributes no rows at all, rather than a row full of nulls. GKG-side output columns are prefixed `GKG_` (and, for `v2`, Mentions bridge fields `Mention_`) to avoid colliding with an identically-named Events column, e.g. `NumArticles` exists on both Events and GKG 1.0.

```
gdeltforge sample --mode filtered --filter '{"ActionGeo_CountryCode": ["US"]}' -n 2000 --out us_events.parquet
gdeltforge crossref --events us_events.parquet --gkg-version v2 --out us_events_enriched.parquet
```

## `gdeltforge codes`

Looks up valid codes for GDELT's CAMEO-coded actor, geo, and event columns, so a filter value can be checked before running a sample. Needs no config file: it's a static reference lookup, usable before `settings.yaml` even exists.

Covers seven code families, each with its own reference list:

| Family | Columns |
|--------|---------|
| CAMEO actor-country (3-letter) | `Actor1CountryCode`, `Actor2CountryCode` |
| FIPS geo-country (2-letter) | `ActionGeo_CountryCode`, `Actor1Geo_CountryCode`, `Actor2Geo_CountryCode` |
| CAMEO ethnic | `Actor1EthnicCode`, `Actor2EthnicCode` |
| CAMEO known-group (IGOs, NGOs, and similar organizations) | `Actor1KnownGroupCode`, `Actor2KnownGroupCode` |
| CAMEO religion | `Actor1Religion1Code`, `Actor1Religion2Code`, `Actor2Religion1Code`, `Actor2Religion2Code` |
| CAMEO actor-type | `Actor1Type1Code`, `Actor1Type2Code`, `Actor1Type3Code`, `Actor2Type1Code`, `Actor2Type2Code`, `Actor2Type3Code` |
| CAMEO event (2-digit root, 3-digit base, up to 4-digit fully specified) | `EventCode`, `EventBaseCode`, `EventRootCode` |

FIPS 10-4 country codes are a different scheme from CAMEO's own 3-letter actor-country codes (`UK` not `GBR`, `RS` not `RUS`), so a value valid on one family can be silently wrong on another; `gdeltforge codes <column>` disambiguates before you run a sample.

A handful of real `EventCode`/`EventBaseCode`/`EventRootCode` values (`"X"`, `"--"`, `"---"`) are GDELT's own markers for rows its event coder couldn't classify, not CAMEO codes, so `gdeltforge codes` deliberately won't list them and a filter using one will still warn.

List which columns have a reference list:

```
gdeltforge codes
```

List every code for a column:

```
gdeltforge codes ActionGeo_CountryCode
```

Search within a column by code or name (case-insensitive substring match):

```
gdeltforge codes ActionGeo_CountryCode --search korea
```

| Flag | Description |
|------|-------------|
| `column` | Positional, optional. A CAMEO/FIPS-coded column, e.g. `ActionGeo_CountryCode` |
| `--search TERM` | Filter results to codes or names containing this substring |

## Full pipeline examples

Sample 10,000 rows end-to-end:

```
gdeltforge scrape
gdeltforge convert
gdeltforge filter
gdeltforge sample --mode indexed -n 10000
```

Reproducible sampling (fixed seed):

```
gdeltforge scrape
gdeltforge convert
gdeltforge filter
gdeltforge sample --mode indexed -n 5000 --seed 42
```

USA-only events:

```
gdeltforge scrape
gdeltforge convert
gdeltforge filter
gdeltforge sample \
    --mode filtered \
    --filter '{"ActionGeo_CountryCode": ["US"]}' \
    -n 3000
```

30 events per day:

```
gdeltforge scrape
gdeltforge convert
gdeltforge filter
gdeltforge sample --mode daily --per-day 30
```

Date-restricted pipeline (the date flags apply only to `scrape`; later stages operate on whatever files are already on disk):

```
gdeltforge scrape --start-date 2020-01-01 --end-date 2023-12-31
gdeltforge convert
gdeltforge filter
gdeltforge sample --mode indexed -n 10000
```

Bash one-liner:

```bash
gdeltforge scrape && \
gdeltforge convert && \
gdeltforge filter && \
gdeltforge sample --mode indexed -n 10000
```

PowerShell loop:

```powershell
foreach ($c in "scrape", "convert", "filter") {
    gdeltforge $c
}
gdeltforge sample --mode indexed -n 10000
```

For the complete filter syntax (nested `AND`/`OR` blocks, all operators) see [Filtered Sampling](filtered-sampling.md); for complete runnable examples see [Recipes](recipes.md).
