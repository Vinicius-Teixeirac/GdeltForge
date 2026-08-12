# Limitations & Roadmap

GdeltForge is intentionally simple and transparent. Current limitations:

## Execution model

Only one pipeline stage per command. No automatic chaining, no dependency resolution. This is **not** supported:

```
gdeltforge scrape convert sample
```

You can run multiple stages at once with a shell script of your own: see [Recipes](recipes.md) for worked examples chaining `gdeltforge` calls together.

## Format

Only CSV -> Parquet is supported. The schema is preserved as-is, with no additional transformations beyond numeric coercion (see [Configuration](configuration.md#columns)).

## Sampling

Supported modes: indexed random, daily, filtered, and stratified. Sampling is without replacement by default. Large samples (>20M rows) require significant disk I/O, since data is intentionally partitioned into many files to avoid extreme RAM usage.

## Roadmap

- [x] Parallel execution of scraping (concurrent downloads) and conversion (multi-process)
- [x] Checksum-verified downloads (MD5, when GDELT provides one) and a pytest unit-test suite
- [x] Package restructuring for distribution: rebranded as GdeltForge, `src/gdeltforge` layout, installable `gdeltforge` entry point, config resolution outside the repo directory
- [x] CI (GitHub Actions): tests + build check on every push/PR to main
- [x] Single-source versioning via `hatch-vcs` (the git tag *is* the version)
- [x] This documentation site
- [x] Linting and type-checking (ruff + pyright) enforced in CI
- [x] Community health files: Code of Conduct, Contributing guide, Security policy, issue/PR templates
- [x] Unit test coverage for the filtering, sampling, indexing, and RNG modules
- [x] Publish to PyPI (`pip install gdeltforge`)
- [ ] Docker image, for running the pipeline without a local Python/uv setup
- [x] Parallel execution of filtering (`filter.max_workers`, matching `converter.max_workers`)
- [ ] Parallel execution of sampling
- [x] Support for additional GDELT datasets (GKG, Mentions) alongside Events
- [ ] CLI pipelines (e.g., `gdeltforge run all`)
- [ ] GPU-aware sampling (cuDF / RAPIDS)
- [ ] More advanced sampling techniques
- [ ] Evaluate migrating performance-critical DataFrame code (CSV parsing in `convert`, row filtering in `filter`) from pandas to Polars
- [x] Evaluate a different Parquet compression codec than the previous snappy default: measured snappy/gzip/zstd/brotli on real GKG 2.1 data, zstd won on the speed/ratio tradeoff (gzip's ~1.6x came at roughly 35x the write time), and a separate check on real Events data (~30% smaller than snappy, comparable or faster write speed) confirmed it generalizes beyond GKG. `filter.compression` defaults to zstd as of 2026-08-07, for every dataset, with a per-dataset override still available; `convert`'s write calls are unchanged
- [x] Evaluated narrowing individual column dtypes (e.g. `int64` to `uint8` for low-cardinality columns like `Actor1Geo_Type`/`QuadClass`) for further disk savings. Real measurement showed it isn't worth pursuing: Parquet already dictionary-encodes low-cardinality integer columns regardless of declared type, so the gain was under 1% except for `IsRootEvent` to `bool` (17%, too small a column to matter alone). `float64` to `float32` for genuinely continuous columns (`AvgTone`, lat/long) does save real space (1.02-1.33x), but is not lossless: real `AvgTone` values carry up to 15 significant figures, well past float32's ~7, and a round-trip test changed values on 31-100% of rows depending on the column. Added as `filter.float32_columns`, opt-in per dataset and per column, off by default
- [x] `filter.output_columns`: optional per-dataset column projection on the filtered output, independent of `columns_to_check`'s row-filtering. GKG 2.1 in particular carries many free-text columns (quotations, all-names, GCAM, extras XML, image/video embeds) a themes/tone/persons/orgs crossref never reads; projecting those out plus the zstd switch above cut measured on-disk size for GKG 2.1 by roughly 12x
- [x] `converter.output_columns`: the same column-projection idea as `filter.output_columns` above, applied at CSV-parse time via `pandas.read_csv`'s `usecols` instead of after the fact. On real GKG 2.1 data this alone sped up conversion roughly 1.4x at the same worker count, and cut peak per-worker memory enough that `converter.max_workers_by_dataset` could safely raise `gdelt_gkg_v2` past the count that previously crashed at `os.cpu_count()`, for a measured ~6x conversion speedup overall once combined with 8 workers. Full sizing numbers are in `configuration.md`'s "Capacity planning" section
- [x] Warn when `output_columns` prunes away a column `crossref` requires (`GlobalEventID` for `gdelt_event`; `EventIds` for both GKG 1.0 datasets; `V2DOCUMENTIDENTIFIER` for `gdelt_gkg_v2`; `GLOBALEVENTID` and `MentionIdentifier` for `gdelt_mentions`). `crossref` already raised a clear error for this, but only at join time, potentially after `convert`, `filter`, and an unrelated `sample` run had already completed on the pruned output. `run_converter` and `run_filter` both now check and warn at their own configure time instead, sharing a single `warn_if_output_columns_drops_join_key` helper and `REQUIRED_JOIN_COLUMNS` mapping defined in `crossref.py` so all three can't drift out of sync. Scraping was considered too, but doesn't have an equivalent: `scrape` downloads whole files and never selects columns, so there's no column-level decision to warn about at that stage; the closest analog there is a dataset-level one (scraping `gkg-v2` without also scraping `mentions`), not yet warned about either
- [x] `crossref_events_gkg_v2` used to hardcode `MentionTimeDate` and `Confidence` as unconditionally-required columns when reading Mentions, even though neither participates in the actual join (only `GLOBALEVENTID` and `MentionIdentifier`, `REQUIRED_JOIN_COLUMNS`'s `gdelt_mentions` entry, do). A Mentions dataset missing either one failed with a raw pyarrow error instead of joining successfully minus that payload field. Now reads `OPTIONAL_MENTIONS_PAYLOAD_COLUMNS` (`MentionTimeDate`, `Confidence`) only if present, carrying through whichever `Mention_<name>` columns actually exist rather than requiring both
- [x] Warn when a sampled event predates the target GKG generation's real coverage start, the other way `crossref` can find nothing besides a missing column: GKG 1.0's earliest published file is 2013-04-01, and GDELT 2.0 (GKG 2.1 and Mentions together) launched 2015-02-18, both verified against GDELT's real file listings rather than assumed. `crossref_events_gkg_v1`/`_v2` now check each sampled event's `DATEADDED` (not `Day`, which reflects when the event is reported to have occurred and can be far in the past for retrospective reporting, unlike `DATEADDED`, which is when GDELT actually processed the record and is what determines whether a GKG/Mentions row could exist at all) against `GKG_V1_COVERAGE_START`/`GKG_V2_COVERAGE_START` and warn, without blocking events that are within coverage in the same sample from joining normally
- [x] `crossref_events_gkg_auto` (`--gkg-version auto`): routes each event to `v1` or `v2` individually by its own `DATEADDED` against `GKG_V2_COVERAGE_START`, instead of requiring one version for the whole sample. Built for a sample spanning both eras, e.g. the 2013-2015 window where only GKG 1.0 exists: `v1` alone would miss GKG 2.1's richer per-article fields for the post-2015 portion, `v2` alone would find nothing for the pre-2015 portion. Output is both paths' results concatenated with a `CrossrefSource` column (`v1`/`v2`); the two schemas' GKG-side columns are never unified (GKG 1.0's 11 fields and GKG 2.1's 27 share no common name), so a row carries `NaN` for whichever set its source path didn't produce. `--columns` isn't supported in `auto` mode for the same reason: a column name valid for one schema is generally meaningless for the other
- [ ] Evaluate a different Parquet writer library altogether (fastparquet, DuckDB, Polars' own writer) instead of pyarrow; most relevant bundled with the pandas -> Polars evaluation above rather than adopted on its own
- [x] `convert` had no resumability for flat/daily output (Events daily, GKG 1.0/2.1, Mentions), unlike `scrape`'s skip-already-downloaded behavior: a `.done` marker file already existed for historical (Hive-partitioned yearly/monthly) output, but was never applied to the flat case. Found for real against a live 30,137-file Mentions batch: two independent runs each died to an OS-level kill around the same ~51% mark after 30+ minutes, having made no net progress relaunch to relaunch, because every attempt reprocessed every zip from file 1 and needlessly overwrote output that was already correct on disk. The marker is now written for every file type, and `process_all_files` skips any zip already marked done on the next run, matching `scrape`'s resume behavior
- [x] Neither the `.done` marker above nor `filter`'s output (which had no resumability at all until now) accounted for a run's own configuration: a marker only ever recorded that a file had been processed, not under what settings. Rerunning `convert` after changing `output_columns`, or `filter` after changing `columns_to_check`/`output_columns`/`float32_columns`/`compression`, would be silently skipped by a marker left from the old configuration, serving output shaped by settings that no longer match the current run. `config_fingerprint`/`is_marked_done`/`mark_done` (`utils/io.py`) now write the relevant config into the marker itself and compare it, not just presence, on every resumed run; a mismatch, including a pre-fingerprint empty marker from before this existed, is treated as not done
