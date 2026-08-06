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
- [x] Evaluate a different Parquet compression codec than the current snappy default: measured snappy/gzip/zstd/brotli on real GKG 2.1 data, zstd won on the speed/ratio tradeoff (gzip's ~1.6x came at roughly 35x the write time). `filter.compression` now takes an optional per-dataset override; the snappy default at every other write call site (`convert`, and `filter` when unset) is unchanged
- [x] `filter.output_columns`: optional per-dataset column projection on the filtered output, independent of `columns_to_check`'s row-filtering. GKG 2.1 in particular carries many free-text columns (quotations, all-names, GCAM, extras XML, image/video embeds) a themes/tone/persons/orgs crossref never reads; projecting those out plus the zstd switch above cut measured on-disk size for GKG 2.1 by roughly 12x
- [x] `converter.output_columns`: the same column-projection idea as `filter.output_columns` above, applied at CSV-parse time via `pandas.read_csv`'s `usecols` instead of after the fact. On real GKG 2.1 data this alone sped up conversion roughly 1.4x at the same worker count, and cut peak per-worker memory enough that `converter.max_workers_by_dataset` could safely raise `gdelt_gkg_v2` past the count that previously crashed at `os.cpu_count()`, for a measured ~6x conversion speedup overall once combined with 8 workers. Full sizing numbers are in `configuration.md`'s "Capacity planning" section
- [ ] Evaluate a different Parquet writer library altogether (fastparquet, DuckDB, Polars' own writer) instead of pyarrow; most relevant bundled with the pandas -> Polars evaluation above rather than adopted on its own
