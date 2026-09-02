# Limitations & Roadmap

GdeltForge is intentionally simple and transparent. Current limitations:

## Execution model

Only one pipeline stage per command. No automatic chaining, no dependency resolution. This is **not** supported:

```bash
gdeltforge scrape convert sample
```

You can run multiple stages at once with a shell script of your own: see [Recipes](recipes.md) for worked examples chaining `gdeltforge` calls together.

## Format

Only CSV -> Parquet is supported. The schema is preserved as-is, with no additional transformations beyond numeric coercion (see [Configuration](configuration.md#columns)).

## Sampling

Supported modes: indexed random, calendar, and filtered; filtered mode also supports stratified sampling (fixed N per group). Sampling is without replacement by default. Large samples (>20M rows) require significant disk I/O, since data is intentionally partitioned into many files to avoid extreme RAM usage.

`--mode calendar` reservoir-samples the true per-period group across every contributing file in a single streamed scan, so it caps correctly regardless of how many files a period's rows are spread across, including `--dataset events-reduced`'s own chunked conversion, which routinely splits a single calendar day's rows across several part-files within one `Year=YYYY/` directory (confirmed directly: a day split across 3 part-files still caps at the requested count, not 3x it).

`gdeltforge crossref` cannot be run against `--dataset events-reduced` samples at all: see [Configuration](configuration.md#output_columns-and-crossref-four-columns-you-cant-prune-away) for why.

## Roadmap

- [ ] Docker image, for running the pipeline without a local Python/uv setup
- [ ] Parallel execution of sampling
- [ ] CLI pipelines (e.g., `gdeltforge run all`)
- [ ] GPU-aware sampling (cuDF / RAPIDS)
- [ ] More advanced sampling techniques

Shipped work previously tracked here now lives in [CHANGELOG.md](https://github.com/Vinicius-Teixeirac/GdeltForge/blob/main/CHANGELOG.md); the numbers behind past decisions (compression codec, dtype narrowing, column pruning) live in [Configuration](configuration.md#capacity-planning-real-measured-numbers).
