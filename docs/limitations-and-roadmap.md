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

`--seed` reproduces a byte-identical sample against the exact same file layout, for every mode. Across a re-chunked layout (the same logical rows split into a different number of files), only `--mode indexed` still guarantees the same sample: it maps seeded random indices to `(file, row)` pairs through a single global index built from the current file list, independent of how many files that list holds. `--mode calendar` and `--stratify` reservoir-sample rows as a streamed multi-file scan reads them, one draw per row in read order; a fixed seed's draws each land on a group's rows in that same order too, so the sample stays reproducible across re-chunking as long as the scan itself reads the underlying rows back in the same relative order, confirmed directly for a layout split across a moderate number of files (see `CHANGELOG.md`'s entry on this). The scan's own internal multi-file scheduling is not guaranteed to preserve that ordering at the scale of a real multi-thousand-file archive, so `calendar`/`stratified` reproducibility across a re-chunked large archive is not a guarantee this project currently makes. Reproducing an exact sample across a re-chunked archive of any size is only guaranteed for `--mode indexed`.

`gdeltforge crossref` cannot be run against `--dataset events-reduced` samples at all: see [Configuration](configuration.md#output_columns-and-crossref-four-columns-you-cant-prune-away) for why.

## Roadmap

- [ ] Docker image, for running the pipeline without a local Python/uv setup
- [ ] Parallel execution of sampling
- [ ] CLI pipelines (e.g., `gdeltforge run all`)
- [ ] GPU-aware sampling (cuDF / RAPIDS)
- [ ] More advanced sampling techniques

Shipped work previously tracked here now lives in [CHANGELOG.md](https://github.com/Vinicius-Teixeirac/GdeltForge/blob/main/CHANGELOG.md); the numbers behind past decisions (compression codec, dtype narrowing, column pruning) live in [Configuration](configuration.md#capacity-planning-real-measured-numbers).
