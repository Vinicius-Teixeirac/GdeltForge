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

Supported modes: indexed random, daily, and filtered; filtered mode also supports stratified sampling (fixed N per group). Sampling is without replacement by default. Large samples (>20M rows) require significant disk I/O, since data is intentionally partitioned into many files to avoid extreme RAM usage.

## Roadmap

- [ ] Docker image, for running the pipeline without a local Python/uv setup
- [ ] Parallel execution of sampling
- [ ] CLI pipelines (e.g., `gdeltforge run all`)
- [ ] GPU-aware sampling (cuDF / RAPIDS)
- [ ] More advanced sampling techniques

Shipped work previously tracked here now lives in [CHANGELOG.md](https://github.com/Vinicius-Teixeirac/GdeltForge/blob/main/CHANGELOG.md); the numbers behind past decisions (compression codec, dtype narrowing, column pruning) live in [Configuration](configuration.md#capacity-planning-real-measured-numbers).
