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
- [ ] Publish to PyPI (`pip install gdeltforge`), planned once the above has settled
- [ ] Docker image, for running the pipeline without a local Python/uv setup
- [ ] Parallel execution of filtering and sampling
- [x] Support for additional GDELT datasets (GKG, Mentions) alongside Events
- [ ] CLI pipelines (e.g., `gdeltforge run all`)
- [ ] GPU-aware sampling (cuDF / RAPIDS)
- [ ] More advanced sampling techniques
