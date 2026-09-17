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

### Representativeness

See [Sampling Theory](sampling-theory.md) for which classical algorithm each mode actually runs (simple random sampling, Algorithm R reservoir sampling, equal-allocation stratified sampling), sourced from that algorithm's own literature; this section is about what the result means for your data, not which code path produced it.

"Reproducible" and "representative" are not the same guarantee, and the three modes don't all make the second one. `--mode indexed` draws a true simple random sample: every row in the selected files has equal inclusion probability, so an indexed sample is representative of whatever population those files cover, up to ordinary sampling variance. `--mode calendar` is a stratified design, but with equal allocation: it draws the same `--per-period` count from every period regardless of that period's real size, so pooling the output and computing an unweighted statistic overrepresents low-volume periods and underrepresents high-volume ones relative to the archive. Recovering a population-level estimate from a calendar sample needs reweighting each period's rows by its true row count first; the sample itself doesn't do this. `--stratify` is the same equal-allocation design applied to a chosen column instead of time, and it says so honestly (a "class-balanced dataset regardless of the natural distribution", see [Filtered Sampling](filtered-sampling.md)): it is the right tool for feeding a model training set that needs minority-class coverage, and the wrong one for estimating that column's real population rate.

This applies one level up, too: GDELT's own archive is a convenience census of media coverage, not a random sample of world events, and `--start-date`/`--end-date` narrowing (or downloading only part of the archive to begin with) redefines the population again. Every mode is representative of whatever was actually downloaded and date-filtered, never of "world events" directly, and no amount of `n` corrects for that; it's a property of the mechanism, not the sample size.

`--mode calendar` reservoir-samples the true per-period group across every contributing file in a single streamed scan, so it caps correctly regardless of how many files a period's rows are spread across, including `--dataset events-reduced`'s own chunked conversion, which routinely splits a single calendar day's rows across several part-files within one `Year=YYYY/` directory (confirmed directly: a day split across 3 part-files still caps at the requested count, not 3x it).

`--seed` reproduces a byte-identical sample against the exact same file layout, for every mode. Across a re-chunked layout (the same logical rows split into a different number of files), only `--mode indexed` still guarantees the same sample: it maps seeded random indices to `(file, row)` pairs through a single global index built from the current file list, independent of how many files that list holds. `--mode calendar` and `--stratify` reservoir-sample rows as a streamed multi-file scan reads them, one draw per row in read order; a fixed seed's draws each land on a group's rows in that same order too, so the sample stays reproducible across re-chunking as long as the scan itself reads the underlying rows back in the same relative order, confirmed directly for a layout split across a moderate number of files (see `CHANGELOG.md`'s entry on this). The scan's own internal multi-file scheduling is not guaranteed to preserve that ordering at the scale of a real multi-thousand-file archive, so `calendar`/`stratified` reproducibility across a re-chunked large archive is not a guarantee this project currently makes. Reproducing an exact sample across a re-chunked archive of any size is only guaranteed for `--mode indexed`.

**Upgrading from before 0.10.0:** the per-group RNG derivation `calendar`/`--stratify` use internally changed in 0.10.0 (see `CHANGELOG.md`). The same `--seed` now picks different rows for those two modes than it did on 0.9.x; `--mode indexed` is unaffected. If a downstream artifact depends on an exact row selection from a `calendar` or `--stratify` sample taken before this version, re-sample it after upgrading rather than assuming the old rows carried over.

`gdeltforge crossref` cannot be run against `--dataset events-reduced` samples at all: see [Configuration](configuration.md#output_columns-and-crossref-four-columns-you-cant-prune-away) for why.

## Platform

CI (tests, docs build, publish) runs on Linux only. The package installs and runs on Windows and macOS too. Windows-specific bugs have surfaced there before (a `MAX_PATH` path-length failure, differences in how a killed process tree is reaped) and were found and fixed through manual testing rather than an automated job, so a change that only breaks on Windows or macOS can land without CI catching it. One feature degrades gracefully rather than failing outright: `gdeltforge` making itself its own process group leader for reliable whole-tree signal handling (see [CLI Reference](cli-reference.md)) is a POSIX-only call, skipped as a no-op on Windows, where `taskkill /F /T /PID <pid>` is the documented equivalent instead.

## Roadmap

- [ ] Docker image, for running the pipeline without a local Python/uv setup
- [ ] Parallel execution of sampling
- [ ] CLI pipelines (e.g., `gdeltforge run all`)
- [ ] GPU-aware sampling (cuDF / RAPIDS)
- [ ] More advanced sampling techniques
- [ ] `--replace` for `--mode calendar` and `--mode filtered --stratify`. `--mode indexed` and non-stratified `--mode filtered` already support it (see [CLI Reference](cli-reference.md#gdeltforge-sample)); the reservoir-based calendar and stratified modes reservoir-sample many groups off one shared accept-probability trick that doesn't generalize to independent with-replacement draws without a materially different, more expensive algorithm (an O(n)-per-row accept test, or n independent per-group counting passes), so it's tracked here rather than bolted on

Shipped work previously tracked here now lives in [CHANGELOG.md](https://github.com/Vinicius-Teixeirac/GdeltForge/blob/main/CHANGELOG.md); the numbers behind past decisions (compression codec, dtype narrowing, column pruning) live in [Configuration](configuration.md#capacity-planning-real-measured-numbers).
