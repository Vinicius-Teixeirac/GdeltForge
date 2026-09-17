# Sampling theory in `samplers.py`

This page maps each sampling mode onto the classical algorithm it actually implements, sourced from that algorithm's own literature. It complements [Limitations & Roadmap](limitations-and-roadmap.md#representativeness), which is about what a sample *means* for your data (bias, allocation, population definition); this page is about which algorithm is actually running underneath each `--mode`. Three different algorithms are used here, not one.

## Term, code, and source

| Theory term | Where it lives in `samplers.py` | Source |
|---|---|---|
| Simple random sampling (SRS), without replacement | `IndexedSampler.get_random_sample` — `rng.choice(total_rows, n, replace=False)` | Cochran, *Sampling Techniques*, 3rd ed. (Wiley, 1977), ch. 2 |
| SRS with replacement | `IndexedSampler.get_random_sample(replace=True)`; `FilteredSampler.get_random_sample(replace=True)` via a two-pass scan | Cochran, ch. 2; see [Two passes for with-replacement filtered sampling](#two-passes-for-with-replacement-filtered-sampling) below |
| Reservoir sampling, Algorithm R | `_apply_reservoir_replacements` / `_dedup_last_write_per_slot`, the fill-then-replace pattern shared by `CalendarSampler` and `FilteredSampler` | Vitter, "Random Sampling with a Reservoir," *ACM Transactions on Mathematical Software* 11(1), 1985, pp. 37–57 |
| Stratified sampling, equal allocation | `CalendarSampler.get_calendar_samples` (stratified by period); `FilteredSampler.get_stratified_sample` (stratified by column) | Cochran, ch. 5; Neyman, "On the Two Different Aspects of the Representative Method," *Journal of the Royal Statistical Society* 97(4), 1934 |
| Independent parallel RNG streams | `_group_rng`'s `np.random.default_rng([key_hash, seed])` | NumPy, ["Parallel Random Number Generation"](https://numpy.org/doc/stable/reference/random/parallel.html) |

## Two algorithms, not one, and why

`IndexedSampler` knows the population size upfront: `FileIndex.total_rows` is computed once, from every file's own Parquet footer, before any sampling happens. Given a known population size, direct SRS (drawing `n` indices from `[0, N)`) is both the simplest and the cheapest correct approach — this is exactly what `rng.choice(total_rows, n, replace=False)` does, and NumPy's own implementation notes for `Generator.choice` describe it as Floyd's algorithm or a partial Fisher-Yates shuffle depending on how `n` compares to `N`.

`CalendarSampler` and `FilteredSampler` don't have this luxury. Both read a streamed, batched scan (`lf.collect_batches(...)`) whose total length, and whose per-group length, isn't known until the scan finishes. That is precisely the problem Algorithm R was designed to solve: maintain a fixed-size sample of a stream of unknown or unbounded length, in a single pass, with every item seen so far having had equal probability of surviving to the current point. Vitter's paper phrases the invariant this way: after processing the *t*-th item, each item seen so far is in the reservoir with probability `k/t`, where `k` is the reservoir's capacity. `_apply_reservoir_replacements` implements exactly this, vectorized across a batch: for a batch of rows at positions `[t0, ..., t0+bs)`, it draws one uniform integer per row in `[0, t]` and accepts the row into the reservoir iff that draw falls below `k` (see `_dedup_last_write_per_slot`'s own docstring for how batch-internal collisions are resolved to match true sequential Algorithm R exactly).

So the two-algorithm split is a direct consequence of what each sampler knows about the population before it starts, not an implementation inconsistency. `IndexedSampler` gets a cheaper, simpler algorithm because it built an index first; `CalendarSampler`/`FilteredSampler` pay Algorithm R's overhead in exchange for never needing that index, or the second full pass building one would cost, over an archive far larger than RAM.

## Two passes for with-replacement filtered sampling

`--replace` on `IndexedSampler` is a one-line change (`rng.choice(..., replace=True)`) because, again, the population size is already known. `--replace` on `FilteredSampler.get_random_sample` (see `_get_random_sample_with_replacement`) can't reuse Algorithm R the same way `--stratify`'s without-replacement path does: Algorithm R's efficiency comes from giving each incoming row exactly one shared accept probability (`k/t`) and, on acceptance, replacing one uniformly-chosen existing slot. Independent with-replacement draws need the opposite: each of the `n` output slots would need its own independent accept test against every incoming row. That's `O(n)` work per row; the without-replacement path above is `O(1)` amortized.

Instead, `_get_random_sample_with_replacement` pays for a known population size the same way `IndexedSampler` gets it for free: a first pass counts the filtered rows (reading only the columns the filter itself needs, no output columns, no row materialization), then `n` indices are drawn with replacement against that count and sorted, and a second streamed pass gathers whichever of those (possibly repeated) positions fall in each batch, via `np.searchsorted` against the sorted target array. This is the streaming analogue of what `IndexedSampler`'s single upfront index scan already does; it costs roughly double the I/O of the same call without `--replace`. A defensive check (`pos != count` after the second pass) catches the case where the underlying files changed between the two passes: an unnoticed mismatch there would otherwise produce a wrong sample with no error at all.

## Stratified allocation: equal, not the only option

`--per-period`/`--n-per-group` implement **equal allocation**: every group gets the same sample size regardless of its true size in the archive. This is a real, named choice among several the stratified-sampling literature documents:

- **Equal allocation** (what's implemented here): simplest, guarantees every stratum, including a rare one, appears at a chosen size. Doesn't account for a stratum's real size or internal variability.
- **Proportional allocation**: `n_h ∝ N_h`, reproducing the population's natural stratum proportions in the sample. Always at least as precise as plain SRS of the same total size, and the more common default in general survey practice.
- **Neyman (optimal) allocation**: `n_h ∝ N_h · S_h` (Neyman 1934), minimizing the variance of a stratified estimator for a fixed total sample size by also weighting each stratum by its own internal standard deviation. Cochran's own finding, echoed by later work, is that Neyman's precision gain over plain proportional allocation is usually modest, which is part of why proportional allocation, not Neyman, is the conventional default when a choice has to be made.

GdeltForge implements equal allocation deliberately, not as a placeholder for one of the other two: it is the correct choice for feeding a model training set that needs guaranteed coverage of a rare class or a low-volume time period, which is a coverage goal, not a population-estimation goal. See [Limitations & Roadmap](limitations-and-roadmap.md#representativeness) for what this means for reading a stratified or calendar sample's own statistics, and for how to recover a population-level estimate from one using the `<out>.strata.json` sidecar (`docs/filtered-sampling.md`, `docs/cli-reference.md`).

## Independent RNG streams

`CalendarSampler` and `FilteredSampler` each reservoir-sample many groups (calendar periods, stratify values) from one shared streamed scan. Every group needs its own, independent sequence of accept/reject draws: if all groups drew from one shared `Generator`, the actual numbers a given group's rows received would depend on which order the scan's own physical batches happened to interleave that group's rows with every other group's. That order is a function of file layout, a dependency `--seed` alone doesn't control (see `_group_rng`'s own docstring for the real regression this caused and fixed).

`_group_rng` solves this the way NumPy's own documentation recommends for exactly this situation: rather than a single global stream, each group gets its own `Generator`, seeded from a `SeedSequence` built from an array `[key_hash, seed]` — the group's own key, hashed via `zlib.crc32` for a process-independent value, followed by the fixed `--seed`. NumPy's parallel-RNG guidance describes this "sequence of integers as seed" pattern as an *ad hoc* alternative to `Generator.spawn()`, carrying the same independence guarantee when used correctly, and is explicit that the varying id should be placed *before* the fixed root seed: `spawn()` itself appends its own counter *after* the seed it's given, so prepending the varying id here avoids a possible collision if the two mechanisms were ever mixed. `IndexedSampler` needs none of this: it has exactly one, group-free reservoir, so there is only ever one consumer of its RNG stream and nothing for a different chunking or batching to reorder.

## Further reading

- Cochran, W. G. *Sampling Techniques*, 3rd ed. Wiley, 1977.
- Vitter, J. S. "Random Sampling with a Reservoir." *ACM Transactions on Mathematical Software* 11(1), 1985, pp. 37–57.
- Neyman, J. "On the Two Different Aspects of the Representative Method." *Journal of the Royal Statistical Society* 97(4), 1934, pp. 558–625.
- NumPy. ["Parallel Random Number Generation."](https://numpy.org/doc/stable/reference/random/parallel.html)
- NumPy. [`numpy.random.Generator.choice`](https://numpy.org/doc/stable/reference/random/generated/numpy.random.Generator.choice.html) implementation notes.
