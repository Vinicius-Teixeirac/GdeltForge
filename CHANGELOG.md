# Changelog

All notable changes to GdeltForge are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and version numbers follow [Semantic Versioning](https://semver.org/). Versions are git tags; the installed package version is derived from them via `hatch-vcs`.

## [Unreleased]

### Added
- `gdeltforge codes` command: looks up valid codes across seven CAMEO/FIPS-coded column families (CAMEO actor-country, FIPS geo-country, CAMEO ethnic, CAMEO known-group, CAMEO religion, CAMEO actor-type, and CAMEO event), with a `--search` filter. Needs no config file
- `FilteredSampler` now warns (not raises) when a filter value on a CAMEO/FIPS-coded column isn't recognized for that column's code family, e.g. a 3-letter CAMEO code used against a 2-letter FIPS column. Covers all seven families above, comparing case-insensitively since GDELT stores ethnic codes lowercase in real data while the source codebook uses uppercase
- Bundled CAMEO/FIPS reference data verified against every distinct value across a full archive scan (~542M rows, all seven families); added 7 previously-missing FIPS Pacific-island codes, 28 previously-missing CAMEO known-group codes for organizations absent from the public CAMEO manual, and 2 previously-missing CAMEO event codes (`1213`/`1214`, undocumented in the public manual's "reject material cooperation" branch but confirmed via GDELT's own TABARI/PETRARCH verb-pattern dictionary), each confirmed against real data rather than guessed. A handful of real event-code values (`"X"`, `"--"`, `"---"`) are GDELT's own markers for unclassifiable rows, not CAMEO codes, and are deliberately excluded so a filter using one still warns
- `gdeltforge sample --source {filtered,converted}`: sampling can now read from the raw converted Parquet directory instead of always requiring the `filter` stage first
- `--columns` now applies to `indexed` and `daily` sampling too, not just `filtered`; both previously always read every column of every file they touched
- `--dataset {gkg-v2,mentions}` support for `scrape`/`convert`/`filter`/`sample`: GKG 2.1 (the current, actively-produced Global Knowledge Graph: themes, tone, GCAM, people, organizations) and Mentions (every re-report of an Event by a different article over time) alongside Events. Discovery uses GDELT's v2 master file list rather than Events' HTML directory listing, since GKG 2.1/Mentions publish every 15 minutes, not daily; a warning logs before a scrape that would download an unusually large number of files given that granularity. Column schemas and field order confirmed against the real production parser in `aamend/spark-gdelt`, not just the codebook
- `--dataset {gkg-v1,gkg-v1-counts}` support for `scrape`/`convert`/`filter`/`sample`: the legacy GKG 1.0 format (the primary GKG feed April 2013 through February 2015, still published daily since for backwards compatibility) and its separate, narrower Counts file (one row per count mention rather than per document). Unlike GKG 2.1, GKG 1.0 rows carry `EventIds` directly, so joining to Events skips the two-hop trip through Mentions that GKG 2.1 needs. Discovery reuses Events' HTML-directory-listing approach, not GKG 2.1/Mentions' master-file-list mechanism, since GKG 1.0 publishes daily like Events rather than every 15 minutes; the two paths are inferred (not yet directly confirmed) to share the same markup, based on both hitting an identical TLS certificate mismatch, i.e. the same underlying GCS bucket. Column schemas confirmed against the same real production parser used for GKG 2.1/Mentions
- `gdeltforge crossref` command: enriches a sampled Events output with GKG (themes, tone, people, organizations). `--gkg-version {v1,v1-counts,v2}` picks the join strategy, not just the data source, since GKG 1.0 carries `EventIds` directly (a direct join) while GKG 2.1 carries no event id at all and needs a two-hop join through Mentions on the source article's URL. Both pyarrow filter-pushdown scan only the rows relevant to the input sample, never materializing the full Mentions/GKG archive; both preserve the real many-to-many structure (one event can produce several output rows, one article covering several events contributes one row per event) rather than silently collapsing it, and dedupe GKG 2.1 rows on document URL so an article reprocessed across batches contributes exactly one row. GKG-side output columns are prefixed `GKG_`/`Mention_` to avoid colliding with an identically-named Events column (`NumArticles` exists on both Events and GKG 1.0)

### Changed
- README hero section: real CI/license/release badges, a tighter pitch, and a terminal-demo screenshot of `codes` and `sample` running against the live dataset
- The CLI now catches failures at the top level: any error prints `Error: <message>` and exits 1, and Ctrl+C prints `Interrupted.` and exits 130, instead of a raw Python traceback either way
- **Breaking config change, foundational work for multi-dataset support (GKG, Mentions):** `columns_numeric` and `filter.columns_to_check` are now nested under the dataset name (`gdelt_event`), matching how `columns` was already structured, instead of being flat lists assuming a single dataset. Update `settings.yaml`: wrap your existing `columns_numeric:` list as `columns_numeric: {gdelt_event: [...]}`, and likewise for `filter.columns_to_check`. `scrape`/`convert`/`filter`/`sample` also gain a `--dataset {events,gkg-v1,gkg-v1-counts,gkg-v2,mentions}` flag (default `events`, matching current behavior exactly)

### Fixed
- `FilteredSampler.get_random_sample`/`get_stratified_sample`'s reservoir replacement phase wrote one row at a time via `DataFrame.iloc`, an inherently slow pandas access pattern that made large filtered/stratified samples over the full archive impractically slow
- The initial fix for the above (bulk-assigning all accepted rows in one indexed write) let pandas resolve same-batch slot collisions independently per column block, silently desyncing string columns from numeric ones when both were written together
- A later fix for that (per-column assignment via `DataFrame.iloc`) still relied on pandas' setter, which was itself the dominant cost on GDELT's ~58-column schema; the reservoir is now held as plain per-column numpy arrays during the scan and converted to a DataFrame once at the end, which is both correct and substantially faster
- Getting there also surfaced that plain numpy assignment doesn't raise when a batch row's `NaN` lands in a column that's been `int64` so far: it silently casts to `INT64_MIN` with only a `RuntimeWarning`, unlike pandas' `TypeError`. The reservoir writer now checks the correct common dtype up front and upcasts before writing, rather than reacting to a write that already corrupted data

## [0.3.0] - 2026-07-28

### Added
- Documentation site (MkDocs Material), deployed to GitHub Pages
- `docs/filtered-sampling.md` and `docs/recipes.md`, replacing the old standalone guide and example scripts
- GitHub Actions CI: Ruff + Pyright, tests on Python 3.10 and 3.12, and a package-build check on every push/PR to `main`
- Pre-commit hooks: Ruff on every commit, Pyright and pytest on every push
- Unit test coverage for `filter.py`, `samplers.py`, `indexer.py`, and `rng.py`, including the reservoir-sampling and filter AND/OR DSL logic, which previously had none
- Community health files: Code of Conduct, Contributing guide, Security policy (private vulnerability reporting), issue templates, pull request template
- World-on-anvil emblem as the project's brand mark
- PyPI-readiness metadata in `pyproject.toml` (authors, keywords, classifiers, project URLs)
- `hatch-vcs`-based versioning

### Changed
- Relicensed from MIT to Apache License 2.0
- README's "Known Limitations" and "Roadmap" sections consolidated into a single docs-site page instead of two independently-drifting copies
- `scrape`, `convert`, and `filter` now exit non-zero and report failed files instead of silently discarding partial failures

### Removed
- `filtered_sampling_guide.md`, `sample.example.sh`, `sample.example.cmd` (content moved into the docs site)

### Fixed
- CI silently installed the wrong Python version, since `setup-uv` has no `python-version` input (now passed via `uv sync --python`)
- Various Ruff and Pyright findings across the codebase
- `_is_gdelt_dataset_file` matched same-length non-ZIP files (e.g. `2020.csv`) as monthly/yearly GDELT archives, since it only checked the digit-prefix length and never verified the `.zip` suffix
- Sample output (`gdeltforge sample`) was written directly to its destination path, so a process killed mid-write could leave a corrupt or empty file there with no indication anything was wrong; sample output and scraped downloads now write atomically via a temp file, and warn instead of silently overwriting a leftover incomplete file from a previous interrupted run

## [0.2.0] - 2026-07-24

### Added
- Default requests-based GDELT link scraper, with no browser or ChromeDriver dependency; Selenium kept as an opt-in fallback via the `selenium` extra
- MD5 checksum verification for downloaded GDELT files
- Concurrent downloads (thread pool) and parallel ZIP-to-Parquet conversion (process pool)
- `--start-date` / `--end-date` filtering for the scrape command
- Hive-partitioned historical directory support for yearly/monthly archives, wired through the converter, filter, and all three samplers
- Stratified sampling (`--stratify` / `--n-per-group`), backed by a vectorized reservoir sampler
- `ReproducibleRNG.multinomial`
- Initial pytest suite for the scraper and converter

### Changed
- Restructured into an installable `src/gdeltforge` package layout with a `gdeltforge` console-script entry point
- Renamed the distribution from its original name to GdeltForge
- `settings.yaml` now resolves outside the repo directory
- Reservoir sampling vectorized to remove per-row DataFrame allocation
- Filtering streams Parquet in batches to bound peak memory usage

### Fixed
- AND/OR expression-builder semantics in `FilteredSampler`
- Downloads now use an atomic temp-file swap, preventing corrupt partial files
- ChromeDriver headless-mode and manual-path issues
- `requests.Session` not being closed; `DailySampler` silently no-op'ing on an empty folder

Everything before this point was early prototype work that predates formal versioning; see the [full commit history](https://github.com/Vinicius-Teixeirac/GdeltForge/commits/main/) for detail.
