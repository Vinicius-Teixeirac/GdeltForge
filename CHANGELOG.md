# Changelog

All notable changes to GdeltForge are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and version numbers follow [Semantic Versioning](https://semver.org/). Versions are git tags; the installed package version is derived from them via `hatch-vcs`.

## [Unreleased]

### Added
- `gdeltforge codes` command: lists valid CAMEO actor codes (`Actor1CountryCode`, `Actor2CountryCode`) and FIPS geo codes (`ActionGeo_CountryCode`, `Actor1Geo_CountryCode`, `Actor2Geo_CountryCode`), with a `--search` filter. Needs no config file
- `FilteredSampler` now warns (not raises) when a filter value on a country-code column isn't recognized for that column's code family, e.g. a 3-letter CAMEO code used against a 2-letter FIPS column

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
