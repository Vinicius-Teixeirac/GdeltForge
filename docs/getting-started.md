# Getting Started

## Installation

=== "pip"

    ```bash
    pip install gdeltforge
    ```

=== "uv"

    ```bash
    uv pip install gdeltforge
    ```

Verify it installed:

```bash
gdeltforge --help
```

### Installing from source

??? note "Only needed if you're contributing to GdeltForge itself, or want an editable install"

    ```bash
    # Install uv (if not already installed)
    pip install uv

    git clone https://github.com/Vinicius-Teixeirac/GdeltForge.git
    cd GdeltForge

    # Create the virtual environment, install dependencies, and install
    # GdeltForge itself (editable): this is what makes the `gdeltforge`
    # command available.
    uv sync

    # Activate the virtual environment
    # Windows
    .venv\Scripts\activate
    # macOS / Linux
    source .venv/bin/activate

    # Verify the CLI is installed
    gdeltforge --help
    ```

`python main.py <command>` also works as a backward-compatible alias for the `gdeltforge` command, in case you have existing scripts built around it. This alias is only present in a source checkout, not in the PyPI package.

## Configuration

!!! tip "Optional: skip this and everything below still runs"

    GdeltForge falls back to a conservative built-in default (real `./data/...` paths, no row/column filtering) if there's no config file, and writes that default to `config/settings.yaml` on first run. A fresh environment (a Colab session, say) works with nothing configured.

Worth doing anyway for a real project, so you're deciding `paths:` and `filter.columns_to_check` deliberately rather than accepting the defaults:

```bash
cp config/settings.example.yaml config/settings.yaml
```

At minimum, review `paths:` (where downloaded/converted/filtered data will live) and `filter.columns_to_check` (which columns must be non-null for a row to survive filtering). See the full [Configuration](configuration.md) reference for every option, including what the built-in fallback actually sets.

## Your first pipeline run

![The five pipeline stages: scrape, convert, filter, sample, crossref](assets/pipeline-diagram.svg)

Each stage is a separate command, run in order. A small, date-restricted run is the fastest way to confirm everything is wired up correctly:

```bash
gdeltforge scrape --dataset events --start-date 2024-01-01 --end-date 2024-01-07
gdeltforge convert --dataset events
gdeltforge filter --dataset events
gdeltforge sample --dataset events --mode indexed -n 1000 --out sample.parquet
```

This downloads one week of daily GDELT files, converts them to Parquet, drops rows missing your configured columns, and writes a 1,000-row random sample to `sample.parquet`.

Once that works, drop the date flags to work with the full archive (1979-present).

!!! warning "The full archive is big"

    Dropping the date flags means thousands of files and hundreds of GB. Try `--dry-run` first to see how many files a scrape would pull, and see [Capacity planning](configuration.md#capacity-planning-real-measured-numbers) for measured sizes per dataset.

## Running the test suite

The test suite is pure unit tests: no network access, no browser, no real GDELT data required.

```bash
uv sync --group dev
uv run pytest
```

## Building the docs locally

This site is built with [MkDocs](https://www.mkdocs.org/) + the [Material theme](https://squidfunk.github.io/mkdocs-material/).

```bash
uv sync --group docs
uv run mkdocs serve
```

Then open `http://127.0.0.1:8000/`.

## Where to go next

<div class="gf-grid gf-grid--3">
  <a class="gf-card gf-card--link" href="../cli-reference/"><h3>CLI Reference →</h3><p>Every command, every flag, with examples.</p></a>
  <a class="gf-card gf-card--link" href="../recipes/"><h3>Recipes →</h3><p>Runnable, end-to-end workflows.</p></a>
  <a class="gf-card gf-card--link" href="../filtered-sampling/"><h3>Filtered Sampling →</h3><p>The complete filter syntax.</p></a>
  <a class="gf-card gf-card--link" href="../configuration/"><h3>Configuration →</h3><p>The full <code>settings.yaml</code> reference.</p></a>
  <a class="gf-card gf-card--link" href="../crossref-join-semantics/"><h3>Crossref Semantics →</h3><p>What the Events<->GKG join really produces.</p></a>
  <a class="gf-card gf-card--link" href="../comparison/"><h3>Comparison →</h3><p>When another tool is the better choice.</p></a>
</div>
