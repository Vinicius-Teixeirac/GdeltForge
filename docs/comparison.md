# Comparison to Other GDELT Tools

GdeltForge is one of many ways to work with GDELT data. This page names the real alternatives, Python, R, official, and commercial, and is an honest comparison: use GdeltForge for the problem it actually solves, and reach for something else when it doesn't fit.

## The GDELT tooling landscape, by name

### From the GDELT Project itself

- **[DOC 2.0](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) / [GEO 2.0](https://blog.gdeltproject.org/gdelt-geo-2-0-api-debuts/) / [TV 2.0](https://blog.gdeltproject.org/gdelt-2-0-television-api-debuts/) APIs**: free, official, built for small and recent queries. The DOC API officially covers only the most recent three months of articles and caps a single query at roughly 250 rows; the others share the same "small window, rate-limited" shape.
- **[GDELT Analysis Service](https://analysis.gdeltproject.org/)**: a free, no-code web dashboard suite: an Event Timeline Visualizer, a Word Cloud Visualizer, a Geographic Network Tool, the Global Conflict Dashboard, and GCAM (Global Content Analysis Measures). Point-and-click exploration in a browser, not something you build an automated pipeline on top of.
- **[BigQuery public dataset](https://console.cloud.google.com/marketplace/product/the-gdelt-project/gdelt-2-events)**: the full Events/GKG/Mentions history as SQL tables (`gdelt-bq.gdeltv2.events` and siblings), inside BigQuery's 1TB/month free query tier. Whole-archive analytics with no infrastructure to run, but you're querying a warehouse, not holding a local, versionable Parquet slice.
- **The raw bulk archive** at `data.gdeltproject.org`: every file GDELT publishes, as individual daily/15-minute CSV `.zip`s going back to 1979. This is what every tool on this page, GdeltForge included, ultimately reads from or wraps.

### Python packages

| Package | What it does | How it differs from GdeltForge |
|---|---|---|
| [`gdelt`](https://pypi.org/project/gdelt/) (gdeltPyR) | The longest-established Python client: pulls Events, Mentions, or GKG for a date range via parallel HTTP GETs straight into a pandas (or R) dataframe. | Purely in-memory, one call at a time; Parquet output has been listed as "coming soon" since 2023 and isn't shipped. No resumability, no sampling; last released November 2023. |
| [`gdeltdoc`](https://pypi.org/project/gdeltdoc/) ([gdelt-doc-api](https://github.com/alex9smith/gdelt-doc-api)) | A clean, actively-maintained wrapper around the DOC 2.0 API specifically. | Inherits the DOC API's own ceiling exactly (≈250 rows/query, last 3 months only): it makes that API pleasant to use, it doesn't lift its limits. Never touches the bulk archive at all. |
| [`gdelt-py`](https://rbozydar.github.io/py-gdelt/) | The broadest API-surface client: 6 REST APIs, the 3 core tables, NGrams, and several graph datasets, async, typed, with a BigQuery fallback. | Built for querying, not for holding a full historical archive on disk. No bulk-download-to-Parquet step and no sampling in its own documentation. |
| [`gdelt-client`](https://pypi.org/project/gdelt-client/) | The newest entrant (Feb 2026): DOC API access plus concurrent async downloads of raw Events/Mentions/GKG files for a date range. | Gets you the files; stops there. No conversion to a persistent columnar store, no reproducible sampling, no Events-to-GKG join. |
| [`gdelt-cli`](https://github.com/shakydata/gdelt-cli) | A Rust CLI explicitly "built for agents": local sync of GDELT data into DuckDB, with Parquet/CSV/JSON export. | Conceptually closest in spirit (a local CLI backed by columnar storage), but experimental at the time of writing (single-digit commit count, no tagged release) and not a Python tool if your downstream stack is pandas/polars-based. |
| [`gdelt2py`](https://pypi.org/project/gdelt2py/), [`getgdelt`](https://pypi.org/project/getgdelt/) | Narrower, older tools: GKG-only downloading, and a browser-automation-driven downloader, respectively. | Single-purpose scripts rather than a multi-stage pipeline; neither converts to Parquet or samples. |

### R packages

| Package | What it does | How it differs from GdeltForge |
|---|---|---|
| [`gdeltr2`](https://github.com/abresler/gdeltr2) | The modern R interface: Events, GKG, VGKG, the Full Text API, and TV GKG, still under active development (GitHub-only, never published to CRAN). | An API/table client in R's idiom, not a Python pipeline: no local Parquet conversion stage or reproducible sampling of its own. |
| [`GDELTtools`](https://github.com/cran/GDELTtools) | Download/slice/normalize GDELT **1.0** Events data. | Removed from CRAN (archive only); GDELT 1.0-only, superseded in scope by `gdeltr2`. |

### Ad hoc bulk/ETL pipelines

A handful of GitHub projects wire the raw archive into Spark or a specific cloud stack, Databricks notebooks for GKG, AWS Glue+Lambda+S3+Redshift pipelines, and similar. These are real and functional, but they're one-off scripts tied to a particular cloud vendor's infrastructure, not portable, pip-installable tools: adopting one means adopting that cloud stack along with the codebase itself.

### Hosted intelligence platforms

**[GDELT Guru](https://gdelt.guru/)**: a commercial, AI-driven layer on top of GDELT's data (and other signals) aimed at emerging-threat and market-trend insights for governments and businesses. This is a downstream analytics product, not a data-access tool: it doesn't hand you rows, it hands you conclusions.

## What GdeltForge actually does differently

None of the clients above, in Python or R, are shaped like GdeltForge, and that's the point: they're built to query GDELT's APIs or pull a date range into memory, not to hold the full historical archive on disk as a versionable, reproducibly-sampled Parquet dataset. `gdeltPyR`, the closest Python peer in scope (Events/Mentions/GKG, bulk pulls), still returns everything as one in-memory dataframe per call, with no persistence, no resumability, and no sampling; `gdelt-client` adds concurrent bulk downloads but stops at raw files, the same gap. GdeltForge's genuinely distinguishing feature is treating **reproducible sampling as a first-class pipeline stage**: seeded indexed, calendar, and filtered reservoir sampling, all designed to stream over an archive far larger than RAM in a single pass, filtered mode also supports stratified sampling. Producing a reproducible, class-balanced sample of the *entire* historical archive with one CLI command, on a single machine, with no cluster or warehouse, is the part none of these tools do.

The other is `crossref`: GKG 2.1 carries no event ID at all, only the source article's URL, so joining it to Events means a two-hop trip through Mentions that's easy to get subtly wrong, most obviously by collapsing the real many-to-many relationship (one event covered by several articles, one article covering several events) down to a naive one-to-one join. None of the clients above do this join at all; `crossref` does that two-hop join with filter pushdown, never materializing the full Mentions/GKG archive, and keeps the many-to-many structure intact rather than silently flattening it.

## When to reach for what

| Need | Reach for |
|---|---|
| Reproducible, seeded, class-balanced samples of the full Events archive, offline, on one machine | **GdeltForge** |
| Events enriched with GKG (themes, tone, people, organizations), preserving the real many-to-many structure instead of collapsing it | **GdeltForge** (`crossref`) |
| Recent article/tone/thematic queries against a small time window | [`gdeltdoc`](https://pypi.org/project/gdeltdoc/), or the DOC 2.0 API directly |
| A one-off bulk pull of a date range straight into a pandas dataframe, no persistence or sampling needed | [`gdelt`](https://pypi.org/project/gdelt/) (gdeltPyR) |
| VGKG, TV, or GDELT tables beyond Events/GKG/Mentions | [`gdelt-py`](https://rbozydar.github.io/py-gdelt/) (Python) or [`gdeltr2`](https://github.com/abresler/gdeltr2) (R) |
| Point-and-click exploration with no code at all | The [GDELT Analysis Service](https://analysis.gdeltproject.org/) |
| Whole-archive analytics at scale with SQL, no local storage to manage | BigQuery's public GDELT dataset |
| An existing Spark, Databricks, or cloud ETL pipeline | Stay on it: GdeltForge's dependency-light, single-machine design trades scale for simplicity, not the reverse |

## What GdeltForge deliberately doesn't do

- **No VGKG, or any GDELT table beyond Events, GKG, and Mentions.** Those three cover what `crossref` needs to join Events to GKG; a fourth table would be a separate, unscoped addition. `gdelt-py` and `gdeltr2` cover more tables if you need them.
- **No data-quality curation beyond null-dropping.** GDELT Events are documented in the academic literature as high-recall, low-precision: a real, nontrivial false-positive rate. `filter` removes rows missing required fields; it doesn't second-guess events GDELT miscategorized in the first place. That's a distinct, harder problem, not something that belongs bolted onto a null-check.
- **No pipeline orchestration.** `scrape`/`convert`/`filter`/`sample` are four separate, explicit commands by design; chaining them is your shell script's job (see [Recipes](recipes.md)), not GdeltForge's.
- **No hosted infrastructure, and no API wrapping.** GdeltForge never talks to the DOC/GEO/TV APIs at all, only the raw bulk archive; SQL-at-scale with nothing to install is BigQuery's job, not this one's.

## For temporal knowledge-graph / event-forecasting work

If you're building on GDELT for research in this space (event-based forecasting, CAMEO-quadruple knowledge graphs), GdeltForge is a reasonable ingestion/sampling front-end: it produces a clean, reproducible, filterable Parquet slice, which you then feed into whatever modeling stack you're using. It doesn't do any of the modeling itself.

<div class="gf-grid gf-grid--2">
  <a class="gf-card gf-card--link" href="../getting-started/"><h3>Getting Started →</h3><p>Install it and run a one-week pipeline.</p></a>
  <a class="gf-card gf-card--link" href="../limitations-and-roadmap/"><h3>Limitations &amp; Roadmap →</h3><p>What's deliberately out of scope.</p></a>
</div>
