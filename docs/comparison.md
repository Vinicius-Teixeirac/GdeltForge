# Comparison to Other GDELT Tools

GdeltForge is one of several ways to work with GDELT data in Python (and beyond). This page is an honest comparison: use GdeltForge for the problem it actually solves, and reach for something else when it doesn't fit.

## The GDELT tooling landscape, roughly

- **Official channels**: the [GDELT DOC 2.0 / GEO / TV APIs](https://www.gdeltproject.org/) for small, recent queries; the raw bulk archive at `data.gdeltproject.org` for everything, distributed as thousands of individual files; and a public BigQuery mirror for SQL-at-scale.
- **API/download client libraries**: Python and R packages that wrap the official APIs or bulk archive into dataframes, aimed at retrieval rather than large-scale local processing.
- **Bulk/ETL pipelines**: tools (Spark-based and otherwise) that ingest the raw archive and convert it to a columnar format for downstream use.

GdeltForge is in the last category.

## What GdeltForge actually does differently

Downloading the full archive and converting CSV to Parquet, on their own, are well-trodden problems: several existing tools do exactly this, and it's not hard to script from scratch. GdeltForge's genuinely distinguishing feature is treating **reproducible sampling as a first-class pipeline stage**, not an afterthought: seeded indexed, daily, filtered, and stratified reservoir sampling, all designed to stream over an archive far larger than RAM in a single pass. Producing a reproducible, class-balanced sample of the *entire* historical archive with one CLI command, on a single machine, with no cluster or warehouse, is the part that's genuinely uncommon among the established GDELT Python/R clients.

The other is `crossref`: GKG 2.1 carries no event ID at all, only the source article's URL, so joining it to Events means a two-hop trip through Mentions that's easy to get subtly wrong, most obviously by collapsing the real many-to-many relationship (one event covered by several articles, one article covering several events) down to a naive one-to-one join. `crossref` does that two-hop join with filter pushdown, never materializing the full Mentions/GKG archive, and keeps the many-to-many structure intact rather than silently flattening it.

## When to reach for what

| Need | Reach for |
|---|---|
| Reproducible, seeded, class-balanced samples of the full Events archive, offline, on one machine | **GdeltForge** |
| Events enriched with GKG (themes, tone, people, organizations), preserving the real many-to-many structure instead of collapsing it | **GdeltForge** (`crossref`) |
| Recent article/tone/thematic queries against a small time window | A DOC 2.0 API client |
| VGKG (Visual GKG), or GDELT tables beyond Events/GKG/Mentions | An established R/Python GDELT client, or BigQuery |
| Whole-archive analytics at scale with SQL, no local storage to manage | BigQuery's public GDELT dataset |
| An existing Spark or DuckDB pipeline | Stay on it: GdeltForge's dependency-light, single-machine design trades scale for simplicity, not the reverse |

## What GdeltForge deliberately doesn't do

- **No VGKG, or any GDELT table beyond Events, GKG, and Mentions.** Those three cover what `crossref` needs to join Events to GKG; a fourth table would be a separate, unscoped addition.
- **No data-quality curation beyond null-dropping.** GDELT Events are documented in the academic literature as high-recall, low-precision: a real, nontrivial false-positive rate. `filter` removes rows missing required fields; it doesn't second-guess events GDELT miscategorized in the first place. That's a distinct, harder problem, not something that belongs bolted onto a null-check.
- **No pipeline orchestration.** `scrape`/`convert`/`filter`/`sample` are four separate, explicit commands by design; chaining them is your shell script's job (see [Recipes](recipes.md)), not GdeltForge's.
- **No hosted infrastructure.** GdeltForge is a local tool. SQL-at-scale with nothing to install is BigQuery's job, not this one's.

## For temporal knowledge-graph / event-forecasting work

If you're building on GDELT for research in this space (event-based forecasting, CAMEO-quadruple knowledge graphs), GdeltForge is a reasonable ingestion/sampling front-end: it produces a clean, reproducible, filterable Parquet slice, which you then feed into whatever modeling stack you're using. It doesn't do any of the modeling itself.