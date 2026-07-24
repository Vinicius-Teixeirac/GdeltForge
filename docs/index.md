# GdeltForge

**Forges the raw GDELT 2.0 Events archive into clean, reproducibly-sampled Parquet.**

GdeltForge is a lightweight but scalable data pipeline to extract, transform, and load the entire [GDELT 2.0 Events Database](https://www.gdeltproject.org/). It's designed for research workflows that need:

- Large-scale event data, from the 1979 historical backfill through today
- Efficient columnar storage (**Parquet**), with optional Hive partitioning for historical data
- Reproducible sampling (indexed, daily, filtered, stratified)
- Transparent, modular data lineage: every stage is explicit, nothing runs "automagically"

```
git clone https://github.com/Vinicius-Teixeirac/GDELT-2.0-EVENT-DATABASE-Pipeline.git
cd GDELT-2.0-EVENT-DATABASE-Pipeline
uv sync
gdeltforge scrape --start-date 2020-01-01 --end-date 2023-12-31
gdeltforge convert
gdeltforge filter
gdeltforge sample --mode indexed -n 10000
```

!!! note
    GdeltForge isn't published to PyPI yet, so installation is from source for now (see [Getting Started](getting-started.md)). A `pip install gdeltforge` release is planned.

## Why this exists

The GDELT Events Database is extremely rich, but getting the *full* archive through official channels is genuinely difficult:

- **The GDELT API** is built for small, recent queries — tight time windows, a ~250-row cap per query, and rate limits that make pulling the full historical dataset impractical.
- **The BigQuery mirror** has full data, but free-tier quotas (1TB/month query, 10GB storage, 1GB egress) are far too small for the hundreds of GB involved, and full-table scans get expensive fast.
- **The raw bulk archives** are available and complete, but they're thousands of individual ZIP files that need automated downloading, streaming/chunked processing, columnar storage, and memory-safe filtering and sampling before they're actually usable.

GdeltForge automates that last path end-to-end: scrape → convert → filter → sample, each stage independent and re-runnable.

## Where to go next

- [Getting Started](getting-started.md) — install it and run your first pipeline
- [CLI Reference](cli-reference.md) — every command, every flag, with real examples
- [Configuration](configuration.md) — the full `settings.yaml` reference
- [Architecture](architecture.md) — how the pipeline is put together, and why
- [Limitations & Roadmap](limitations-and-roadmap.md) — what's not supported yet, and what's next
