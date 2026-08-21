# GdeltForge

**Forges the raw GDELT 2.0 archive into clean, reproducibly-sampled, cross-referenced Parquet.**

GdeltForge is a lightweight but scalable data pipeline to extract, transform, and load [GDELT 2.0](https://www.gdeltproject.org/): the Events table, the Global Knowledge Graph (both the current GKG 2.1 and the legacy GKG 1.0), and Mentions, the bridge table between them. It's designed for research workflows that need:

- Large-scale event data, from the 1979 historical backfill through today
- Events enriched with GKG (themes, tone, people, organizations), via a dedicated cross-reference stage that preserves the real many-to-many relationship instead of silently collapsing it
- Efficient columnar storage (**Parquet**), with optional Hive partitioning for historical Events data
- Reproducible sampling (indexed, daily, filtered, stratified)
- Transparent, modular data lineage: every stage is explicit, nothing runs "automagically"

```
pip install gdeltforge
gdeltforge scrape --dataset events --start-date 2020-01-01 --end-date 2023-12-31
gdeltforge convert --dataset events
gdeltforge filter --dataset events
gdeltforge sample --dataset events --mode indexed -n 10000
```

See [Getting Started](getting-started.md) for configuration and an installation-from-source option, useful if you're contributing to GdeltForge itself.

## About GDELT

[GDELT](https://www.gdeltproject.org/) (the Global Database of Events, Language, and Tone) monitors broadcast, print, and web news from nearly every country, in over 100 languages (translating 65 of them into English in realtime), processing it continuously with new records published every 15 minutes. It's one of the largest open datasets of global news activity available.

GDELT actually publishes several distinct tables, all of which GdeltForge processes:

- **Events**: structured, CAMEO-coded records of who-did-what-to-whom-where. Each row is a single event extracted from a news article: two actors, an action, a date, and a location.
- **Global Knowledge Graph (GKG)**: themes, emotions (2,300+ dimensions via GDELT's GCAM sentiment engine), people, organizations, and imagery/video, extracted from the same articles. GDELT has published two generations: **GKG 2.1**, the current format (live since Feb 2015, updated every 15 minutes), and legacy **GKG 1.0** (the primary feed April 2013 through February 2015, still published daily since for backwards compatibility).
- **Mentions**: every re-report of an Event by a different article over time, not just the first. It's the bridge table between Events and GKG 2.1: GKG 2.1 carries no event ID of its own, only the source article's URL, so joining it to Events means going through Mentions.

`gdeltforge crossref` enriches a sampled Events output with GKG data, picking the right join strategy for the GKG generation you're using: GKG 1.0 carries `EventIds` directly (a direct join), while GKG 2.1 needs the two-hop join through Mentions. Both preserve the real many-to-many structure GDELT's data actually has, rather than silently collapsing it: one event can be covered by many articles, and one article can cover many events. See [CLI Reference](cli-reference.md#gdeltforge-crossref) for the full command, and [Comparison to Other Tools](comparison.md) for how this stacks up against other GDELT clients.

## Why this exists

GDELT is extremely rich, but getting the *full* archive through official channels is genuinely difficult:

- **The GDELT API** is built for small, recent queries: tight time windows, a ~250-row cap per query, and rate limits that make pulling the full historical dataset impractical.
- **The BigQuery mirror** has full data, but free-tier quotas (1TB/month query, 10GB storage, 1GB egress) are far too small for the hundreds of GB involved, and full-table scans get expensive fast.
- **The raw bulk archives** are available and complete, but they're thousands (for GKG 2.1/Mentions' 15-minute cadence, hundreds of thousands) of individual files that need automated downloading, streaming/chunked processing, columnar storage, and memory-safe filtering and sampling before they're actually usable.

GdeltForge automates that last path end-to-end: scrape -> convert -> filter -> sample, each stage independent and re-runnable, plus `crossref` to join a sampled Events output back to GKG once you have both.

## Where to go next

- [Getting Started](getting-started.md): install it and run your first pipeline
- [CLI Reference](cli-reference.md): every command, every flag, with real examples
- [Configuration](configuration.md): the full `settings.yaml` reference
- [Architecture](architecture.md): how the pipeline is put together, and why
- [Limitations & Roadmap](limitations-and-roadmap.md): what's not supported yet, and what's next
