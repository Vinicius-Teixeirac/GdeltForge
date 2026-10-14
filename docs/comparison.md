# Related Work

GDELT can be reached in several ways, each designed for a different kind of question. This page describes them by what they are built for, and says where GdeltForge's case, the whole archive kept on disk and sampled with a stated design, differs from theirs. For many analyses one of these is the shorter path.

## From the GDELT Project

- **[DOC 2.0](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/), [GEO 2.0](https://blog.gdeltproject.org/gdelt-geo-2-0-api-debuts/) and [TV 2.0](https://blog.gdeltproject.org/gdelt-2-0-television-api-debuts/) APIs**: free, official search services, each answering a bounded query. The DOC API searches the most recent three months of articles by default and returns up to 250 results per query; the GEO API covers the last 15 minutes to 7 days; the TV API searches television news coverage from 2009 on. For questions about news coverage that fit those bounds, they are the direct route.
- **GDELT Analysis Service**: browser tools for exploring the data without code, such as the Word Cloud Visualizer and the Global Knowledge Graph Network Visualizer, listed on [GDELT's home page](https://www.gdeltproject.org/).
- **[BigQuery public dataset](https://console.cloud.google.com/marketplace/product/the-gdelt-project/gdelt-2-events)**: the full Events, GKG and Mentions history as SQL tables (`gdelt-bq.gdeltv2.events` and others), within BigQuery's 1 TB/month free query tier. It suits whole-archive aggregate queries with nothing to install; the result is a query answer, not a local dataset you keep and version.
- **The raw bulk archive** at `data.gdeltproject.org`: every file GDELT publishes, as CSV `.zip` files. Events come in yearly and monthly files from 1979 to March 2013 and in daily files since April 2013; GDELT 2.0's Events, Mentions and GKG 2.1 come in 15-minute files since 2015. The download-based tools on this page, GdeltForge included, read from it.

## Python packages

- **[`gdelt`](https://pypi.org/project/gdelt/) (gdeltPyR)**, the longest-established Python client, loads Events, Mentions or GKG for a date range into a pandas dataframe in one call, with parallel downloads. For an analysis that fits in memory, it is the shorter path. Its last release was in November 2023.
- **[`gdeltdoc`](https://pypi.org/project/gdeltdoc/)** ([gdelt-doc-api](https://github.com/alex9smith/gdelt-doc-api)) wraps the DOC 2.0 API and is the natural choice for recent article queries, within that API's limits.
- **[`gdelt-py`](https://rbozydar.github.io/py-gdelt/)** covers the widest API surface: six REST APIs, the three core tables, NGrams and several graph datasets, asynchronous and typed, with a BigQuery fallback. It is the choice for tables GdeltForge doesn't handle, such as VGKG.
- **[`gdelt-client`](https://pypi.org/project/gdelt-client/)** (2026) combines DOC API access with concurrent downloads of the raw Events, Mentions and GKG files for a date range.
- **[`gdelt-cli`](https://github.com/shakydata/gdelt-cli)**, a Rust command-line tool designed for use by agents, syncs GDELT data into a local DuckDB database and exports Parquet, CSV or JSON. It is the closest in approach (a local command-line tool over columnar storage); its first release, v0.1.0, came out in March 2026.
- **[`gdelt2py`](https://pypi.org/project/gdelt2py/)** and **[`getgdelt`](https://pypi.org/project/getgdelt/)** are narrower: a GKG downloader, and a downloader driven by browser automation.

## R packages

- **[`gdeltr2`](https://github.com/abresler/gdeltr2)** is the current R interface: Events, GKG, VGKG, the Full Text API and TV GKG, distributed through GitHub.
- **[`GDELTtools`](https://github.com/cran/GDELTtools)** downloads, slices and normalizes GDELT 1.0 Events data; it has been archived on CRAN.

## Cloud pipelines

Several projects load the raw archive into Spark or a particular cloud stack (Databricks notebooks for GKG, AWS Glue, Lambda, S3 and Redshift pipelines, and similar). They suit teams already on that infrastructure.

## Hosted analysis

**[GDELT Guru](https://gdelt.guru/)** describes itself as an AI-powered layer on top of the GDELT dataset, with raw events, the Global Knowledge Graph and visual datasets available through downloads and API connections.

## Where GdeltForge differs

GdeltForge's focus is sampling. It keeps the whole archive on disk as Parquet and treats sampling as a stage of its own: seeded designs (simple random, stratified by period or by column) that stream over an archive far larger than memory, on one machine, and that can be stated in a methods section. Its second concern is the Events to GKG join. GKG 2.1 carries no event identifier, only the article's URL, so the join goes through Mentions, and the relation is many-to-many: one event is reported by many articles, one article reports many events. `crossref` performs that join and keeps every combination; a one-to-one join would collapse that relation.

## Choosing

| Need | Suited to |
|---|---|
| Seeded samples of the full Events archive with a stated design, offline, on one machine | GdeltForge |
| Events joined to GKG (themes, tone, people, organizations), many-to-many | GdeltForge (`crossref`) |
| Recent article, tone or theme queries over a short window | [`gdeltdoc`](https://pypi.org/project/gdeltdoc/), or the DOC 2.0 API directly |
| A date range in a pandas dataframe, for one analysis | [`gdelt`](https://pypi.org/project/gdelt/) (gdeltPyR) |
| VGKG, TV, or tables beyond Events, GKG and Mentions | [`gdelt-py`](https://rbozydar.github.io/py-gdelt/) (Python) or [`gdeltr2`](https://github.com/abresler/gdeltr2) (R) |
| Exploring without code | The GDELT Analysis Service (see [GDELT's home page](https://www.gdeltproject.org/)) |
| SQL over the whole archive, with no local storage | BigQuery's public GDELT dataset |
| An existing Spark, Databricks or cloud pipeline | That pipeline: GdeltForge is built for one machine |

## What GdeltForge leaves out

- **Tables beyond Events, GKG and Mentions**, such as VGKG. Those three are what the Events to GKG join needs; `gdelt-py` and `gdeltr2` cover more.
- **Judging GDELT's coding.** GDELT Events are documented in the literature as high-recall and low-precision, with a real false-positive rate. `clean` repairs errors that are demonstrably GDELT's own (such as events dated 1920 for 2020), keeps GDELT's values beside each repair, and by default keeps every row. Deciding which events GDELT miscoded is a separate research problem. See [Data Cleaning](data-cleaning.md).
- **Orchestration.** Each stage is a separate command; chaining them is a shell script's job (see [Recipes](recipes.md)).
- **Hosted infrastructure and API access.** GdeltForge reads only the raw bulk archive.

## For event forecasting and temporal knowledge graphs

For research in this area (event forecasting, knowledge graphs of CAMEO quadruples), GdeltForge can serve as the ingestion and sampling step: it produces a reproducible, filterable Parquet sample that a modeling stack then reads. It does no modeling itself.
