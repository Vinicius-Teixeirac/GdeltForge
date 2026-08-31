# Crossref Join Semantics

`crossref_events_gkg_v2`'s two-hop join (`Events -[GlobalEventID]-> Mentions
-[document URL]-> GKG 2.1`, see [Configuration](configuration.md#output_columns-and-crossref-four-columns-you-cant-prune-away))
deliberately preserves a many-to-many structure rather than collapsing it:
one event can match several articles, and one article can cover several
events. That's documented behavior, not a surprise. What isn't obvious
from the docstring alone is how often it actually happens, and that the
join has two separate, independent sources of repeated rows for the same
(event, article) pair, only one of which used to be handled. This page
records the real-data investigation behind both, and the resulting
`on_duplicate_document`/`dedupe_mentions` parameters.


![GKG 2.1 joins to Events in two hops through Mentions on the article URL; GKG 1.0 joins directly on EventIds. Both preserve the many-to-many structure.](assets/crossref-join.svg)

## Two events sharing an article get identical GKG features

Ran the real `crossref_events_gkg_v2` against 5,000 real events from
2020-01-01 and local Mentions/GKG 2.1 data:

- 12,954 distinct articles matched → **7,542 of them (58.2%)** were
  matched to more than one distinct event.
- Most extreme case in the sample: one Daily Mail article was the
  matching document for **184 distinct `GlobalEventID`s**, and every one
  of them carries the identical `GKG_V1THEMES` and `GKG_V1.5TONE` value,
  confirmed programmatically, not eyeballed.

This isn't a bug. Events' CAMEO extraction can pull several separate
actor/action tuples out of one article (each qualifying pair becomes its
own event record), while GKG's enrichment (themes, tone, persons,
organizations) is computed once, for the whole document. N events sourced
from the same article all inherit that one document's single set of GKG
features; they are not independent observations of what the article was
about.

## The same event can appear in many rows

Same run: of the 4,999 events that matched at all, **2,584 (51.7%)**
appeared in more than one output row. Most extreme case: a single event
about the Australian bushfires appeared in **626 separate rows**, one per
distinct article that covered it that day. This matches the documented
contract exactly: "an event mentioned by several articles contributes
several rows."

Practical implication for anything downstream of this join: rows are not
independent event observations. If you need one row per event, aggregate
on `GlobalEventID` after the join. If you're doing anything statistical
with the GKG-side columns, rows sharing an article are not independent
samples of that article's features; they're the same values copied across
every event pulled from it.

## Duplicate rows, source one: GKG 2.1 recrawling the same URL

GKG 2.1 can carry more than one record for the same `V2DOCUMENTIDENTIFIER`.
Without deduplication, one Mentions row referencing that URL would match
every GKG record for it, multiplying the join for reasons that have
nothing to do with how many events or articles are actually involved.

Scanned `GKGRECORDID`/`V2DOCUMENTIDENTIFIER` across the full local
`gkg_v2/parquet` dataset (3,368,659 rows): **3 URLs out of 3,368,654**
distinct ones appear more than once. Rare, but real:

```text
GKGRECORDID              V2DOCUMENTIDENTIFIER
20200101040000-94        https://www.nbcchicago.com/tag/east-garfield-park/
20230601010000-1643      https://www.nbcchicago.com/tag/east-garfield-park/
```

That's a tag/listing page, not a single article, recrawled three years
apart. Its content genuinely differs between visits (different stories
tagged under it at each point), so the two records aren't a stale
duplicate of the same information; they're two different, both legitimate,
snapshots. Silently picking one and discarding the other is a real
editorial choice, not noise removal, and the "obvious" default isn't
even clearly correct: always keeping the globally most recent record can
mean joining a 2020 event to content GDELT captured in 2023, rather than
the snapshot that was actually current when the event's mention was
recorded.

**Resolution**: `on_duplicate_document` (`crossref_events_gkg_v2`,
forwarded through `crossref_events_gkg_auto`; CLI:
`--on-duplicate-document`):

| Value | Behavior |
|---|---|
| `"all"` (default) | Keep every record; a shared URL then contributes one row per (event, article, GKG record) instead of one. Nothing is silently discarded. |
| `"latest"` | Keep only the chronologically most recent record. |
| `"earliest"` | Keep only the chronologically first record. |

This only affects the rare URL that genuinely has more than one GKG
record; every other URL is unaffected regardless of the setting. `"all"`
is the default rather than `"latest"` deliberately: picking a single
winner is an editorial choice, and per the example above, "most recent"
isn't even reliably the *right* choice, so nothing is discarded unless a
caller opts into it.

## Duplicate rows, source two: Mentions recording one row per sentence

A separate, more common source of repetition has nothing to do with GKG.
Checked raw Mentions rows directly (2020-01-01, before any GKG join): of
320,920 distinct `(GlobalEventID, MentionIdentifier)` pairs, **6,187
(1.93%) already appear more than once**. Mentions records one row per
sentence that references an event, so an article mentioning the same
event in two different sentences produces two raw rows for that one
(event, article) relationship, before GKG is even involved.

These rows are not always literal duplicates. Of the two Mentions-side
columns this join actually carries through (`MentionTimeDate`,
`Confidence`), a check across 2,000 duplicated pairs found **453 (22.6%)
differ in `Confidence` and/or `SentenceID`** across their duplicate rows.
Example, the same event mentioned twice in what turned out to be the
same sentence, detected as Actor1 in one raw row and Actor2 in the other:

```text
GLOBALEVENTID  SentenceID  Confidence  Actor1CharOffset  Actor2CharOffset
813416601      1           50          -1                377
813416601      1           50          377               -1
```

This particular pair happens to share `Confidence`, but across the
sampled duplicates, differing `Confidence` was common enough (22.6%) that
treating repeated rows as pure noise would be wrong: since this join
never reads `SentenceID` at all, row count was, until now, the *only*
surviving signal of how many times an event was actually mentioned within
one article.

**Resolution**: `dedupe_mentions` (`crossref_events_gkg_v2`, forwarded
through `crossref_events_gkg_auto`; CLI: `--collapse-duplicate-mentions`
to enable it):

- **`False` (default)**: every raw Mentions row is kept, so the row
  count itself still tells you how many times an event was mentioned
  within an article, and nothing is silently collapsed away.
- **`True`**: raw Mentions rows for the same (event, article) pair
  collapse into one, keeping the highest-`Confidence` row when
  `Confidence` is available. A new `Mention_Count` column records how
  many raw rows collapsed into it, so mention frequency survives as
  explicit data instead of being implicitly, fragilely tied to row
  count.

Same reasoning as the GKG-side default above: collapsing rows is a real
choice about what "one result" means, not risk-free noise removal, so
it's opt-in rather than silently applied.

## Methodology: verify schema uniformity before trusting a cross-file number

Both investigations above read Parquet directories with `pyarrow.dataset`
across many files. That API tolerates per-file schema differences and
fills a column missing from one file with `null` rather than raising,
which already produced one wrong conclusion earlier in this pipeline's
history: an apparent ~6% null rate for `V2SOURCECOLLECTIONIDENTIFIER`
that turned out to be 96 files converted under an older, narrower
`output_columns` setting, not real missing data.

Before trusting a number produced this way, check the actual files, not
just the config: `output_columns` is only configured for `gdelt_gkg_v2`
in this pipeline (confirmed via `grep` over `settings.yaml`), and all
33,303 local Mentions files were confirmed to share one identical
16-column schema before the sentence-duplicate numbers above were
reported. The GKG 2.1 cardinality numbers requested only
`V2DOCUMENTIDENTIFIER`, `V1THEMES`, and `V1.5TONE`, columns present in
every local GKG 2.1 file regardless of the `output_columns` split,
avoiding the same trap, though only because those particular columns
happened to be safe, not because the mismatch was ruled out in advance.
`pq.ParquetFile(f).schema_arrow.names` consistency across the actual file
set is worth checking explicitly before trusting any cross-file
aggregate against this pipeline's output, since its own history shows the
config and the files on disk can disagree.
