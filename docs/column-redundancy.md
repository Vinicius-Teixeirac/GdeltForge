# Column Redundancy

GDELT's tables carry a lot of overlapping fields: GKG 2.1 ships both a plain
and an "enhanced" version of several columns, Events derives some columns
from others, and Mentions and GKG 2.1 both carry a source label for the
same article. This page documents which of that overlap is genuinely
redundant, measured against real GDELT data rather than assumed from the
codebook, and which looks redundant but isn't.

Nothing here is synthetic. Every number below comes from either the local
converted dataset or a fresh `scrape` + `convert` + `filter` run against
`data.gdeltproject.org`, using the pipeline's own code paths, not ad hoc
parsing.

## GKG 2.1: `V1THEMES` vs. `V2ENHANCEDTHEMES`

GKG 2.1 ships two forms of its theme, person, and organization tags: a
plain semicolon-delimited list (`V1THEMES`, `V1PERSONS`,
`V1ORGANIZATIONS`) and an "enhanced" version carrying the same tags plus a
character offset per occurrence (`V2ENHANCEDTHEMES`,
`V2ENHANCEDPERSONS`, `V2ENHANCEDORGANIZATIONS`). The natural assumption is
that the enhanced field is a strict superset: same tags, plus position
data. That assumption is wrong for themes, and only mostly right for
persons and organizations.

### What the data shows

Comparing each `V1*` field against its `V2ENHANCED*` counterpart as sets
(stripping offsets, matching case) gives the opposite relationship: the
enhanced field is consistently the one missing content, not the other way
around.

| Field | V2Enhanced ⊆ V1 | V1 ⊆ V2Enhanced | Extra items V1 has, per row (avg) |
|---|---|---|---|
| Themes | 99.94% | 5.54% | 3.35 |
| Persons | 100.00% | 92.07% | 0.13 |
| Organizations | 100.00% | 77.71% | 0.34 |

`V2ENHANCED*` is essentially always a subset of `V1*`. Its only unique
contribution is character offsets and original text casing, neither of
which anything in this codebase reads. `V1*`, meanwhile, regularly carries
content the enhanced version drops entirely, themes especially.

### Verified at scale, across the full GKG 2.1 history

The numbers above come from one local sample (2020-01-01, ~83K rows). To
check they weren't an artifact of that one day, the same comparison was
re-run twice more, at increasing scale, against data pulled fresh from
GDELT for each:

1. **12 single-file snapshots**: the first 15-minute batch of one day per
   year, 2015 to 2026, scraped, converted, and filtered independently.
2. **11 full days**: every 15-minute batch (96/day) of one day per year,
   2015 to 2026, same pipeline. 1,054 files, 2,146,256 filtered rows,
   1,917,585 rows with both theme fields present, 51,724,547 individual
   `V1THEMES` tag instances checked.

| Metric | Single day (2020) | 12 single-file snapshots | 11 full days, 2.1M rows |
|---|---|---|---|
| V2Enhanced ⊆ V1 | 99.94% | 99.11% | 99.08% |
| V1 tag instances invisible to V2Enhanced | 11.46% | 11.43% | 11.40% |
| Rows where V1 has tags V2Enhanced lacks | 94.46% | 93.94% | 93.74% |

Three independent samples, three different scales, agree to within 0.06
percentage points on the headline number.

Per year, the gap is stable and shows no drift across a decade:

| Year | Rows | V2 ⊆ V1 | Tag instances invisible to V2Enhanced |
|---|---|---|---|
| 2015 | 210,879 | 99.95% | 12.14% |
| 2016 | 331,313 | 99.98% | 11.67% |
| 2017 | 269,576 | 99.97% | 11.45% |
| 2018 | 223,326 | 99.94% | 11.51% |
| 2019 | 108,251 | 99.91% | 11.53% |
| 2020 | 150,326 | 99.98% | 10.94% |
| 2021 | 138,189 | 99.96% | 10.80% |
| 2022 | 126,701 | 92.75% | 11.29% |
| 2023 | 176,524 | 96.67% | 11.20% |
| 2024 | 87,048 | 98.29% | 11.60% |
| 2026 | 95,452 | 99.44% | 10.58% |

2025-06-15 has no files at all in GDELT's own master file list for that
date, a real gap in their archive, not a limitation of this method. 2015,
the launch year, has the highest gap (12.14%), consistent with GKG 2.1's
theme-tagging pipeline still maturing right after the Feb 2015 launch.
2022 is the one year where the V2 ⊆ V1 relationship weakens noticeably
(92.75%), worth flagging as an outlier but not enough to change the
overall pattern.

### Why the gap exists

It isn't random. The tags missing from `V2ENHANCEDTHEMES` are dominated by
one family: GDELT's dictionary/taxonomy themes (`TAX_*`), plus a handful
of similar whole-document categories.

| Theme code | Missing instances (11-full-day sample) |
|---|---|
| `TAX_FNCACT` | 1,665,250 |
| `TAX_ETHNICITY` | 683,021 |
| `EPU_POLICY` | 673,842 |
| `SOC_POINTSOFINTEREST` | 527,281 |
| `TAX_WORLDLANGUAGES` | 495,476 |
| `TAX_DISEASE` | 307,126 |
| `TAX_WORLDMAMMALS` | 280,926 |
| `TAX_MILITARY_TITLE` | 204,051 |
| `TAX_RELIGION` | 154,054 |
| `TAX_POLITICAL_PARTY` | 150,886 |

These are keyword-dictionary themes, detected by scanning the whole
document for matches against a word list rather than one specific text
span. GKG 2.1's "enhanced" pass only keeps a tag if it can anchor it to a
character offset; dictionary-count themes apparently don't always get
one, so they surface in `V1THEMES`'s simpler "is this theme present"
tagging and are silently absent from `V2ENHANCEDTHEMES`.

The reverse direction stays negligible at every scale checked: in the
full 11-day sample, only 13 distinct theme codes ever appear in
`V2ENHANCEDTHEMES` but not `V1THEMES`, totaling 17,713 instances against
51.7 million, 0.03%.

### Conclusion

`V1THEMES`, `V1PERSONS`, and `V1ORGANIZATIONS` are the more complete tag
sets. `V2ENHANCEDTHEMES`, `V2ENHANCEDPERSONS`, and
`V2ENHANCEDORGANIZATIONS` trade some of that completeness, themes
especially, for character-offset data nothing in this codebase uses.

Within `converter.output_columns.gdelt_gkg_v2` / `filter.output_columns.gdelt_gkg_v2`
as currently configured, the three `V2ENHANCED*` fields account for **60%
of the pruned dataset's size** (107.7 of 178.4 MB in a compressed-size
sample), so dropping them and keeping only the `V1*` fields more than
halves GKG 2.1's storage footprint under this pipeline's current column
selection, for close to zero information loss.

## Other columns confirmed redundant

### Events: `Year`, `MonthYear`, `EventBaseCode`, `EventRootCode`

Exact integer/string identities, not correlations:

```
MonthYear     == Day // 100
Year          == Day // 10000
EventBaseCode == EventCode[:3]
EventRootCode == EventCode[:2]
```

100.00% match, zero exceptions, across ~7M real Events rows spanning five
files from 1990 to 2026. Storage impact is modest: these are small,
low-cardinality columns Parquet already dictionary-encodes hard, so
together they're only about 2% of Events' on-disk size (roughly 1.1 GB of
a 56 GB `events/parquet` sample). Real and risk-free to drop, just not the
main lever.

### GKG 2.1: `V2SOURCECOMMONNAME`

Derivable from the domain of `V2DOCUMENTIDENTIFIER`. Exact
`urlparse(...).netloc` match on 90.21% of a 227,378-row sample; the
remaining ~10% differ only by subdomain (e.g. `chicago.suntimes.com` vs.
the stored `suntimes.com`) or a multi-label TLD (`chinadaily.com.cn`),
i.e. GDELT's own registrable-domain canonicalization, not independent
information. Already excluded from `output_columns.gdelt_gkg_v2`.

### GKG 2.1: `V2SOURCECOLLECTIONIDENTIFIER`

Constant across every row observed with the column present: value `1`
(web-scraped news) in 100.00% of 3,167,917 rows. Codes 2 through 6
(citation-only, core, DTIC/government, JSTOR, non-textual) are legacy
markers from GDELT's historical/batch corpora that were never wired into
the live 15-minute feed this pipeline scrapes from. Zero information
content for data from this source; already excluded from
`output_columns.gdelt_gkg_v2`.

## Cross-table: `Mentions.MentionSourceName` vs. `GKG_V2SOURCECOMMONNAME`

Both fields hold the same source-domain label for an article. Joined on
the shared document URL (`MentionIdentifier` = `V2DOCUMENTIDENTIFIER`)
across two dates 6 years apart, the values are identical for every
matched row:

| Date | Joined rows | Case-sensitive exact match |
|---|---|---|
| 2020-01-01 | 301,097 | 100.00% |
| 2026-07-31 | 313,640 | 100.00% |

Since `V2SOURCECOMMONNAME` is already pruned from GKG 2.1's output (see
above), this isn't live duplication on disk today. Noted for anyone
considering adding `MentionSourceName` to `crossref`'s Mentions payload
columns: don't, it would only duplicate `GKG_V2SOURCECOMMONNAME`, which
already flows through the crossref join by default.

## Checked, and not redundant

Not every overlapping-looking pair turned out to be one. These were
tested and rejected, kept here so the same ground isn't re-investigated
later.

### Events: `Actor1Code` vs. concatenating its 8 sub-fields

GDELT's CAMEO actor codes are documented as a concatenation of
`Actor1CountryCode` + `Actor1KnownGroupCode` + `Actor1EthnicCode` +
`Actor1Religion1Code` + `Actor1Religion2Code` + `Actor1Type1Code` +
`Actor1Type2Code` + `Actor1Type3Code`. Only 95.24-97.13% match across five
sample files spanning 1990-2026, not 100%. Inspecting mismatches shows
genuine exceptions, not an ordering bug: some codes (e.g. `CHRANG001`)
contain characters absent from every one of the 8 sub-fields checked.
Dropping either side would lose real data in 3-5% of rows. (Only
`Actor1Code` was tested directly; `Actor2Code` was assumed to follow the
same pattern but wasn't independently verified.)

### Events: `Actor1Geo_*` / `Actor2Geo_*` / `ActionGeo_*`

Tested for lat/long overlap on one real file (2003, 5.5M rows):
Actor1Geo matches ActionGeo 68.64% of the time, Actor2Geo matches
ActionGeo 60.26%, Actor1Geo matches Actor2Geo 44.08%. That's real-world
correlation (many events are domestic, so the actors' and the action's
locations coincide), not database redundancy. Each pair diverges in a
third to over half of rows and none should be dropped.

### Mentions: `Extras`, `MentionDocTranslationInfo`

100% empty across every row sampled. Not worth dropping for storage,
though: Parquet's run-length encoding already compresses these to about
0.02% of Mentions' size each, so removing them saves close to nothing.
Only worth dropping for schema simplicity, if at all.

## Reproducing these numbers

The GKG 2.1 theme comparisons were run by calling this pipeline's own
`collect_gdelt_links`, `download_gdelt_files`, `GDELTConverter`, and
`run_filter` directly (not through the `gdeltforge` CLI, to select
individual 15-minute files rather than whole days or date ranges),
against a config pointed at scratch paths so the run never touched a
locally cached `data/` directory or its resumability markers. Row/tag
counts were then computed by loading `V1THEMES` and `V2ENHANCEDTHEMES`
from the resulting filtered Parquet and comparing them as sets per row.
None of the downloaded or intermediate files from these runs are kept in
this repository; only the aggregate numbers above are.
