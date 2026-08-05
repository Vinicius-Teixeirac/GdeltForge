# Recipes

Practical, runnable sampling recipes, assuming you've already run:

```
gdeltforge scrape
gdeltforge convert
gdeltforge filter
```

## Random Global Samples

Sweep a range of sample sizes in one pass:

```bash
mkdir -p samples
for n in 100000 150000 200000 250000 300000 350000 400000 450000 500000; do
    gdeltforge sample --mode indexed -n "$n" --out "samples/sample_${n}.parquet"
done
```

**Reproducible run**: the same seed always produces the same rows, useful for sharing a dataset others can regenerate exactly:

```
gdeltforge sample \
  --mode indexed \
  -n 500000 \
  --seed 42 \
  --out samples/reproducible_500k.parquet
```

## Daily Samples

A fixed number of rows per day, across the whole period your downloaded data covers:

```bash
for d in 2 3 4 5; do
    gdeltforge sample --mode daily --per-day "$d" --out "samples/daily_${d}.parquet"
done
```

## Country-Filtered Dataset

Combine an `OR` block across every column that can carry a country code to catch all Brazil-related events, regardless of which actor or location field it showed up in:

```
gdeltforge sample \
  --mode filtered \
  --filter '{ "OR": { "Actor1CountryCode": "BRA", "Actor2CountryCode": "BRA", "Actor1Geo_CountryCode": "BR", "Actor2Geo_CountryCode": "BR", "ActionGeo_CountryCode": "BR" } }' \
  -n 100000 \
  --out samples/brazil_100k.parquet
```

**Slim version**: keep only the columns you actually need, which keeps memory use down on large samples:

```
gdeltforge sample \
  --mode filtered \
  --filter '{ "OR": { "Actor1CountryCode": "BRA", "Actor2CountryCode": "BRA", "ActionGeo_CountryCode": "BR" } }' \
  --columns GlobalEventID Year MonthYear Day Actor1Code Actor2Code QuadClass GoldsteinScale AvgTone ActionGeo_CountryCode \
  -n 100000 \
  --out samples/brazil_slim_100k.parquet
```

## Stratified Samples

Stratified sampling draws exactly N rows per distinct value of a chosen column, producing a class-balanced dataset regardless of natural event frequencies; see [Filtered Sampling](filtered-sampling.md) for the full filter syntax these combine with.

50k events per `QuadClass` (4 classes -> 200k total rows):

```
gdeltforge sample \
  --mode filtered \
  --stratify QuadClass \
  --n-per-group 50000 \
  --out samples/stratified_quadclass_50k.parquet
```

Brazil events balanced by event type:

```
gdeltforge sample \
  --mode filtered \
  --filter '{ "ActionGeo_CountryCode": "BR" }' \
  --stratify QuadClass \
  --n-per-group 25000 \
  --out samples/brazil_stratified_quadclass.parquet
```

Verbal events (`QuadClass` 1 or 2) balanced by root-event flag:

```
gdeltforge sample \
  --mode filtered \
  --filter '{ "QuadClass": [1, 2] }' \
  --stratify IsRootEvent \
  --n-per-group 50000 \
  --out samples/verbal_stratified_rootflag.parquet
```

## GKG-Enriched Events

Sample Events, then enrich the sample with GKG (themes, tone, people, organizations) via `crossref`. GKG 2.1, the current format, carries no event ID at all, only the source article's URL, so it needs Mentions too, as the bridge to Events; legacy GKG 1.0 carries `EventIds` directly, so it skips straight to GKG. See [Comparison](comparison.md#what-gdeltforge-actually-does-differently) for why that two-hop join is worth a dedicated command rather than a one-line pandas merge.

**GKG 2.1** (live since Feb 2015, updated every 15 minutes):

```bash
gdeltforge scrape --dataset gkg-v2 --start-date 2020-01-01 --end-date 2020-01-31
gdeltforge scrape --dataset mentions --start-date 2020-01-01 --end-date 2020-01-31
gdeltforge convert --dataset gkg-v2
gdeltforge convert --dataset mentions
gdeltforge filter --dataset gkg-v2
gdeltforge filter --dataset mentions

gdeltforge sample --mode indexed -n 5000 --seed 42 --out samples/events_5k.parquet

gdeltforge crossref \
  --events samples/events_5k.parquet \
  --gkg-version v2 \
  --out samples/events_gkg_5k.parquet
```

**GKG 1.0** (legacy: the primary feed April 2013 through February 2015, still published daily since for backwards compatibility) is a direct join, no Mentions needed:

```bash
gdeltforge scrape --dataset gkg-v1 --start-date 2020-01-01 --end-date 2020-01-31
gdeltforge convert --dataset gkg-v1
gdeltforge filter --dataset gkg-v1

gdeltforge crossref \
  --events samples/events_5k.parquet \
  --gkg-version v1 \
  --out samples/events_gkgv1_5k.parquet
```

Both preserve the real many-to-many structure rather than collapsing it: one event can produce several output rows (one per article that covered it), and one article covering several events contributes one row per event.
