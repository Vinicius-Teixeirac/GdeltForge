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

**Reproducible run** -- the same seed always produces the same rows, useful for sharing a dataset others can regenerate exactly:

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

**Slim version** -- keep only the columns you actually need, which keeps memory use down on large samples:

```
gdeltforge sample \
  --mode filtered \
  --filter '{ "OR": { "Actor1CountryCode": "BRA", "Actor2CountryCode": "BRA", "ActionGeo_CountryCode": "BR" } }' \
  --columns GlobalEventID Year MonthYear Day Actor1Code Actor2Code QuadClass GoldsteinScale AvgTone ActionGeo_CountryCode \
  -n 100000 \
  --out samples/brazil_slim_100k.parquet
```

## Stratified Samples

Stratified sampling draws exactly N rows per distinct value of a chosen column, producing a class-balanced dataset regardless of natural event frequencies -- see [Filtered Sampling](filtered-sampling.md) for the full filter syntax these combine with.

50k events per `QuadClass` (4 classes → 200k total rows):

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
