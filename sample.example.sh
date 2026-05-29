#!/usr/bin/env bash

# =============================================================================
# GDELT Pipeline - Sampling Examples
# Run after: python main.py scrape && python main.py convert && python main.py filter
# =============================================================================

mkdir -p samples

# -----------------------------------------------------------------------------
echo "=== Random Global Samples ==="
# -----------------------------------------------------------------------------

for n in 100000 150000 200000 250000 300000 350000 400000 450000 500000; do
    echo "Creating sample $n ..."
    python main.py sample --mode indexed -n "$n" --out "samples/sample_${n}.parquet"
done

# Reproducible run — same seed always produces the same rows
python main.py sample \
  --mode indexed \
  -n 500000 \
  --seed 42 \
  --out samples/reproducible_500k.parquet

# -----------------------------------------------------------------------------
echo "=== Daily Samples ==="
# -----------------------------------------------------------------------------

for d in 2 3 4 5; do
    echo "Creating daily $d ..."
    python main.py sample --mode daily --per-day "$d" --out "samples/daily_${d}.parquet"
done

# -----------------------------------------------------------------------------
echo "=== Brazil-Filtered Dataset ==="
# -----------------------------------------------------------------------------

python main.py sample \
  --mode filtered \
  --filter '{ "OR": { "Actor1CountryCode": "BRA", "Actor2CountryCode": "BRA", "Actor1Geo_CountryCode": "BR", "Actor2Geo_CountryCode": "BR", "ActionGeo_CountryCode": "BR" } }' \
  -n 100000 \
  --out samples/brazil_100k.parquet

# Slim version — keep only the columns you actually need (saves RAM)
python main.py sample \
  --mode filtered \
  --filter '{ "OR": { "Actor1CountryCode": "BRA", "Actor2CountryCode": "BRA", "ActionGeo_CountryCode": "BR" } }' \
  --columns GlobalEventID Year MonthYear Day Actor1Code Actor2Code QuadClass GoldsteinScale AvgTone ActionGeo_CountryCode \
  -n 100000 \
  --out samples/brazil_slim_100k.parquet

# -----------------------------------------------------------------------------
echo "=== Stratified Samples ==="
# Stratified sampling draws exactly N rows per distinct value of a chosen column,
# producing a class-balanced dataset regardless of natural event frequencies.
# -----------------------------------------------------------------------------

# 50k events per QuadClass (4 classes -> 200k total rows)
python main.py sample \
  --mode filtered \
  --stratify QuadClass \
  --n-per-group 50000 \
  --out samples/stratified_quadclass_50k.parquet

# Brazil events balanced by event type
python main.py sample \
  --mode filtered \
  --filter '{ "ActionGeo_CountryCode": "BR" }' \
  --stratify QuadClass \
  --n-per-group 25000 \
  --out samples/brazil_stratified_quadclass.parquet

# Verbal events (QuadClass 1 or 2) balanced by root-event flag
python main.py sample \
  --mode filtered \
  --filter '{ "QuadClass": [1, 2] }' \
  --stratify IsRootEvent \
  --n-per-group 50000 \
  --out samples/verbal_stratified_rootflag.parquet
