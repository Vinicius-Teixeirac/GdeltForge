#!/usr/bin/env bash

echo "=== Random Global Samples ==="

for n in 10000 15000 20000 25000 30000 35000 40000 45000 50000; do
    echo "Creating sample $n ..."
    python main.py sample --mode indexed -n "$n" --out "sample_${n}.parquet"
done

echo "=== Daily Samples ==="

for d in 2 3 4 5; do
    echo "Creating daily $d ..."
    python main.py sample --mode daily --per-day "$d" --out "daily_${d}.parquet"
done

echo "=== Brazil-Filtered Dataset ==="

python main.py sample \
  --mode filtered \
  --filter '{ "OR": { "Actor1CountryCode": "BRA", "Actor2CountryCode": "BRA", "Actor1Geo_CountryCode": "BR", "Actor2Geo_CountryCode": "BR", "ActionGeo_CountryCode": "BR" } }' \
  -n 10000 \
  --out brazil_10k.parquet
