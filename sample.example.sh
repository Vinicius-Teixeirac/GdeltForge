#!/usr/bin/env bash

echo "=== Random Global Samples ==="

for n in 100000 150000 200000 250000 300000 350000 400000 450000 500000; do
    echo "Creating sample $n ..."
    python main.py sample --mode indexed -n "$n" --out "samples/sample_${n}.parquet"
done

echo "=== Daily Samples ==="

for d in 2 3 4 5; do
    echo "Creating daily $d ..."
    python main.py sample --mode daily --per-day "$d" --out "samples/daily_${d}.parquet"
done

echo "=== Brazil-Filtered Dataset ==="

python main.py sample \
  --mode filtered \
  --filter '{ "OR": { "Actor1CountryCode": "BRA", "Actor2CountryCode": "BRA", "Actor1Geo_CountryCode": "BR", "Actor2Geo_CountryCode": "BR", "ActionGeo_CountryCode": "BR" } }' \
  -n 100000 \
  --out "samples/brazil_100k.parquet"