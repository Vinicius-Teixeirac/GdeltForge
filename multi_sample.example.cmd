@echo off
echo === Random Global Samples ===

FOR %%n IN (10000 15000 20000 25000 30000 35000 40000 45000 50000) DO (
    echo Creating sample %%n ...
    python main.py sample --mode indexed -n %%n --out sample_%%n.parquet
)

echo === Daily Samples ===

FOR %%d IN (2 3 4 5) DO (
    echo Creating daily %%d ...
    python main.py sample --mode daily --per-day %%d --out daily_%%d.parquet
)

echo === Brazil-Filtered Dataset ===

python main.py sample ^
  --mode filtered ^
  --filter "{ \"OR\": { \"Actor1CountryCode\": \"BRA\", \"Actor2CountryCode\": \"BRA\", \"Actor1Geo_CountryCode\": \"BR\", \"Actor2Geo_CountryCode\": \"BR\", \"ActionGeo_CountryCode\": \"BR\" } }" ^
  -n 10000 ^
  --out brazil_10k.parquet

