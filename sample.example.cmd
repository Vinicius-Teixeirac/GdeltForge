@echo off
REM =============================================================================
REM GdeltForge - Sampling Examples
REM Run after: gdeltforge scrape && gdeltforge convert && gdeltforge filter
REM =============================================================================

if not exist samples mkdir samples

REM -----------------------------------------------------------------------------
echo === Random Global Samples ===
REM -----------------------------------------------------------------------------

FOR %%n IN (100000 150000 200000 250000 300000 350000 400000 450000 500000) DO (
    echo Creating sample %%n ...
    gdeltforge sample --mode indexed -n %%n --out samples/sample_%%n.parquet
)

REM Reproducible run -- same seed always produces the same rows
gdeltforge sample ^
  --mode indexed ^
  -n 500000 ^
  --seed 42 ^
  --out samples/reproducible_500k.parquet

REM -----------------------------------------------------------------------------
echo === Daily Samples ===
REM -----------------------------------------------------------------------------

FOR %%d IN (2 3 4 5) DO (
    echo Creating daily %%d ...
    gdeltforge sample --mode daily --per-day %%d --out samples/daily_%%d.parquet
)

REM -----------------------------------------------------------------------------
echo === Brazil-Filtered Dataset ===
REM -----------------------------------------------------------------------------

gdeltforge sample ^
  --mode filtered ^
  --filter "{ \"OR\": { \"Actor1CountryCode\": \"BRA\", \"Actor2CountryCode\": \"BRA\", \"Actor1Geo_CountryCode\": \"BR\", \"Actor2Geo_CountryCode\": \"BR\", \"ActionGeo_CountryCode\": \"BR\" } }" ^
  -n 100000 ^
  --out samples/brazil_100k.parquet

REM Slim version -- keep only the columns you actually need (saves RAM)
gdeltforge sample ^
  --mode filtered ^
  --filter "{ \"OR\": { \"Actor1CountryCode\": \"BRA\", \"Actor2CountryCode\": \"BRA\", \"ActionGeo_CountryCode\": \"BR\" } }" ^
  --columns GlobalEventID Year MonthYear Day Actor1Code Actor2Code QuadClass GoldsteinScale AvgTone ActionGeo_CountryCode ^
  -n 100000 ^
  --out samples/brazil_slim_100k.parquet

REM -----------------------------------------------------------------------------
echo === Stratified Samples ===
REM Stratified sampling draws exactly N rows per distinct value of a chosen column,
REM producing a class-balanced dataset regardless of natural event frequencies.
REM -----------------------------------------------------------------------------

REM 50k events per QuadClass (4 classes -> 200k total rows)
gdeltforge sample ^
  --mode filtered ^
  --stratify QuadClass ^
  --n-per-group 50000 ^
  --out samples/stratified_quadclass_50k.parquet

REM Brazil events balanced by event type
gdeltforge sample ^
  --mode filtered ^
  --filter "{ \"ActionGeo_CountryCode\": \"BR\" }" ^
  --stratify QuadClass ^
  --n-per-group 25000 ^
  --out samples/brazil_stratified_quadclass.parquet

REM Verbal events (QuadClass 1 or 2) balanced by root-event flag
gdeltforge sample ^
  --mode filtered ^
  --filter "{ \"QuadClass\": [1, 2] }" ^
  --stratify IsRootEvent ^
  --n-per-group 50000 ^
  --out samples/verbal_stratified_rootflag.parquet
