"""
places.py

The place table behind the clean stage's places step (clean.places, see
docs/data-cleaning.md#places): every (FeatureID, Lat, Long) combination
GDELT's Events archive carries that names a place, with that place's ID,
type, canonical point and name. It is built once from the full archive by
tools/build_place_table.py and ships inside the package, so resolving a
point never needs anything but the point itself.

A place's ID is GDELT's own FeatureID, except where one ID names places in
two gazetteers: then a US state takes its ADM1 code (CA is Canada, so
California is USCA) and a GNS place the gns: prefix (449676 is Indiana
University in GNIS, so Kula, Afghanistan is gns:449676).

Provides:
    - place_table: the table, one row per combination
    - place_readings: one row per place, with GDELT's own FeatureID for it
      and the gazetteer it reads that ID in
    - place_ids: per FeatureID, how many places use it
    - place_table_info: what the table was built from (rule version,
      archive span, counts)
    - place_table_id: a short identity of the table, for the resumability
      fingerprint and the cleaned-file marker
    - GAZETTEERS: the gazetteer each GDELT Type reads a FeatureID in
"""

from __future__ import annotations

import json
from functools import lru_cache
from importlib.resources import as_file, files

import polars as pl

INFO_KEY = "gdeltforge:places"
# The table's columns: the key GDELT wrote, then what it resolves to.
KEY_COLUMNS = ("FeatureID", "Lat", "Long")
PLACE_COLUMNS = ("place", "place_type", "place_lat", "place_lon", "place_name")
# GDELT's Type says which gazetteer a FeatureID is from: 1 a FIPS country
# code, 2 a US state code, 3 a GNIS ID (US places), 4 and 5 a GNS ID (the
# rest of the world: cities and landmarks, then first-level divisions).
GAZETTEERS = {1: "country", 2: "state", 3: "gnis", 4: "gns", 5: "gns"}


def _resource():
    return files("gdeltforge") / "data" / "places.parquet"


@lru_cache(maxsize=1)
def place_table() -> pl.DataFrame:
    """
    The table: FeatureID (as GDELT wrote it, "" where it wrote none), Lat
    and Long (float64, exactly as converted; both null for a FeatureID
    written without a location), then place, place_type, place_lat,
    place_lon and place_name. About 80 MB in memory, loaded once per
    process.
    """
    with as_file(_resource()) as path:
        # Not memory-mapped: on Windows a mapped file can't be replaced
        # while any process holds it, which would block reinstalling.
        return pl.read_parquet(path, memory_map=False)


@lru_cache(maxsize=1)
def place_readings() -> pl.DataFrame:
    """
    One row per place: FeatureID, GDELT's own ID for it (gns:449676 ->
    449676, USCA -> CA), gazetteer, and the place columns. A point the
    table doesn't know names the place its FeatureID has in the gazetteer
    its Type says, when there is one.
    """
    readings = place_table().select(PLACE_COLUMNS).unique("place", maintain_order=True)
    place = pl.col("place")
    gdelt_id = (
        pl.when(place.str.contains(":")).then(place.str.replace(r"^[a-z]+:", ""))
        .when((pl.col("place_type") == 2) & place.str.contains(r"^US[A-Z]{2}$"))
        .then(place.str.slice(2))
        .otherwise(place)
    )
    gazetteer = pl.col("place_type").cast(pl.Int64).replace_strict(
        GAZETTEERS, return_dtype=pl.String
    )
    return readings.select(gdelt_id.alias("FeatureID"), gazetteer.alias("gazetteer"),
                           *PLACE_COLUMNS)


@lru_cache(maxsize=1)
def place_ids() -> pl.DataFrame:
    """Per GDELT FeatureID the places use: how many (`readings`) and, where
    exactly one, that place's columns. An untyped point the table doesn't
    know names a place only through an ID one place uses."""
    return place_readings().group_by("FeatureID", maintain_order=True).agg(
        pl.len().alias("readings"), *[pl.col(c).first() for c in PLACE_COLUMNS]
    )


@lru_cache(maxsize=1)
def place_table_info() -> dict:
    """The table's own metadata: the rule version it was built under, the
    archive files it was built from, and its counts."""
    with as_file(_resource()) as path:
        return json.loads(pl.read_parquet_metadata(path)[INFO_KEY])


def place_table_id() -> str:
    """The rule version and the archive span, e.g. "v1, 1979.parquet to
    20260731.export.parquet": a rebuilt table changes it."""
    info = place_table_info()
    source = info.get("source") or {}
    return (
        f"v{info['version']}, {source.get('first_file', '?')} to "
        f"{source.get('last_file', '?')}"
    )
