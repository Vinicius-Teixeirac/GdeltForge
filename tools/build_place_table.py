"""
build_place_table.py

Maintainer tool: builds the place table gdeltforge bundles for the clean
stage's place resolution (clean.places, see docs/data-cleaning.md), from a
full converted Events archive. Users never run it; the tables it writes
ship inside the package.

Two stages, each resumable from the previous one's output:

    python tools/build_place_table.py combos <converted events dir> <combos.parquet>
    python tools/build_place_table.py places <combos.parquet> <output dir>

combos: every distinct (FeatureID, Type, FullName, Lat, Long) the archive's
three geo points ever carry, with how many geo values carry it (`values`)
and the first and last event Year it occurs in. Exact values as converted:
FeatureID as GDELT writes it, Lat/Long as float64, Type as an integer.

places: resolves every combination to a place with the rules
docs/data-cleaning.md describes, and writes

    places.parquet       (FeatureID, Lat, Long) -> the place's ID, type,
                         canonical point and name, with Lat and Long null
                         for a FeatureID written without a location; the
                         table gdeltforge bundles as
                         src/gdeltforge/data/places.parquet
    assignments.parquet  every combination with the rule that decided it,
                         for review; not bundled

Type and FullName are used only here, as witnesses; the clean step looks
places up by FeatureID, Lat and Long alone.
"""

import glob
import json
import os
import sys
import time

import polars as pl

ROLES = ("Actor1Geo", "Actor2Geo", "ActionGeo")
COMBO_KEYS = ["FeatureID", "Type", "FullName", "Lat", "Long"]
MERGE_EVERY = 50


def _file_combos(path: str) -> pl.DataFrame:
    lf = pl.scan_parquet(path)
    parts = [
        lf.select(
            pl.col(f"{r}_FeatureID").cast(pl.String).alias("FeatureID"),
            pl.col(f"{r}_Type").cast(pl.Float64).cast(pl.Int64, strict=False).alias("Type"),
            pl.col(f"{r}_FullName").cast(pl.String).alias("FullName"),
            pl.col(f"{r}_Lat").cast(pl.Float64).alias("Lat"),
            pl.col(f"{r}_Long").cast(pl.Float64).alias("Long"),
            pl.col("Year").cast(pl.Int64).alias("Year"),
        )
        for r in ROLES
    ]
    return (
        pl.concat(parts)
        .group_by(COMBO_KEYS)
        .agg(pl.len().cast(pl.Int64).alias("values"),
             pl.col("Year").min().alias("first_year"),
             pl.col("Year").max().alias("last_year"))
        .collect(engine="streaming")
    )


def _merge(frames: list[pl.DataFrame]) -> pl.DataFrame:
    return (
        pl.concat(frames)
        .group_by(COMBO_KEYS)
        .agg(pl.col("values").sum(), pl.col("first_year").min(), pl.col("last_year").max())
    )


def build_combos(archive: str, out: str) -> None:
    files = sorted(glob.glob(os.path.join(archive, "*.parquet")))
    if not files:
        sys.exit(f"no parquet files in {archive}")
    start, total, pending, failed = time.time(), None, [], []
    for i, path in enumerate(files, 1):
        try:
            pending.append(_file_combos(path))
        except Exception as exc:  # a corrupt file must not end a multi-hour run
            failed.append(os.path.basename(path))
            print(f"skipped {os.path.basename(path)}: {exc}", flush=True)
        if len(pending) >= MERGE_EVERY or i == len(files):
            total = _merge(([total] if total is not None else []) + pending)
            pending = []
            print(f"{i}/{len(files)} files, {total.height:,} combinations, "
                  f"{time.time() - start:.0f}s", flush=True)
    assert total is not None
    # Which archive the table describes, carried into places.parquet.
    source = {"files": len(files) - len(failed), "first_file": os.path.basename(files[0]),
              "last_file": os.path.basename(files[-1]), "skipped": failed}
    total.sort(["FeatureID", "Type", "Lat", "Long", "FullName"], nulls_last=True).write_parquet(
        out, metadata={"gdeltforge:place_combos": json.dumps(source)})
    print(f"wrote {out}: {total.height:,} combinations, {int(total['values'].sum()):,} geo values, "
          f"{len(files) - len(failed)} files read, skipped {failed}", flush=True)


def main(argv: list[str]) -> None:
    if len(argv) == 3 and argv[0] == "combos":
        build_combos(argv[1], argv[2])
    elif len(argv) == 3 and argv[0] == "places":
        from build_place_rules import build_places  # noqa: PLC0415

        build_places(argv[1], argv[2])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
