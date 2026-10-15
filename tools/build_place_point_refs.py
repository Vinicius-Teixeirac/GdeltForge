"""
build_place_point_refs.py

Maintainer tool: gives every country and US state GDELT writes a
reference point from a gazetteer, and writes the references
build_place_rules.py picks the place table's canonical country and state
points by. Users never run it; its output is reviewed and committed under
tools/.

GDELT writes many countries at two or more points, and some of them at
another country's point: Mauritius (FIPS MP) at the Northern Mariana
Islands (ISO MP), Europa Island (FIPS EU) at Switzerland's point more often
than at its own. Which written point is the country's own can't be read
from the archive alone. The references come from the gazetteers GDELT's
own IDs come from: NGA's GEOnet Names Server (GNS) for the world, USGS's
Geographic Names Information System (GNIS) for the United States, its
states and territories, which GNS leaves out.

Two subcommands:

    python tools/build_place_point_refs.py fetch <dir>
    python tools/build_place_point_refs.py match <combos.parquet> <dir> <out dir>

fetch downloads the four files in SOURCES into <dir> and records each
file's URL, download time and sha256 in <dir>/sources.json.

match pairs each country (Type 1) and state (Type 2) feature of the
archive (build_place_table.py's combos) with one gazetteer feature, and
writes <out dir>/place_point_refs.csv and <out dir>/place_point_sources.json:

    name    GDELT's name for it (the most written) is the name of exactly
            one country-level GNS feature, or of one GNIS state or
            territory.
    manual  a row of <out dir>/place_point_refs_manual.csv, reviewed by
            hand, naming the feature and the reason: a country GNS knows
            by another name (Burma, Timor-Leste), or none (the United
            States, which no gazetteer lists as a feature). A manual row
            also overrides an automatic match.

Each reference also says how build_place_rules.py uses it ("point"):
nearest, GDELT's own point nearest the reference is the canonical point;
reference, GDELT never wrote the place near itself, so the reference's
point is. A place every written point of which lies nearer another
place's reference than its own (wrong_everywhere) gets "reference" only
from a reviewed manual row: the row decides, since a large country's own
point can lie nearer a small neighbour's reference (Morocco's is nearer
Gibraltar's). A feature no rule matches, or a place wrong everywhere
without a reviewed row, stops the run.
"""

from __future__ import annotations

import csv
import datetime
import hashlib
import io
import json
import math
import re
import sys
import unicodedata
import urllib.request
import zipfile
from pathlib import Path

# file name -> (gazetteer, URL)
SOURCES = {
    "Administrative_Regions.zip": (
        "GNS", "https://geonames.nga.mil/geonames/GNSData/fc_files/Administrative_Regions.zip"),
    "Paracel Islands.zip": (
        "GNS", "https://geonames.nga.mil/geonames/GNSData/Paracel%20Islands.zip"),
    "Spratly Islands.zip": (
        "GNS", "https://geonames.nga.mil/geonames/GNSData/Spratly%20Islands.zip"),
    "DomesticNames_National_Text.zip": (
        "GNIS", "https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/DomesticNames/"
                "DomesticNames_National_Text.zip"),
}
CITATIONS = {
    "GNS": ("Toponymic information is based on the Geographic Names Database, containing "
            "official standard names approved by the United States Board on Geographic Names "
            "and maintained by the National Geospatial-Intelligence Agency. More information "
            "is available at the Resources link at www.nga.mil. The National "
            "Geospatial-Intelligence Agency name, initials, and seal are protected by 10 "
            "United States Code § Section 425."),
    "GNIS": ("U.S. Geological Survey, National Geospatial Program, Geographic Names "
             "Information System (GNIS) Domestic Names National, published 2026-09-29. "
             "Acknowledgment of the U.S. Geological Survey would be appreciated for products "
             "derived from these data."),
}
# GNS designations of a country-level feature, in the order a name shared
# by two is read: an independent state before a dependency, before an
# island group (the Paracel and Spratly Islands), before a first-level
# division (France's overseas regions, which GDELT writes as countries).
COUNTRY_DESIGS = ("PCLI", "PCLD", "PCLIX", "PCLS", "PCLF", "PCL", "TERR", "LTER", "ISLS", "ADM1")
# GNS name types read: conventional and approved names.
NAME_TYPES = ("C", "N")
# GNIS "states" whose features are a country's in GDELT (Type 1).
US_TERRITORIES = ("American Samoa", "Commonwealth of the Northern Mariana Islands", "Guam",
                  "Puerto Rico", "U.S. Minor Outlying Islands", "United States Virgin Islands")
POINTS = ("nearest", "reference")
FIELDS = ["feature", "gdelt_name", "source", "source_id", "source_name", "lat", "lon", "point",
          "tier", "source_file", "evidence"]


# --- matching (pure functions, tested in tests/test_place_point_refs.py) -------

def name_key(name: str) -> str:
    """name compared without accents, case, spaces or punctuation."""
    plain = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z]", "", plain)


def gdelt_names(name: str) -> list[str]:
    """The forms a GDELT country name is looked up in: as written, and with
    a trailing article moved to the front ("Bahamas, The" -> "The
    Bahamas")."""
    moved = re.sub(r"^(.*), (The|the)$", r"\2 \1", name)
    return [name] if moved == name else [name, moved]


def gnis_state_names(state: str) -> set[str]:
    """GNIS's names for a state's own feature ("State of Georgia",
    "Commonwealth of Virginia", "District of Columbia")."""
    return {f"State of {state}", f"Commonwealth of {state}", state}


def km(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Great-circle distance in km."""
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = (math.sin((la2 - la1) / 2) ** 2
         + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2)
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def nearer_another(point: tuple[float, float], own: tuple[str, str],
                   refs: dict[str, tuple[str, str, float, float]]) -> str | None:
    """The feature whose reference lies nearer point than own's does, or
    None. refs: {feature: (source, source_id, lat, lon)}; own: own (source,
    source_id). A reference to the same gazetteer feature (Puerto Rico, a
    country and a state in GDELT) isn't another place's."""
    mine = next((km(point, (r[2], r[3])) for r in refs.values() if (r[0], r[1]) == own), None)
    if mine is None:
        return None
    best, best_d = None, mine
    for feature, r in refs.items():
        if (r[0], r[1]) == own:
            continue
        d = km(point, (r[2], r[3]))
        if d < best_d:
            best, best_d = feature, d
    return best


def wrong_everywhere(points: list[tuple[float, float]], own: tuple[str, str],
                     refs: dict[str, tuple[str, str, float, float]]) -> bool:
    """Whether every point GDELT wrote a place at lies nearer another
    place's reference than its own."""
    return bool(points) and all(nearer_another(p, own, refs) is not None for p in points)


# --- I/O ------------------------------------------------------------------------

def _rows(zip_path: Path, member: str, delimiter: str):
    csv.field_size_limit(sys.maxsize if sys.maxsize < 2**31 else 2**31 - 1)
    with zipfile.ZipFile(zip_path) as zf:
        text = io.TextIOWrapper(zf.open(member), encoding="utf-8-sig")
        yield from csv.DictReader(text, delimiter=delimiter, quoting=csv.QUOTE_NONE)


def load_gns(root: Path) -> tuple[dict[str, dict[str, tuple]], dict[str, tuple]]:
    """({name_key: {ufi: (ufi, name, desig_cd, lat, lon, file)}} of the
    country-level GNS features' conventional and approved names, {ufi:
    (name, lat, lon, file)} of every country-level feature, for a manual
    row's lookup)."""
    by_name: dict[str, dict[str, tuple]] = {}
    by_id: dict[str, tuple] = {}
    shown: dict[str, tuple] = {}
    for file, member in (("Administrative_Regions.zip", "Administrative_Regions.txt"),
                         ("Paracel Islands.zip", "Paracel Islands.txt"),
                         ("Spratly Islands.zip", "Spratly Islands.txt")):
        for r in _rows(root / file, member, "\t"):
            if r["desig_cd"] not in COUNTRY_DESIGS:
                continue
            la, lo = float(r["lat_dd"]), float(r["long_dd"])
            # A feature's names all share its point. A manual row's lookup
            # shows its shortest conventional name, else its shortest
            # approved one, else its shortest name in Latin script.
            rank = ({"C": 0, "N": 1}.get(r["nt"], 2 if r["nt"] in ("V", "VA", "D") else 3),
                    len(r["full_name"]), r["full_name"])
            if r["ufi"] not in shown or rank < shown[r["ufi"]]:
                shown[r["ufi"]] = rank
                by_id[r["ufi"]] = (r["full_name"], la, lo, file)
            if r["nt"] in NAME_TYPES:
                by_name.setdefault(name_key(r["full_name"]), {})[r["ufi"]] = (
                    r["ufi"], r["full_name"], r["desig_cd"], la, lo, file)
    return by_name, by_id


def load_gnis(root: Path) -> tuple[list[tuple], dict[str, tuple]]:
    """((feature_id, name, state_name, lat, lon) of every GNIS Civil
    feature of a state or territory as a whole, or of a territory's parts;
    {feature_id: (name, lat, lon, file)} of every Civil feature, for a
    manual row's lookup)."""
    file = "DomesticNames_National_Text.zip"
    matchable, by_id = [], {}
    for r in _rows(root / file, "Text/DomesticNames_National.txt", "|"):
        if r["feature_class"] != "Civil":
            continue
        state, la, lo = r["state_name"], float(r["prim_lat_dec"]), float(r["prim_long_dec"])
        by_id[r["feature_id"]] = (r["feature_name"], la, lo, file)
        if state in US_TERRITORIES or r["feature_name"] in gnis_state_names(state):
            matchable.append((r["feature_id"], r["feature_name"], state, la, lo))
    return matchable, by_id


def _gnis_territory_key(name: str) -> str:
    return name_key(re.sub(r"^(Territory of|Commonwealth of( the)?) ", "", name))


def match_one(feature: str, name: str, gns: dict, gnis: list[tuple]) -> list[tuple]:
    """Candidate (source, id, name, lat, lon, file) for one GDELT feature
    ("country:EU", "state:GA"), best designation first."""
    kind = feature.split(":")[0]
    if kind == "state":
        state = name.split(",")[0]
        return [("GNIS", f, n, la, lo, "DomesticNames_National_Text.zip")
                for f, n, s, la, lo in gnis if s == state and n in gnis_state_names(state)]
    keys = {name_key(n) for n in gdelt_names(name)}
    hits = {u: h for k in keys for u, h in gns.get(k, {}).items()}
    if hits:
        rank = min(COUNTRY_DESIGS.index(h[2]) for h in hits.values())
        return [("GNS", u, n, la, lo, file) for u, n, d, la, lo, file in hits.values()
                if COUNTRY_DESIGS.index(d) == rank]
    return [("GNIS", f, n, la, lo, "DomesticNames_National_Text.zip")
            for f, n, s, la, lo in gnis if s in US_TERRITORIES and _gnis_territory_key(n) in keys]


def fetch(out_dir: str) -> None:
    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    manifest = {}
    for name, (_, url) in SOURCES.items():
        target = root / name
        with urllib.request.urlopen(url, timeout=600) as resp:  # noqa: S310 (fixed https hosts)
            target.write_bytes(resp.read())
        manifest[name] = {
            "url": url,
            "downloaded": datetime.datetime.now(datetime.timezone.utc)
            .isoformat(timespec="seconds"),
            "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        }
        print(f"{name}: {manifest[name]['sha256']}")
    (root / "sources.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                                       "utf-8", newline="\n")


def features(combos_path: str) -> dict[str, tuple[str, list[tuple[float, float]]]]:
    """{"country:EU": (most written name, [points written])} for every
    country and state feature of the archive, (0, 0) left out."""
    import polars as pl  # noqa: PLC0415 (only match needs it)

    c = (pl.read_parquet(combos_path, columns=["FeatureID", "Type", "FullName", "Lat", "Long",
                                                "values"])
         .filter(pl.col("Type").is_in([1, 2]) & pl.col("FeatureID").str.contains(r"^[A-Z]{2}$"))
         .with_columns((pl.when(pl.col("Type") == 1).then(pl.lit("country:"))
                        .otherwise(pl.lit("state:")) + pl.col("FeatureID")).alias("feature")))
    names = (c.filter(pl.col("FullName").is_not_null())
             .group_by("feature", "FullName").agg(pl.col("values").sum())
             .sort(["feature", "values", "FullName"], descending=[False, True, False])
             .unique("feature", keep="first", maintain_order=True))
    located = c.filter(pl.col("Lat").is_not_null() & pl.col("Long").is_not_null()
                       & ~((pl.col("Lat") == 0) & (pl.col("Long") == 0)))
    points = (located.select("feature", pl.col("Lat").round(6), pl.col("Long").round(6))
              .unique().sort("feature", "Lat", "Long"))
    by_feature: dict[str, list[tuple[float, float]]] = {}
    for f, la, lo in points.iter_rows():
        by_feature.setdefault(f, []).append((la, lo))
    return {f: (n, by_feature.get(f, [])) for f, n, _ in names.iter_rows()}


def match(combos_path: str, src_dir: str, out_dir: str) -> None:
    root, out = Path(src_dir), Path(out_dir)
    manual_path = out / "place_point_refs_manual.csv"
    manual = {}
    if manual_path.exists():
        with open(manual_path, encoding="utf-8", newline="") as f:
            manual = {r["feature"]: r for r in csv.DictReader(f)}
    (gns, gns_ids), (gnis, gnis_ids) = load_gns(root), load_gnis(root)
    ids = {"GNS": gns_ids, "GNIS": gnis_ids}
    # Only features GDELT writes at a point need one; a feature written
    # only without a location (state MH, the Marshall Islands) has none.
    feats = {f: v for f, v in features(combos_path).items() if v[1]}
    rows, undecided = [], []
    for feature, (name, _) in sorted(feats.items()):
        if feature in manual:
            m = manual[feature]
            row = {"feature": feature, "gdelt_name": name, "source": m["source"],
                   "source_id": m["source_id"], "source_name": "", "lat": "", "lon": "",
                   "point": m["point"] or "nearest", "tier": "manual", "source_file": "",
                   "evidence": m["evidence"]}
            if m["source"] != "none":
                found = ids.get(m["source"], {}).get(m["source_id"])
                if found is None:
                    sys.exit(f"{manual_path}: {m['source']} {m['source_id']} ({feature}) "
                             f"isn't a country-level GNS or a Civil GNIS feature")
                sname, la, lo, file = found
                row.update(source_name=sname, lat=repr(la), lon=repr(lo), source_file=file)
            rows.append(row)
            continue
        hits = match_one(feature, name, gns, gnis)
        if len(hits) != 1:
            undecided.append((feature, name, hits))
            continue
        source, sid, sname, la, lo, file = hits[0]
        rows.append({"feature": feature, "gdelt_name": name, "source": source,
                     "source_id": sid, "source_name": sname, "lat": repr(la), "lon": repr(lo),
                     "point": "nearest", "tier": "name", "source_file": file,
                     "evidence": f"GDELT's name is the {source} feature's name"})
    for feature, name, hits in undecided:
        shown = "; ".join(f"{s} {i} {n} ({la}, {lo})" for s, i, n, la, lo, _ in hits)
        print(f"UNDECIDED {feature} | {name} | {shown or 'no candidate'}")
    refs = {r["feature"]: (r["source"], r["source_id"], float(r["lat"]), float(r["lon"]))
            for r in rows if r["source"] != "none"}
    wrong = []
    for r in rows:
        own = (r["source"], r["source_id"])
        if (r["source"] != "none" and r["feature"] not in manual
                and wrong_everywhere(feats[r["feature"]][1], own, refs)):
            wrong.append(r["feature"])
            print(f"WRONG EVERYWHERE {r['feature']} | {r['gdelt_name']} | written at "
                  f"{feats[r['feature']][1]}, reference {r['source']} {r['source_id']} "
                  f"({r['lat']}, {r['lon']})")
    bad_points = [r["feature"] for r in rows if r["point"] not in POINTS]
    if undecided or wrong or bad_points:
        sys.exit(f"{len(undecided)} unmatched, {len(wrong)} wrong everywhere and "
                 f"{len(bad_points)} with an unknown point decision: each needs a reviewed "
                 f"row in {manual_path}")
    with open(out / "place_point_refs.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    fetched = json.loads((root / "sources.json").read_text("utf-8"))
    used = sorted({r["source_file"] for r in rows if r["source_file"]})
    missing = [f for f in used if f not in fetched]
    if missing:
        sys.exit(f"{missing} not in {root / 'sources.json'}: fetch them with this tool")
    tiers: dict[str, int] = {}
    for r in rows:
        key = f"{r['tier']}, {r['point']}" if r["source"] != "none" else "no reference"
        tiers[key] = tiers.get(key, 0) + 1
    gazetteers = sorted({SOURCES[f][0] for f in used})
    manifest = {
        "sources": {g: {"citation": CITATIONS[g],
                        "files": {f: fetched[f] for f in used if SOURCES[f][0] == g}}
                    for g in gazetteers},
        "references": tiers,
    }
    (out / "place_point_sources.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n", "utf-8",
        newline="\n")
    print(f"{len(rows)} references {tiers} -> {out / 'place_point_refs.csv'}")


def main(argv: list[str]) -> None:
    # Gazetteer names reach the console; Windows' default console encoding
    # can't print most of them.
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(encoding="utf-8")
    if len(argv) == 2 and argv[0] == "fetch":
        fetch(argv[1])
    elif len(argv) == 4 and argv[0] == "match":
        match(argv[1], argv[2], argv[3])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
