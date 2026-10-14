"""
build_place_name_repairs.py

Maintainer tool: repairs the region names GDELT damaged in its geo
FullName strings, from NGA's GEOnet Names Server (GNS), and writes the
repairs build_place_rules.py applies to the place table's names. Users
never run it; its output is reviewed and committed under tools/.

GDELT encoded place names to Windows-1252 and read them back as Latin-1:
a letter Windows-1252 lacks became '?' ("Ḩalab" is written "?alab",
"Nghệ An" "Ngh? An"), and one it has outside Latin-1 a control character
("Prešovský" is written "Preovský"). In some Vietnamese names the
letters after a surviving accented one were lost as well ("Bình Thuận" is
written "Bì Thu?n"). Only the region part of a FullName (between the
place's own name and its country) is ever damaged, and GDELT never writes
it undamaged anywhere in the archive. A control character reads back as
its letter. The other names are GNS's own, so the repair looks the damaged
string up among the first-level administrative divisions (desig_cd ADM1,
then ADM1H) of GNS's file for the place's country.

Three subcommands:

    python tools/build_place_name_repairs.py fetch <gns dir> COUNTRY_FILE...
    python tools/build_place_name_repairs.py segments <combos.parquet> <segments.csv>
    python tools/build_place_name_repairs.py match <segments.csv> <gns dir> <out dir>

fetch downloads GNS country files (e.g. "Syria", "Saudi Arabia") into
<gns dir> and records each file's URL, download time and sha256 in
<gns dir>/sources.json.

segments lists every damaged region segment of every FullName the archive
holds (build_place_table.py's combos), with its country and how many geo
values carry it.

match decides each segment's repair, in this order, and writes
<out dir>/place_name_repairs.csv and <out dir>/place_name_sources.json:

    cp1252  a segment with control characters and no '?' reads them back
            as their Windows-1252 letters (unmangle): no source needed.
    replay  a GNS name of one division, written the way GDELT wrote names
            (replay), equals GDELT's string exactly. The repaired name is
            that GNS name.
    folded  GDELT could have written the names of exactly one division
            as its string (could_be: each '?' a letter Windows-1252 lacks,
            letters lost after a surviving accented one, accents GDELT's
            source left out, debris at the end dropped). The repaired name
            is the division's matching name, approved names (nt N) first.
    manual  a row of <out dir>/place_name_repairs_manual.csv, reviewed by
            hand, with the GNS feature it names and the reason. A manual
            row also overrides an automatic decision.

A segment no rule decides stops the run, listing its candidates, so no
damaged name passes unreviewed. Every repair records the GNS feature (UFI),
the name type and the source file, whose sha256 sources.json holds:
together they are the audit trail. NGA's suggested citation note is
CREDIT below.
"""

from __future__ import annotations

import csv
import datetime
import hashlib
import io
import json
import sys
import unicodedata
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

GNS_URL = "https://geonames.nga.mil/geonames/GNSData/{}.zip"
CREDIT = ("Toponymic information is based on the Geographic Names Database, containing "
          "official standard names approved by the United States Board on Geographic Names "
          "and maintained by the National Geospatial-Intelligence Agency. More information is "
          "available at the Resources link at www.nga.mil. The National Geospatial-Intelligence "
          "Agency name, initials, and seal are protected by 10 United States Code § Section 425.")

# GDELT's country name (the end of a FullName) -> (GNS country file, FIPS code).
# The FIPS code is the debris GDELT left at the end of some damaged names
# ("Qu?ng NgãVM").
COUNTRIES = {
    "Azerbaijan": ("Azerbaijan", "AJ"),
    "Bahrain": ("Bahrain", "BA"),
    "Cambodia": ("Cambodia", "CB"),
    "Egypt": ("Egypt", "EG"),
    "Iran": ("Iran", "IR"),
    "Iraq": ("Iraq", "IZ"),
    "Israel": ("Israel", "IS"),
    "Kuwait": ("Kuwait", "KU"),
    "Qatar": ("Qatar", "QA"),
    "Saudi Arabia": ("Saudi Arabia", "SA"),
    "Sudan": ("Sudan", "SU"),
    "Syria": ("Syria", "SY"),
    "Vietnam, Republic Of": ("Vietnam", "VM"),
    "Yemen": ("Yemen", "YM"),
}
DIVISIONS = ("ADM1", "ADM1H")  # current first-level divisions first, then historical
FIELDS = ["country", "damaged", "repaired", "tier", "gns_ufi", "gns_name_type",
          "gns_file", "evidence"]


# --- matching (pure functions, tested in tests/test_place_name_repairs.py) ----

def replay(name: str) -> str:
    """name as GDELT wrote it: encoded to Windows-1252 (letters it lacks as
    '?') and read back as Latin-1, so a letter Windows-1252 has outside
    Latin-1 (š, œ, ‘) comes out as a control character (\x9a, \x9c, \x91)."""
    return name.encode("cp1252", "replace").decode("latin-1")


def _is_control(ch: str) -> bool:
    return 0x80 <= ord(ch) <= 0x9F


def unmangle(segment: str) -> str:
    """segment with each control character read back as the Windows-1252
    letter it was ("Pre\x9aovský" -> "Prešovský"): lossless, no source
    needed."""
    return "".join(bytes([ord(c)]).decode("cp1252") if _is_control(c) else c for c in segment)


# Letters GDELT's source wrote plainly where GNS now writes a letter of
# its own: dotless i ("Bayramli", GNS "Bayramlı") and the apostrophe-like
# marks of Arabic romanization ("Ha'il", GNS "Ḩāʼil").
_PLAIN = str.maketrans({"ı": "i", "ʼ": "'", "’": "'", "‘": "'", "`": "'"})


def fold(name: str) -> str:
    """name without accents: decomposed, its combining marks dropped, and
    the letters in _PLAIN written plainly. Other letters that aren't
    accented forms (ə, Đ) stay."""
    plain = "".join(c for c in unicodedata.normalize("NFKD", name) if not unicodedata.combining(c))
    return plain.translate(_PLAIN)


def split_marker(segment: str) -> tuple[str, str]:
    """(name, marker): GDELT ends some division names with '*' or '**'
    ("Yerevan*", "Ajaria**"), undamaged ones too. The marker isn't damage:
    it is matched without and kept on the repaired name."""
    name = segment.rstrip("*")
    return name, segment[len(name):]


def strip_debris(damaged: str, fips: str) -> tuple[str, bool]:
    """(name, truncated): a trailing FIPS code of the country is debris GDELT
    left where a name was cut short ("Qu?ng NgãVM"); the name before it
    may be incomplete."""
    if fips and damaged.endswith(fips) and len(damaged) > len(fips):
        return damaged[: -len(fips)], True
    return damaged, False


def _in_cp1252(ch: str) -> bool:
    try:
        ch.encode("cp1252")
    except UnicodeEncodeError:
        return False
    return True


def could_be(damaged: str, name: str, truncated: bool = False) -> bool:
    """Whether GDELT could have written name as damaged (no marker, no
    debris), character by character:

    - '?' is one letter Windows-1252 lacks (GDELT kept every letter it
      has), possibly with one more letter lost after it; a control
      character is its Windows-1252 letter (replay);
    - a surviving non-ASCII letter (ì, ê, ã, ç) is that letter, and up to
      two letters after it may be lost (the Vietnamese damage, "Bì Thu?n"
      for "Bình Thuận"); GDELT's Ð stands for Đ the same way;
    - any other letter is the same letter, or one GDELT's source wrote
      without its accent (fold: "Riya" for "Riyā").

    truncated: the name may continue after damaged ends."""
    d, n = damaged, name
    seen: dict[tuple[int, int], bool] = {}

    def at(i: int, j: int) -> bool:
        if (i, j) in seen:
            return seen[(i, j)]
        if i == len(d):
            ok = truncated or j == len(n)
        elif j >= len(n):
            ok = False
        else:
            ch, c = d[i], n[j]
            if ch == "?":
                ok = not _in_cp1252(c) and (at(i + 1, j + 1) or at(i + 1, j + 2))
            elif _is_control(ch):
                ok = c == unmangle(ch) and at(i + 1, j + 1)
            elif ch == "Ð" or (ord(ch) > 127 and ord(ch) <= 0xFF):
                same = c in "ĐÐ" if ch == "Ð" else c.lower() == ch.lower()
                ok = same and any(at(i + 1, j + 1 + k) for k in range(3))
            else:
                ok = c.lower() == ch.lower() or fold(c).lower() == ch.lower()
                ok = ok and at(i + 1, j + 1)
        seen[(i, j)] = ok
        return ok

    return at(0, 0)


def damaged_segments(full_name: str) -> list[str]:
    """The damaged region parts of a FullName: every part after the
    place's own name and before its country (the last part, or two parts
    for a COUNTRIES name with a comma), carrying '?' or a control
    character."""
    country = country_of(full_name)
    parts = full_name.split(", ")
    body = parts[1:-2] if country and "," in country else parts[1:-1]
    return [p for p in body if is_damaged(p)]


def is_damaged(text: str) -> bool:
    return "?" in text or any(_is_control(c) for c in text)


def country_of(full_name: str) -> str | None:
    return next((c for c in COUNTRIES if full_name.endswith(", " + c)), None)


def decide(damaged: str, fips: str, divisions: dict[str, list[tuple[str, str, str]]]):
    """(tier, ufi, name, name_type, candidates) for one damaged string,
    without its marker (split_marker). divisions: {ufi: [(name, name_type,
    desig_cd), ...]}. tier is None when no rule decides; candidates then
    lists what came close."""
    for desig in DIVISIONS:
        exact = {(u, n, t) for u, names in divisions.items()
                 for n, t, d in names if d == desig and replay(n) == damaged}
        if len({u for u, _, _ in exact}) == 1:
            u, n, t = sorted(exact, key=_preference)[0]
            return "replay", u, n, t, []
    bare, truncated = strip_debris(damaged, fips)
    for desig in DIVISIONS:
        hits = {(u, n, t) for u, names in divisions.items() for n, t, d in names
                if d == desig and could_be(bare, n, truncated)}
        ufis = {u for u, _, _ in hits}
        if len(ufis) == 1:
            u, n, t = sorted(hits, key=_preference)[0]
            return "folded", u, n, t, []
        if hits:
            return None, None, None, None, sorted(hits, key=_preference)
    return None, None, None, None, []


def _preference(hit: tuple[str, str, str]):
    """Approved names first, then the shortest, then by text: deterministic."""
    u, n, t = hit
    return (t != "N", len(n), n, u)


# --- I/O ------------------------------------------------------------------------

def _gns_rows(zip_path: Path):
    with zipfile.ZipFile(zip_path) as zf:
        main = next(n for n in zf.namelist() if "/" not in n and n.endswith(".txt")
                    and n not in ("disclaimer.txt", "GNS_User_Guide.txt"))
        csv.field_size_limit(sys.maxsize if sys.maxsize < 2**31 else 2**31 - 1)
        yield from csv.DictReader(io.TextIOWrapper(zf.open(main), encoding="utf-8"), delimiter="\t")


def load_divisions(zip_path: Path) -> dict[str, list[tuple[str, str, str]]]:
    out: dict[str, list[tuple[str, str, str]]] = {}
    for row in _gns_rows(zip_path):
        if row["desig_cd"] in DIVISIONS:
            out.setdefault(row["ufi"], []).append((row["full_name"], row["nt"], row["desig_cd"]))
    return out


def fetch(gns_dir: str, countries: list[str]) -> None:
    root = Path(gns_dir)
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "sources.json"
    manifest = json.loads(manifest_path.read_text("utf-8")) if manifest_path.exists() else {}
    for name in countries:
        url = GNS_URL.format(urllib.parse.quote(name))
        target = root / f"{name}.zip"
        with urllib.request.urlopen(url, timeout=600) as resp:  # noqa: S310 (fixed https host)
            target.write_bytes(resp.read())
        manifest[target.name] = {
            "url": url,
            "downloaded": datetime.datetime.now(datetime.timezone.utc)
            .isoformat(timespec="seconds"),
            "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        }
        print(f"{target.name}: {manifest[target.name]['sha256']}")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", "utf-8",
                             newline="\n")


def segments(combos_path: str, out_csv: str) -> None:
    import polars as pl  # noqa: PLC0415 (only this subcommand needs it)

    combos = (pl.read_parquet(combos_path, columns=["FullName", "values"])
              .filter(pl.col("FullName").str.contains(r"[?\x80-\x9f]"))
              .group_by("FullName").agg(pl.col("values").sum()))
    counts: dict[tuple[str, str], int] = {}
    unknown: dict[str, int] = {}
    for full, n in combos.iter_rows():
        segs = damaged_segments(full)
        if not segs:
            continue
        country = country_of(full) or full.split(", ")[-1]
        if country not in COUNTRIES and any("?" in s for s in segs):
            unknown[full] = unknown.get(full, 0) + n
        for seg in segs:
            counts[(country, seg)] = counts.get((country, seg), 0) + n
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["country", "damaged", "values"])
        for (country, seg), n in sorted(counts.items()):
            w.writerow([country, seg, n])
    print(f"{len(counts)} damaged segments in {len({c for c, _ in counts})} countries -> {out_csv}")
    if unknown:
        top = sorted(unknown.items(), key=lambda x: -x[1])[:10]
        print(f"{len(unknown)} names with '?' in countries COUNTRIES doesn't list, e.g. {top}")


def match(segments_csv: str, gns_dir: str, out_dir: str) -> None:
    root, out = Path(gns_dir), Path(out_dir)
    manual_path = out / "place_name_repairs_manual.csv"
    manual = {}
    if manual_path.exists():
        with open(manual_path, encoding="utf-8", newline="") as f:
            manual = {(r["country"], r["damaged"]): r for r in csv.DictReader(f)}
    with open(segments_csv, encoding="utf-8", newline="") as f:
        segs = [(r["country"], r["damaged"]) for r in csv.DictReader(f)]
    # A manual row may name damage without a '?' (letters lost after a
    # surviving accented one), which segments can't detect.
    segs += sorted(k for k in manual if k not in set(segs))
    cache: dict[str, dict] = {}
    rows, undecided = [], []
    for country, damaged in segs:
        stem, fips = COUNTRIES.get(country, (country, ""))
        if "?" not in damaged and (country, damaged) not in manual:
            rows.append({"country": country, "damaged": damaged, "repaired": unmangle(damaged),
                         "tier": "cp1252", "gns_ufi": "", "gns_name_type": "", "gns_file": "",
                         "evidence": "control characters read back as Windows-1252"})
            continue
        if (country, damaged) in manual:
            m = manual[(country, damaged)]
            rows.append({**{k: m[k] for k in FIELDS if k in m}, "tier": "manual",
                         "gns_file": f"{stem}.zip"})
            continue
        if stem not in cache:
            cache[stem] = load_divisions(root / f"{stem}.zip")
        bare, marker = split_marker(damaged)
        tier, ufi, name, ntype, cands = decide(bare, fips, cache[stem])
        if tier is None or name is None:
            undecided.append((country, damaged, cands[:6]))
            continue
        evidence = ("GNS name written the way GDELT wrote names equals GDELT's"
                    if tier == "replay" else "the only GNS division GDELT could have written so")
        rows.append({"country": country, "damaged": damaged, "repaired": name + marker,
                     "tier": tier,
                     "gns_ufi": ufi, "gns_name_type": ntype, "gns_file": f"{stem}.zip",
                     "evidence": evidence})
    if undecided:
        for country, damaged, cands in undecided:
            shown = "; ".join(f"{u} {n} ({t})" for u, n, t in cands) or "no candidate"
            print(f"UNDECIDED {country} | {damaged} | {shown}")
        sys.exit(f"{len(undecided)} damaged segments need a row in {manual_path}")
    with open(out / "place_name_repairs.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerows(sorted(rows, key=lambda r: (r["country"], r["damaged"])))
    tiers: dict[str, int] = {}
    for r in rows:
        tiers[r["tier"]] = tiers.get(r["tier"], 0) + 1
    # The GNS files the repairs cite, as fetch recorded them: GNS changes
    # monthly and keeps no old versions, so the audit trail can't rely on
    # downloading them again.
    fetched = json.loads((root / "sources.json").read_text("utf-8"))
    used = sorted({r["gns_file"] for r in rows if r["gns_file"]})
    missing = [f for f in used if f not in fetched]
    if missing:
        sys.exit(f"{missing} not in {root / 'sources.json'}: fetch them with this tool")
    manifest = {"source": "NGA GEOnet Names Server (GNS)", "citation": CREDIT,
                "files": {f: fetched[f] for f in used}, "repairs": tiers}
    (out / "place_name_sources.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n", "utf-8",
        newline="\n")
    print(f"{len(rows)} repairs {tiers} -> {out / 'place_name_repairs.csv'}")


def main(argv: list[str]) -> None:
    # GNS names reach the console (UNDECIDED lines); Windows' default
    # console encoding can't print most of them.
    if isinstance(sys.stdout, io.TextIOWrapper):
        sys.stdout.reconfigure(encoding="utf-8")
    if len(argv) >= 3 and argv[0] == "fetch":
        fetch(argv[1], argv[2:])
    elif len(argv) == 3 and argv[0] == "segments":
        segments(argv[1], argv[2])
    elif len(argv) == 4 and argv[0] == "match":
        match(argv[1], argv[2], argv[3])
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
