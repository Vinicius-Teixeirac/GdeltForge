"""
The gazetteer points of countries and states (tools/build_place_point_refs.py),
and the bundled place table's canonical points chosen by them. The tool
isn't part of the package, so it is loaded from its file.
"""

import csv
import hashlib
import importlib.util
import json
from pathlib import Path

import polars as pl
import pytest

from gdeltforge.cleaning import places

TOOLS = Path(__file__).resolve().parent.parent / "tools"


def _load_tool():
    spec = importlib.util.spec_from_file_location(
        "build_place_point_refs", TOOLS / "build_place_point_refs.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


refs = _load_tool()

# (source, id, lat, lon) as the committed references hold them
MOROCCO = ("GNS", "-39830", 28.5, -10.0)
GIBRALTAR = ("GNS", "-1205054", 36.133333, -5.35)
KINGMAN = ("GNIS", "1802708", 6.4097222, -162.3669444)
NAVASSA = ("GNIS", "1393098", 18.4027778, -75.0133333)
PR_COUNTRY = ("GNIS", "1779808", 18.2156983, -66.4146668)


def _rows(name: str) -> list[dict]:
    with open(TOOLS / name, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


class TestNames:
    @pytest.mark.parametrize("name, key", [
        ("Côte d’Ivoire", "cotedivoire"),
        ("Cote D'ivoire", "cotedivoire"),
        ("Bosnia-Herzegovina", "bosniaherzegovina"),
    ])
    def test_names_compare_without_accents_case_or_punctuation(self, name, key):
        assert refs.name_key(name) == key

    def test_a_trailing_article_is_also_read_in_front(self):
        assert refs.gdelt_names("Bahamas, The") == ["Bahamas, The", "The Bahamas"]
        assert refs.gdelt_names("Vietnam, Republic Of") == ["Vietnam, Republic Of"]

    def test_a_state_is_its_own_gnis_feature(self):
        assert "State of Georgia" in refs.gnis_state_names("Georgia")
        assert "Commonwealth of Virginia" in refs.gnis_state_names("Virginia")


class TestMatchOne:
    GNS = {
        "congo": {"-2205398": ("-2205398", "Congo", "PCLI", -1.0, 15.5, "a.zip"),
                  "9": ("9", "Congo", "ADM1", -0.4, 17.0, "a.zip")},
    }
    GNIS = [
        ("1705317", "State of Georgia", "Georgia", 32.6386418, -83.4272463),
        ("1802702", "Wake Island", "U.S. Minor Outlying Islands", 19.28, 166.65),
        ("1802705", "Territory of Guam", "Guam", 13.4427595, 144.7694517),
    ]

    def test_a_country_beats_a_division_of_the_same_name(self):
        hits = refs.match_one("country:CF", "Congo", self.GNS, self.GNIS)
        assert [h[1] for h in hits] == ["-2205398"]

    def test_a_state_is_matched_in_gnis_by_its_state_name(self):
        hits = refs.match_one("state:GA", "Georgia, United States", self.GNS, self.GNIS)
        assert [h[1] for h in hits] == ["1705317"]

    def test_a_us_territory_is_matched_in_gnis(self):
        hits = refs.match_one("country:GQ", "Guam", self.GNS, self.GNIS)
        assert [(h[0], h[1]) for h in hits] == [("GNIS", "1802705")]

    def test_a_name_no_gazetteer_has_matches_nothing(self):
        assert refs.match_one("country:US", "United States", self.GNS, self.GNIS) == []


class TestWrongPoints:
    REFS = {"country:MO": MOROCCO, "country:GI": GIBRALTAR, "country:KQ": KINGMAN,
            "country:BQ": NAVASSA, "country:RQ": PR_COUNTRY,
            "state:PR": PR_COUNTRY}

    def test_km_is_the_great_circle_distance(self):
        assert refs.km((0.0, 0.0), (0.0, 1.0)) == pytest.approx(111.19, abs=0.01)

    def test_a_point_nearer_another_reference_names_it(self):
        # Navassa Island, written in the central Pacific next to Kingman Reef
        assert refs.nearer_another((6.30711, -162.686), NAVASSA[:2], self.REFS) == "country:KQ"
        assert refs.nearer_another((18.4, -75.0), NAVASSA[:2], self.REFS) is None

    def test_the_same_gazetteer_feature_is_not_another_place(self):
        # Puerto Rico is a country and a US state in GDELT, one GNIS feature
        assert refs.nearer_another((18.2359, -66.4838), PR_COUNTRY[:2], self.REFS) is None

    def test_wrong_everywhere_needs_every_point_wrong(self):
        assert refs.wrong_everywhere([(6.30711, -162.686)], NAVASSA[:2], self.REFS)
        assert not refs.wrong_everywhere([(6.30711, -162.686), (18.4, -75.0)],
                                         NAVASSA[:2], self.REFS)
        assert not refs.wrong_everywhere([], NAVASSA[:2], self.REFS)

    def test_a_large_countrys_own_point_can_lie_nearer_a_neighbour(self):
        # why a reviewed row decides a place wrong everywhere: Morocco's own
        # point, in central Morocco, is nearer Gibraltar's reference
        assert refs.wrong_everywhere([(32.0, -5.0)], MOROCCO[:2], self.REFS)


class TestTheCommittedReferences:
    def test_every_reference_is_decided_and_cited(self):
        rows = _rows("place_point_refs.csv")
        assert rows
        for r in rows:
            assert r["point"] in refs.POINTS, r
            assert r["tier"] in {"name", "manual"}, r
            if r["source"] == "none":
                assert r["tier"] == "manual" and r["evidence"], r
                continue
            assert r["source"] in {"GNS", "GNIS"}, r
            assert r["source_id"] and r["source_name"] and r["source_file"], r
            assert -90 <= float(r["lat"]) <= 90 and -180 <= float(r["lon"]) <= 180, r
            if r["point"] == "reference":
                assert r["tier"] == "manual", r

    def test_every_manual_row_is_used_and_explained(self):
        manual = _rows("place_point_refs_manual.csv")
        used = {r["feature"]: r for r in _rows("place_point_refs.csv")}
        for m in manual:
            assert m["evidence"], m
            assert used[m["feature"]]["tier"] == "manual"
            assert used[m["feature"]]["source_id"] == m["source_id"]

    def test_every_cited_file_is_recorded_with_its_hash_and_citation(self):
        manifest = json.loads((TOOLS / "place_point_sources.json").read_text("utf-8"))
        cited = {(r["source"], r["source_file"]) for r in _rows("place_point_refs.csv")
                 if r["source"] != "none"}
        recorded = {(g, f) for g, s in manifest["sources"].items() for f in s["files"]}
        assert cited == recorded
        for s in manifest["sources"].values():
            assert s["citation"]
            for entry in s["files"].values():
                assert len(entry["sha256"]) == 64 and entry["url"].startswith("https://")


class TestTheBundledTable:
    @staticmethod
    def _countries() -> pl.DataFrame:
        return (places.place_table().filter(pl.col("place_type") == 1)
                .select("place", "place_lat", "place_lon").unique())

    def test_it_was_built_with_the_committed_references(self):
        info = places.place_table_info()["point_refs"]
        text = (TOOLS / "place_point_refs.csv").read_bytes().replace(b"\r\n", b"\n")
        assert info["sha256"] == hashlib.sha256(text).hexdigest()

    def test_a_place_gdelt_never_wrote_near_itself_takes_the_gazetteer_point(self):
        countries = {p: (la, lo) for p, la, lo in self._countries().iter_rows()}
        reviewed = [r for r in _rows("place_point_refs.csv") if r["point"] == "reference"]
        assert reviewed
        for r in reviewed:
            code = r["feature"].split(":")[1]
            assert countries[code] == (float(r["lat"]), float(r["lon"])), r

    @pytest.mark.parametrize("code, point", [
        ("EU", (-22.333333, 40.366667)),  # Europa Island, written more often at (47, 8)
        ("MP", (-20.2833, 57.55)),  # Mauritius, written more often at (16, 146)
        ("LS", (47.166667, 9.533333)),  # Liechtenstein, also written at Lesotho
        ("GZ", (31.425074, 34.373398)),  # the Gaza Strip, also written in Mindanao
    ])
    def test_a_country_takes_its_own_point_over_a_more_frequent_wrong_one(self, code, point):
        countries = {p: (la, lo) for p, la, lo in self._countries().iter_rows()}
        assert countries[code] == point

    def test_its_identity_names_rule_version_3(self):
        assert places.place_table_id().startswith("v3, ")
