"""
The repairs of the region names GDELT's encoding damaged
(tools/build_place_name_repairs.py), and the bundled place table that
carries them. The tool isn't part of the package, so it is loaded from
its file.
"""

import csv
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

from gdeltforge.cleaning import places

TOOLS = Path(__file__).resolve().parent.parent / "tools"


def _load_tool():
    spec = importlib.util.spec_from_file_location(
        "build_place_name_repairs", TOOLS / "build_place_name_repairs.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


repairs = _load_tool()


def _repair_rows() -> list[dict]:
    with open(TOOLS / "place_name_repairs.csv", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


class TestTheDamage:
    """GDELT encoded names to Windows-1252 and read them back as Latin-1."""

    @pytest.mark.parametrize("name, written", [
        ("Ḩalab", "?alab"),  # a letter Windows-1252 lacks becomes '?'
        ("Nghệ An", "Ngh? An"),
        ("Prešovský", "Pre\x9aovský"),  # one it has outside Latin-1, a control character
        ("Stœ̆ng", "St\x9c?ng"),  # œ is in Windows-1252, its combining breve isn't
        ("Bình", "Bình"),  # Latin-1 letters survive
    ])
    def test_replay_writes_a_name_the_way_gdelt_did(self, name, written):
        assert repairs.replay(name) == written

    @pytest.mark.parametrize("written, name", [
        ("Pre\x9aovský", "Prešovský"),
        ("Ha\x91apai", "Ha‘apai"),
        ("\x8aibensko", "Šibensko"),
    ])
    def test_a_control_character_reads_back_as_its_windows_1252_letter(self, written, name):
        assert repairs.unmangle(written) == name

    @pytest.mark.parametrize("segment, expected", [
        ("Yerevan*", ("Yerevan", "*")),
        ("Ajaria**", ("Ajaria", "**")),
        ("Ha N?i", ("Ha N?i", "")),
    ])
    def test_the_star_marker_is_not_damage(self, segment, expected):
        assert repairs.split_marker(segment) == expected

    def test_a_trailing_country_code_is_debris_that_may_have_cut_the_name(self):
        assert repairs.strip_debris("Qu?ng NgãVM", "VM") == ("Qu?ng Ngã", True)
        assert repairs.strip_debris("Qu?ng Nam", "VM") == ("Qu?ng Nam", False)

    @pytest.mark.parametrize("full_name, damaged", [
        ("Aleppo, ?alab, Syria", ["?alab"]),
        ("Thara, B?c K?n, Vietnam, Republic Of", ["B?c K?n"]),  # a country with a comma
        ("Lomnica, Pre\x9aovský, Slovak Republic", ["Pre\x9aovský"]),
        ("Kinshasa, Kinshasa, Congo", []),
        ("Yerevan, Yerevan*, Armenia", []),
    ])
    def test_damaged_segments_are_the_region_parts_only(self, full_name, damaged):
        assert repairs.damaged_segments(full_name) == damaged


class TestCouldBe:
    """Whether GDELT could have written a GNS name as a damaged string."""

    @pytest.mark.parametrize("damaged, name", [
        ("?alab", "Ḩalab"),
        ("Ar Riya?", "Ar Riyāḑ"),  # GDELT's source left out the macron
        ("Bì Thu?n", "Bình Thuận"),  # letters lost after a surviving accented one
        ("H? Chíinh", "Hồ Chí Minh"),
        ("Ð N?ng", "Đà Nẵng"),  # GDELT's Ð is Đ, and lost the à after it
        ("St\x9c?ng Trê", "Stœ̆ng Trêng"),
        ("?a'il", "Ḩāʼil"),  # Arabic romanization's apostrophe
        ("?li Bayramli", "Əli Bayramli"),
    ])
    def test_it_accepts_what_the_damage_explains(self, damaged, name):
        assert repairs.could_be(damaged, name)

    @pytest.mark.parametrize("damaged, name", [
        # '?' is a letter Windows-1252 lacks: ô, a and plain letters aren't
        ("Ð N?ng", "Đắk Nông"),
        ("H?i Duong", "Hai Duong"),
        ("Q?x", "Qax"),
        ("?alab", "Halab"),
        ("Ha N?i", "Hanoi"),  # every surviving letter must agree
    ])
    def test_it_rejects_what_the_damage_cant_explain(self, damaged, name):
        assert not repairs.could_be(damaged, name)

    def test_a_cut_name_matches_as_a_prefix_only_when_cut(self):
        assert repairs.could_be("Qu?ng Ngã", "Quảng Ngãi", truncated=True)
        assert not repairs.could_be("Qu?ng Ng", "Quảng Ngãi")


class TestDecide:
    DIVISIONS = {
        "1": [("Ḩalab", "N", "ADM1"), ("Aleppo", "VA", "ADM1")],
        "2": [("Lənkəran", "N", "ADM1"), ("Lənkəran Rayonu", "N", "ADM1")],
        "3": [("Lənkəran", "N", "ADM1")],
        "4": [("Ḩamāh", "N", "ADM1")],
        "5": [("Old Ḩamāh", "N", "ADM1H")],
    }

    def test_replay_names_the_exact_gns_name(self):
        tier, ufi, name, _, _ = repairs.decide("?alab", "SY", self.DIVISIONS)
        assert (tier, ufi, name) == ("replay", "1", "Ḩalab")

    def test_folded_names_the_only_division_that_fits(self):
        tier, ufi, name, _, _ = repairs.decide("?amah", "SY", self.DIVISIONS)
        assert (tier, ufi, name) == ("folded", "4", "Ḩamāh")

    def test_two_divisions_that_fit_are_left_to_a_reviewed_row(self):
        tier, ufi, _, _, candidates = repairs.decide("L?nk?ran", "AJ", self.DIVISIONS)
        assert tier is None and ufi is None
        assert {u for u, _, _ in candidates} == {"2", "3"}

    def test_current_divisions_come_before_historical_ones(self):
        divisions = {**self.DIVISIONS, "6": [("Ḩamāh", "N", "ADM1H")]}
        assert repairs.decide("?amah", "SY", divisions)[1] == "4"


class TestTheCommittedRepairs:
    def test_every_row_is_decided_and_undamaged(self):
        rows = _repair_rows()
        assert rows
        for r in rows:
            assert r["tier"] in {"cp1252", "replay", "folded", "manual"}, r
            assert not repairs.is_damaged(r["repaired"]), r
            if r["tier"] == "cp1252":
                assert r["repaired"] == repairs.unmangle(r["damaged"]), r
            else:
                assert r["gns_ufi"] and r["gns_file"] and r["evidence"], r

    def test_every_gns_repair_keeps_gdelts_marker(self):
        for r in _repair_rows():
            assert repairs.split_marker(r["damaged"])[1] == repairs.split_marker(r["repaired"])[1]

    def test_every_cited_gns_file_is_recorded_with_its_hash(self):
        manifest = json.loads((TOOLS / "place_name_sources.json").read_text("utf-8"))
        cited = {r["gns_file"] for r in _repair_rows() if r["gns_file"]}
        assert cited == set(manifest["files"])
        for entry in manifest["files"].values():
            assert len(entry["sha256"]) == 64 and entry["url"].startswith("https://")
        assert manifest["citation"].startswith("Toponymic information is based on")


class TestTheBundledTable:
    def test_it_was_built_with_the_committed_repairs(self):
        info = places.place_table_info()["name_repairs"]
        # Line endings read as LF, as the build hashes them
        # (build_place_rules.repairs_digest): a checkout may convert them.
        text = (TOOLS / "place_name_repairs.csv").read_bytes().replace(b"\r\n", b"\n")
        digest = hashlib.sha256(text).hexdigest()
        assert info["sha256"] == digest
        assert info["names_repaired"] > 0

    def test_no_place_name_carries_encoding_damage(self):
        names = places.place_table().get_column("place_name").drop_nulls().unique()
        damaged = names.filter(names.str.contains(r"[?\x80-\x9f]"))
        assert damaged.len() == 0, damaged.head(5).to_list()

    def test_its_identity_names_rule_version_2(self):
        assert places.place_table_id().startswith("v2, ")


def test_a_name_with_no_repair_stops_the_build(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "build_place_rules", TOOLS / "build_place_rules.py"
    )
    assert spec is not None and spec.loader is not None
    rules = importlib.util.module_from_spec(spec)
    monkeypatch.syspath_prepend(str(TOOLS))
    spec.loader.exec_module(rules)
    with pytest.raises(SystemExit, match="no repair for"):
        rules._repaired("Somewhere, Unr?paired, Syria", {})
    with pytest.raises(SystemExit, match="damaged place name"):
        rules._repaired("S?mewhere, Halab, Syria", {})
    assert rules._repaired("Aleppo, ?alab, Syria", {("Syria", "?alab"): "Ḩalab"}) == (
        "Aleppo, Ḩalab, Syria"
    )
