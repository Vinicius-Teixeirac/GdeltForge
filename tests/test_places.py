import json
import logging

import polars as pl
import pytest

from gdeltforge.cleaning import places
from gdeltforge.cleaning.cleaner import GDELTCleaner, run_cleaner
from gdeltforge.cleaning.steps import STEP_ORDER, FileContext, ResolvePlaces
from gdeltforge.utils.io import cleaned_marker

CTX = FileContext("f")
STEP = ResolvePlaces(table="test")
ROLES = ("Actor1Geo", "Actor2Geo", "ActionGeo")
NO_LOCATION = (None, None, None, None, 0)


def _points(rows, roles=ROLES, type_dtype: type[pl.DataType] = pl.Int64):
    """Events with the given geo points: rows of (FeatureID, Lat, Long,
    FullName, Type), one per event, written to every role in `roles`."""
    data: dict[str, list] = {"GlobalEventID": list(range(1, len(rows) + 1))}
    schema: dict[str, pl.DataType] = {"GlobalEventID": pl.Int64()}
    for r in roles:
        for i, (column, dtype) in enumerate((
            ("FeatureID", pl.String()), ("Lat", pl.Float64()), ("Long", pl.Float64()),
            ("FullName", pl.String()), ("Type", type_dtype()),
        )):
            data[f"{r}_{column}"] = [row[i] for row in rows]
            schema[f"{r}_{column}"] = dtype
        data[f"{r}_CountryCode"] = ["XX"] * len(rows)
        schema[f"{r}_CountryCode"] = pl.String()
    return pl.DataFrame(data, schema=schema)


def _resolved(rows, **kwargs):
    return STEP.apply(_points(rows, **kwargs).lazy(), CTX).collect()


def _point(df, role="ActionGeo", row=0):
    return tuple(df[f"{role}_{c}"][row] for c in ("FeatureID", "Lat", "Long", "FullName", "Type"))


def _place(place_id):
    """A place's (ID, point, name, type) from the bundled table."""
    row = places.place_readings().filter(pl.col("place") == place_id).row(0, named=True)
    return (row["place"], row["place_lat"], row["place_lon"], row["place_name"],
            row["place_type"])


class TestPlaceTable:
    def test_one_row_per_combination_gdelt_wrote(self):
        table = places.place_table()
        assert table.columns == [*places.KEY_COLUMNS, *places.PLACE_COLUMNS]
        assert table.select(places.KEY_COLUMNS).is_unique().all()
        assert table.height == places.place_table_info()["rows"]
        assert table.select(places.PLACE_COLUMNS).null_count().row(0) == (0,) * len(
            places.PLACE_COLUMNS
        )

    def test_a_point_without_a_location_is_keyed_by_its_id_alone(self):
        table = places.place_table()
        assert table.filter((pl.col("Lat") == 0) & (pl.col("Long") == 0)).is_empty()
        no_location = table.filter(pl.col("Lat").is_null() | pl.col("Long").is_null())
        assert no_location.select(pl.col("Lat", "Long").null_count()).row(0) == (
            no_location.height, no_location.height
        )

    def test_one_id_per_place_and_gdelt_ids_kept_where_nothing_collides(self):
        readings = places.place_readings()
        assert readings["place"].is_unique().all()
        shared = readings.filter(pl.col("place") != pl.col("FeatureID"))
        # Only the second reading of a shared ID changes form: a US state
        # takes its ADM1 code, a GNS place the gns: prefix.
        assert set(shared["gazetteer"].unique()) == {"state", "gns"}
        assert shared.filter(pl.col("gazetteer") == "state")["place"].str.contains(
            "^US[A-Z]{2}$").all()
        assert shared.filter(pl.col("gazetteer") == "gns")["place"].str.starts_with(
            "gns:").all()
        assert shared["FeatureID"].is_in(
            readings.filter(pl.col("place") == pl.col("FeatureID"))["FeatureID"].implode()
        ).all()

    def test_its_identity_names_the_rule_version_and_the_archive(self):
        info = places.place_table_info()
        assert places.place_table_id() == (
            f"v{info['version']}, {info['source']['first_file']} to "
            f"{info['source']['last_file']}"
        )


class TestResolvePlaces:
    """The cases the place-resolution proposal was built on, all real
    combinations from the full Events archive, and the rules for points
    the table doesn't know."""

    @pytest.mark.parametrize("written", [
        ("US", 39.828175, -98.5795, "United States", 1),  # the US
        ("US", 38.0, -97.0, "United States", 1),  # GDELT 1.0's round centre
        ("0", 38.0, -97.0, "United States", 1),  # "only knew the country"
    ])
    def test_the_three_forms_of_the_us_become_one_place(self, written):
        assert _point(_resolved([written])) == _place("US")
        assert _place("US")[1:3] == (39.828175, -98.5795)

    def test_an_id_two_gazetteers_share_keeps_it_for_gnis_and_prefixes_gns(self):
        df = _resolved([
            ("449676", 39.1662, -86.5264, "Indiana University, Indiana, United States", 3),
            ("449676", 34.6767, 69.0073, "Kula, Kabol, Afghanistan", 4),
        ])
        assert _point(df, row=0)[:3] == ("449676", 39.1662, -86.5264)
        assert _point(df, row=1) == ("gns:449676", 34.6767, 69.0073,
                                     "Kula, Kabol, Afghanistan", 4)

    def test_a_code_a_country_and_a_us_state_share_stays_the_countrys(self):
        california = places.place_table().filter(pl.col("place") == "USCA").row(0)
        df = _resolved([
            ("CA", 60.0, -95.0, "Canada", 1),
            (california[0], california[1], california[2], "California, United States", 2),
        ])
        assert _point(df, row=0)[0] == "CA"
        assert _point(df, row=1) == _place("USCA")
        assert _place("USCA")[3] == "California, United States"

    def test_a_country_written_at_another_countrys_point_moves_to_its_own(self):
        # Mauritius (FIPS MP) written at the Northern Mariana Islands.
        df = _resolved([("MP", 16.0, 146.0, "Mauritius", 1)])
        assert _point(df) == ("MP", -20.2833, 57.55, "Mauritius", 1)

    @pytest.mark.parametrize("lat, lon", [(0.0, 0.0), (None, None)])
    def test_a_point_without_a_location_resolves_by_its_id(self, lat, lon):
        # GA at (0, 0), or with no coordinates, is always Georgia, the US
        # state, in the archive; GG with no coordinates is Georgia, the
        # country.
        df = _resolved([("GA", lat, lon, "Georgia, United States", 2),
                        ("GG", lat, lon, None, 0)])
        assert _point(df, row=0) == _place("USGA")
        assert _point(df, row=1) == _place("GG")

    @pytest.mark.parametrize("written", [
        ("0", 0.0, 0.0, None, 0),  # the placeholder at (0, 0)
        (None, 0.0, 0.0, None, 0),
        ("0", 0.0, 0.0, "Georgia, United States", 2),  # 0 names no one place
        ("MH", None, None, "Marshall Islands, United States", 2),  # MH is Montserrat
        ("RB", None, None, None, 1),  # never written with a location
        (None, None, None, None, 0),  # GDELT's own way of writing none
    ])
    def test_a_point_without_a_location_or_a_place_is_no_location(self, written):
        df = _resolved([written])
        assert _point(df) == NO_LOCATION
        # Only the point: the country code stays as GDELT wrote it.
        assert df["ActionGeo_CountryCode"][0] == "XX"

    def test_a_new_id_at_its_own_point_is_kept_as_written(self):
        written = ("999999999", 1.23456, 2.34567, "Nowhere", 4)
        assert _point(_resolved([written])) == written

    @pytest.mark.parametrize("written, place_id", [
        (("CA", 36.1, -119.9, "California", 2), "USCA"),
        (("CA", 56.1, -106.3, "Canada", 1), "CA"),
        (("449676", 34.7, 69.1, "Kula", 4), "gns:449676"),
        (("449676", 39.2, -86.5, "Indiana University", 3), "449676"),
    ])
    def test_a_known_place_at_a_new_point_is_found_by_its_id_and_type(
        self, written, place_id
    ):
        assert _point(_resolved([written])) == _place(place_id)

    def test_an_id_another_place_uses_gets_its_own_gazetteers_form(self):
        gns = places.place_readings().filter(
            (pl.col("gazetteer") == "gns") & (pl.col("place") == pl.col("FeatureID"))
            & pl.col("FeatureID").str.contains(r"^\d+$")
        )["FeatureID"][0]
        df = _resolved([(gns, 1.0, 2.0, "A GNIS place", 3), ("GG", 1.0, 2.0, "A state", 2)])
        assert _point(df, row=0) == (f"gnis:{gns}", 1.0, 2.0, "A GNIS place", 3)
        assert _point(df, row=1) == ("USGG", 1.0, 2.0, "A state", 2)

    @pytest.mark.parametrize("written", [
        ("0", 12.3, 45.6, "Somewhere", 1),  # the placeholder names no place
        ("449676", 1.0, 2.0, "Somewhere", 0),  # untyped, and two places use it
    ])
    def test_an_id_that_names_no_one_place_becomes_null_and_the_point_stays(self, written):
        assert _point(_resolved([written])) == (None, *written[1:])

    def test_an_untyped_id_one_place_uses_is_that_place(self):
        assert _point(_resolved([("US", 40.0, -100.0, None, 0)])) == _place("US")

    def test_rows_columns_and_dtypes_are_kept(self):
        # Actor2Geo_Type is Float64 in the files up to 2007-10.
        written = _points([
            ("999999999", 1.23456, 2.34567, "Nowhere", 4),
            ("0", 38.0, -97.0, "United States", 1),
            ("0", 0.0, 0.0, None, 1),
        ] * 50, type_dtype=pl.Float64)
        out = STEP.apply(written.lazy(), CTX).collect()
        assert out.schema == written.schema
        assert out["GlobalEventID"].to_list() == written["GlobalEventID"].to_list()
        assert out["ActionGeo_Type"].to_list()[:3] == [4.0, 1.0, 0.0]

    def test_each_role_present_is_resolved(self):
        df = _resolved([("0", 38.0, -97.0, None, 1)], roles=("ActionGeo",))
        assert _point(df)[0] == "US"
        assert "Actor1Geo_FeatureID" not in df.columns

    def test_without_type_and_fullname_the_id_and_point_are_resolved(self):
        written = _points([("MP", 16.0, 146.0, "Mauritius", 1),
                           ("449676", 1.0, 2.0, None, 3)]).drop(
            [f"{r}_{c}" for r in ROLES for c in ("FullName", "Type")]
        )
        out = STEP.apply(written.lazy(), CTX).collect()
        assert out.columns == written.columns
        assert out.row(0)[1:4] == ("MP", -20.2833, 57.55)
        # Without a Type, a new point's shared ID can't be read.
        assert out.row(1)[1:4] == (None, 1.0, 2.0)

    def test_counts_every_point_of_every_role(self):
        written = _points([
            ("0", 38.0, -97.0, None, 1),  # resolved, changed
            ("US", 39.828175, -98.5795, None, 1),  # resolved, unchanged
            ("GA", 0.0, 0.0, None, 2),  # resolved by its ID, changed
            ("0", 0.0, 0.0, None, 0),  # cleared
            ("999999999", 1.23456, 2.34567, None, 4),  # unresolved
            (None, None, None, None, 0),  # already no location: not counted
        ])
        frame = STEP.count_frame(written.lazy(), CTX)
        assert frame is not None
        assert frame.collect().row(0, named=True) == {
            "places.resolved": 9, "places.changed": 6, "places.cleared": 3,
            "places.unresolved": 3,
        }

    def test_a_file_without_any_geo_point_is_counted_as_skipped(self):
        written = pl.DataFrame({"GlobalEventID": [1], "ActionGeo_FullName": ["x"]}).lazy()
        frame = STEP.count_frame(written, CTX)
        assert frame is not None
        assert frame.collect().row(0, named=True) == {"places_skipped_files": 1}
        assert STEP.apply(written, CTX).collect().equals(written.collect())

    def test_runs_after_normalize_and_before_the_null_check(self):
        assert STEP_ORDER.index("normalize") < STEP_ORDER.index("places")
        assert STEP_ORDER.index("places") < STEP_ORDER.index("require")

    def test_is_lossy_and_guarded(self):
        assert STEP.lossy is True and STEP.guarded is True


class TestPlacesInClean:
    @staticmethod
    def _write(in_dir, rows=None):
        in_dir.mkdir(parents=True, exist_ok=True)
        _points(rows or [
            ("0", 38.0, -97.0, "United States", 1),
            ("MP", 16.0, 146.0, "Mauritius", 1),
            ("0", 0.0, 0.0, None, 0),
        ]).write_parquet(in_dir / "20150301.export.parquet")

    def test_a_cleaned_file_carries_resolved_places_and_says_so(self, tmp_path):
        self._write(tmp_path / "in")
        GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
            places={"resolve": True},
        ).clean_all_files()
        out = tmp_path / "out" / "20150301.export_cleaned.parquet"
        df = pl.read_parquet(out)
        assert df["ActionGeo_FeatureID"].to_list() == ["US", "MP", None]
        marker = cleaned_marker(out)
        assert marker is not None
        assert {"step": "places", "lossy": True, "table": places.place_table_id()} in (
            marker["steps"]
        )
        audit = pl.read_parquet(next((tmp_path / "out_runs").glob("*.parquet")))
        assert audit.select(
            "places.resolved", "places.changed", "places.cleared", "places.unresolved"
        ).row(0) == (6, 6, 3, 0)

    def test_a_point_without_a_location_meets_the_null_check(self, tmp_path):
        self._write(tmp_path / "in")
        GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=["ActionGeo_Lat"],
            places={"resolve": True},
        ).clean_all_files()
        df = pl.read_parquet(tmp_path / "out" / "20150301.export_cleaned.parquet")
        assert df["GlobalEventID"].to_list() == [1, 2]

    def test_turning_it_on_or_rebuilding_the_table_cleans_again(self, tmp_path, monkeypatch):
        (tmp_path / "in").mkdir()

        def fingerprint(**kwargs):
            return GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[], **kwargs
            )._config_fingerprint

        off, on = fingerprint(), fingerprint(places={"resolve": True})
        assert off == fingerprint(places={"resolve": False}) != on
        monkeypatch.setattr(
            "gdeltforge.cleaning.cleaner.place_table_id", lambda: "v1, a to b"
        )
        assert fingerprint(places={"resolve": True}) != on

    def test_delete_source_refuses_it_unless_allowed(self, tmp_path):
        (tmp_path / "in").mkdir()
        with pytest.raises(ValueError, match="places.resolve.*turn places off"):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                places={"resolve": True}, delete_source=True,
            )
        GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
            places={"resolve": True}, delete_source=True, allow_lossy_delete_source=True,
        )

    def test_a_file_without_geo_points_is_warned_about(self, tmp_path, caplog):
        (tmp_path / "in").mkdir()
        pl.DataFrame({"GlobalEventID": [1]}).write_parquet(
            tmp_path / "in" / "20150301.export.parquet"
        )
        with caplog.at_level(logging.WARNING):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                places={"resolve": True},
            ).clean_all_files()
        assert any(
            "places didn't run on 1 file(s)" in r.message
            and "their points unresolved" in r.message
            for r in caplog.records
        )

    @staticmethod
    def _config(tmp_path, places_setting):
        from gdeltforge.utils.config import _bundled_default_dict

        cfg = _bundled_default_dict()
        (tmp_path / "in").mkdir(exist_ok=True)
        for prefix in ("", "gkg_v2_"):
            cfg["paths"][f"{prefix}parquet_data_directory"] = str(tmp_path / "in")
            cfg["paths"][f"{prefix}cleaned_data_directory"] = str(tmp_path / "out")
        cfg["clean"]["places"] = places_setting
        return cfg

    @pytest.mark.parametrize("setting, dataset, message", [
        ({"gdelt_gkg_v2": {"resolve": True}}, "gdelt_gkg_v2",
         r"clean\.places\.gdelt_gkg_v2: places are resolved in gdelt_event and "
         r"gdelt_event_15min only"),
        ({"gdelt_event": {"resolve": "yes"}}, "gdelt_event",
         r"clean\.places\.resolve must be true or false"),
        ({"gdelt_event": {"resolv": True}}, "gdelt_event",
         r"clean\.places: unknown setting\(s\) \['resolv'\]"),
        ({"gdelt_event": None}, "gdelt_event",
         r"clean\.places\.gdelt_event is empty \(null\)\. Remove the key, or write the "
         r"places settings"),
    ])
    def test_a_misapplied_setting_fails_before_any_file_is_read(
        self, tmp_path, setting, dataset, message
    ):
        with pytest.raises(ValueError, match=message):
            run_cleaner(self._config(tmp_path, setting), dataset=dataset)
        assert not (tmp_path / "out").exists()

    def test_the_setting_reaches_the_cleaner_from_the_config(self, tmp_path):
        self._write(tmp_path / "in")
        run_cleaner(
            self._config(tmp_path, {"gdelt_event": {"resolve": True}}), dataset="gdelt_event"
        )
        out = tmp_path / "out" / "20150301.export_cleaned.parquet"
        assert pl.read_parquet(out)["Actor2Geo_Lat"].to_list() == [39.828175, -20.2833, None]
        marker = cleaned_marker(out)
        assert marker is not None
        assert "places" in json.dumps(marker["steps"])
