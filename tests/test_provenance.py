"""
Row provenance: SourceFile and SourceRow name the converted file a sampled
row comes from and its position there, in every sampling mode, through
the clean stage too.
"""

import json

import polars as pl
import pytest

from gdeltforge.cleaning.cleaner import GDELTCleaner
from gdeltforge.sampling.samplers import (
    CalendarSampler,
    FilteredSampler,
    IndexedSampler,
    _keeps_positions,
)
from gdeltforge.utils.io import cleaned_marker

COLUMNS = ["GlobalEventID", "Day", "QuadClass"]
FILES = {
    # file name -> (day, QuadClass per row); ids are 1000 * file + row
    "20150301.export.parquet": (20150301, [1, 2, 1, 3, 1, 2, 4, 1]),
    "20150302.export.parquet": (20150302, [2, 2, 1, 1, 3, 1]),
}


def _write(folder):
    folder.mkdir(parents=True, exist_ok=True)
    for i, (name, (day, quads)) in enumerate(FILES.items()):
        pl.DataFrame({
            "GlobalEventID": [1000 * i + r for r in range(len(quads))],
            "Day": [day] * len(quads),
            "QuadClass": quads,
        }).write_parquet(folder / name)
    return folder


def _assert_rows_found_at_their_source(sample: pl.DataFrame, converted):
    assert sample.columns[-2:] == ["SourceFile", "SourceRow"]
    assert sample.height > 0
    for gid, file, row in sample.select("GlobalEventID", "SourceFile", "SourceRow").iter_rows():
        written = pl.read_parquet(converted / file)
        assert written["GlobalEventID"][row] == gid, (gid, file, row)


class TestEveryMode:
    def test_indexed(self, tmp_path):
        converted = _write(tmp_path / "converted")
        sample = IndexedSampler(str(converted), provenance=True).get_random_sample(10)
        _assert_rows_found_at_their_source(sample, converted)

    def test_indexed_with_replacement(self, tmp_path):
        converted = _write(tmp_path / "converted")
        sample = IndexedSampler(str(converted), provenance=True).get_random_sample(
            30, replace=True)
        _assert_rows_found_at_their_source(sample, converted)

    def test_calendar(self, tmp_path):
        converted = _write(tmp_path / "converted")
        sample = CalendarSampler(str(converted), provenance=True).get_calendar_samples(3)
        _assert_rows_found_at_their_source(sample, converted)

    @pytest.mark.parametrize("replace", [False, True])
    def test_filtered_numbers_rows_before_the_filter(self, tmp_path, replace):
        converted = _write(tmp_path / "converted")
        sampler = FilteredSampler(str(converted), COLUMNS, filter_dict={"QuadClass": [1]},
                                  provenance=True)
        sample = sampler.get_random_sample(4, replace=replace)
        _assert_rows_found_at_their_source(sample, converted)
        assert set(sample["QuadClass"]) == {1}

    def test_stratified(self, tmp_path):
        converted = _write(tmp_path / "converted")
        sampler = FilteredSampler(str(converted), COLUMNS, provenance=True)
        _assert_rows_found_at_their_source(
            sampler.get_stratified_sample("QuadClass", 2), converted)

    def test_off_by_default(self, tmp_path):
        converted = _write(tmp_path / "converted")
        assert "SourceRow" not in IndexedSampler(str(converted)).get_random_sample(5).columns
        assert "SourceRow" not in CalendarSampler(str(converted)).get_calendar_samples(2).columns
        assert "SourceRow" not in FilteredSampler(
            str(converted), COLUMNS, filter_dict={"QuadClass": [1]}).get_random_sample(2).columns


class TestThroughClean:
    def test_a_cleaned_row_names_its_converted_file_and_position(self, tmp_path):
        converted = _write(tmp_path / "converted")
        GDELTCleaner(str(converted), str(tmp_path / "cleaned"),
                     columns_to_check=[]).clean_all_files()
        sample = CalendarSampler(str(tmp_path / "cleaned"), provenance=True).get_calendar_samples(4)
        assert set(sample["SourceFile"]) <= set(FILES)
        _assert_rows_found_at_their_source(sample, converted)

    def test_the_marker_records_rows_in_and_out(self, tmp_path):
        converted = _write(tmp_path / "converted")
        GDELTCleaner(str(converted), str(tmp_path / "cleaned"),
                     columns_to_check=[]).clean_all_files()
        marker = cleaned_marker(tmp_path / "cleaned" / "20150301.export_cleaned.parquet")
        assert marker is not None
        assert (marker["rows_in"], marker["rows_out"]) == (8, 8)

    def test_a_cleaned_file_that_lost_rows_stops_the_sample(self, tmp_path):
        converted = _write(tmp_path / "converted")
        df = pl.read_parquet(converted / "20150301.export.parquet")
        df.with_columns(pl.when(pl.col("QuadClass") == 4).then(None)
                        .otherwise(pl.col("QuadClass")).alias("QuadClass")
                        ).write_parquet(converted / "20150301.export.parquet")
        GDELTCleaner(str(converted), str(tmp_path / "cleaned"),
                     columns_to_check=["QuadClass"]).clean_all_files()
        sampler = IndexedSampler(str(tmp_path / "cleaned"), provenance=True)
        with pytest.raises(ValueError, match="dropped rows"):
            sampler.get_random_sample(13)

    @pytest.mark.parametrize("marker, keeps", [
        ({"rows_in": 5, "rows_out": 5, "steps": [{"step": "require"}]}, True),
        ({"rows_in": 5, "rows_out": 4, "steps": []}, False),
        # written before the row counts: the steps decide
        ({"steps": [{"step": "errata", "rule": "date_1920"}]}, True),
        ({"steps": [{"step": "require"}]}, False),
        ({"steps": [{"step": "require", "skipped": True}]}, True),
        ({"steps": [{"step": "errata", "rule": "event_markers", "mode": "drop"}]}, False),
        ({"steps": [{"step": "errata", "rule": "event_markers", "mode": "keep"}]}, True),
    ])
    def test_whether_a_cleaned_file_keeps_its_rows_positions(self, marker, keeps):
        assert _keeps_positions(json.loads(json.dumps(marker))) is keeps
