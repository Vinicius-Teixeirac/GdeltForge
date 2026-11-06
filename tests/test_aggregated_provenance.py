"""Provenance of a sample drawn from aggregated files: each row names the
converted 15-minute file it came from and its position there, from the
record `aggregate` keeps in its output, or, for a file aggregated before
that record existed, from its .done marker and its sources on disk."""

import json
from datetime import date

import polars as pl
import pyarrow.parquet as pq
import pytest

from gdeltforge.aggregation.aggregator import GDELTAggregator
from gdeltforge.cleaning.cleaner import GDELTCleaner
from gdeltforge.sampling.samplers import FilteredSampler, IndexedSampler
from gdeltforge.utils.io import (
    AGGREGATE_MARKER_KEY,
    aggregate_sources_record,
    aggregated_marker,
    done_marker_fields,
)

# Three 15-minute files of one day, one of them empty (GDELT publishes
# some empty exports), and one of the next day.
_FILES = {
    "20200101000000.export.parquet": [1, 2, 3],
    "20200101001500.export.parquet": [],
    "20200101003000.export.parquet": [4, 5],
    "20200102000000.export.parquet": [6, 7, 8, 9],
}


def _date_parser(filename: str):
    raw = filename[:8]
    if not raw.isdigit():
        return None, None
    d = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    return d, d


def _converted(tmp_path):
    folder = tmp_path / "converted"
    folder.mkdir()
    for name, ids in _FILES.items():
        pl.DataFrame(
            {"GlobalEventID": ids, "Day": [int(name[:8])] * len(ids)},
            schema={"GlobalEventID": pl.Int64, "Day": pl.Int64},
        ).write_parquet(folder / name)
    return folder


def _aggregate(source_folder, out_folder, source="converted"):
    processed, failed = GDELTAggregator(
        str(source_folder), str(out_folder), period="day", source=source,
        date_parser=_date_parser, max_workers=1,
    ).aggregate_all_periods()
    assert (processed, failed) == (2, 0)
    return out_folder


def _assert_rows_match_their_source(sample, converted):
    """Every sampled row equals the row its provenance names."""
    assert not sample.is_empty()
    for row in sample.iter_rows(named=True):
        source = pl.read_parquet(converted / row["SourceFile"])
        assert source.row(row["SourceRow"], named=True)["GlobalEventID"] == row["GlobalEventID"]


class TestAggregateRecordsItsSources:
    def test_the_output_records_each_source_in_order_with_its_rows(self, tmp_path):
        out = _aggregate(_converted(tmp_path), tmp_path / "day")

        record = aggregated_marker(out / "20200101.parquet")

        assert record == {"sources": [
            {"file": "20200101000000.export.parquet",
             "converted": "20200101000000.export.parquet", "rows": 3},
            {"file": "20200101001500.export.parquet",
             "converted": "20200101001500.export.parquet", "rows": 0},
            {"file": "20200101003000.export.parquet",
             "converted": "20200101003000.export.parquet", "rows": 2},
        ]}

    def test_a_cleaned_source_records_the_converted_file_it_holds(self, tmp_path):
        converted = _converted(tmp_path)
        cleaned = tmp_path / "cleaned"
        GDELTCleaner(str(converted), str(cleaned), columns_to_check=[]).clean_all_files()

        record = aggregate_sources_record(sorted(cleaned.glob("*.parquet")))

        assert record[0] == {
            "file": "20200101000000.export_cleaned.parquet",
            "converted": "20200101000000.export.parquet", "rows": 3,
        }

    def test_a_cleaned_source_that_lost_rows_records_no_converted_file(self, tmp_path):
        converted = _converted(tmp_path)
        pl.DataFrame({"GlobalEventID": [1, None]}).write_parquet(
            converted / "20200103000000.export.parquet"
        )
        GDELTCleaner(
            str(converted), str(tmp_path / "cleaned"), columns_to_check=["GlobalEventID"]
        ).clean_all_files()

        record = aggregate_sources_record(
            [tmp_path / "cleaned" / "20200103000000.export_cleaned.parquet"]
        )

        assert record[0]["converted"] is None
        assert record[0]["rows"] == 1


class TestProvenanceOfAggregatedRows:
    def test_each_row_names_its_15_minute_file_and_position(self, tmp_path):
        converted = _converted(tmp_path)
        out = _aggregate(converted, tmp_path / "day")

        sample = IndexedSampler(str(out), random_state=1, provenance=True).get_random_sample(9)

        assert sorted(sample["GlobalEventID"]) == list(range(1, 10))
        _assert_rows_match_their_source(sample, converted)
        assert sample.columns[-2:] == ["SourceFile", "SourceRow"]
        assert set(sample["SourceFile"]) == {
            "20200101000000.export.parquet", "20200101003000.export.parquet",
            "20200102000000.export.parquet",
        }

    def test_the_sample_keeps_its_row_order(self, tmp_path):
        converted = _converted(tmp_path)
        out = _aggregate(converted, tmp_path / "day")

        plain = IndexedSampler(str(out), random_state=7).get_random_sample(6)
        labelled = IndexedSampler(str(out), random_state=7, provenance=True).get_random_sample(6)

        assert labelled["GlobalEventID"].to_list() == plain["GlobalEventID"].to_list()

    def test_rows_aggregated_from_cleaned_files_name_the_converted_files(self, tmp_path):
        converted = _converted(tmp_path)
        cleaned = tmp_path / "cleaned"
        GDELTCleaner(str(converted), str(cleaned), columns_to_check=[]).clean_all_files()
        out = _aggregate(cleaned, tmp_path / "day", source="cleaned")

        sample = IndexedSampler(str(out), random_state=3, provenance=True).get_random_sample(9)

        _assert_rows_match_their_source(sample, converted)

    def test_a_filtered_sample_is_labelled_the_same_way(self, tmp_path):
        converted = _converted(tmp_path)
        out = _aggregate(converted, tmp_path / "day")

        sample = FilteredSampler(
            str(out), ["GlobalEventID", "Day"], filter_dict={"Day": [20200101]},
            random_state=2, provenance=True,
        ).get_random_sample(5)

        assert sorted(sample["GlobalEventID"]) == [1, 2, 3, 4, 5]
        _assert_rows_match_their_source(sample, converted)


def _strip_record(path):
    """Rewrite an aggregated file without its record, as aggregate wrote
    it before 0.16.0; its .done marker stays."""
    table = pq.read_table(path)
    table = table.replace_schema_metadata({
        k: v for k, v in (table.schema.metadata or {}).items()
        if k != AGGREGATE_MARKER_KEY.encode()
    })
    pq.write_table(table, path)
    assert aggregated_marker(path) is None


class TestProvenanceOfFilesAggregatedBefore016:
    def test_the_sources_are_read_from_the_directory_the_marker_names(self, tmp_path):
        converted = _converted(tmp_path)
        out = _aggregate(converted, tmp_path / "day")
        for f in out.glob("*.parquet"):
            _strip_record(f)
        fields = done_marker_fields(out / "20200101.parquet")
        assert fields is not None and fields["source"] == "converted"

        sample = IndexedSampler(
            str(out), random_state=1, provenance=True,
            aggregate_sources={"converted": converted},
        ).get_random_sample(9)

        _assert_rows_match_their_source(sample, converted)

    def test_a_missing_source_stops_the_sample_naming_the_file(self, tmp_path):
        converted = _converted(tmp_path)
        out = _aggregate(converted, tmp_path / "day")
        _strip_record(out / "20200101.parquet")
        (converted / "20200101003000.export.parquet").unlink()

        sampler = IndexedSampler(
            str(out), random_state=1, provenance=True,
            aggregate_sources={"converted": converted},
        )
        with pytest.raises(ValueError, match=r"20200101\.parquet.*aggregate --force"):
            sampler.get_random_sample(9)

    def test_without_source_directories_it_stops_too(self, tmp_path):
        out = _aggregate(_converted(tmp_path), tmp_path / "day")
        _strip_record(out / "20200101.parquet")

        sampler = IndexedSampler(str(out), random_state=1, provenance=True)
        with pytest.raises(ValueError, match="no source directory given"):
            sampler.get_random_sample(9)

    def test_a_pre_012_marker_reads_its_sources_from_the_cleaned_directory(self, tmp_path):
        converted = _converted(tmp_path)
        cleaned = tmp_path / "cleaned"
        GDELTCleaner(str(converted), str(cleaned), columns_to_check=[]).clean_all_files()
        out = _aggregate(cleaned, tmp_path / "day", source="cleaned")
        for f in out.glob("*.parquet"):
            _strip_record(f)
            marker = f.parent / f".{f.name}.done"
            marker.write_text(marker.read_text().replace("source=cleaned", "source=filtered"))

        sample = IndexedSampler(
            str(out), random_state=1, provenance=True,
            aggregate_sources={"cleaned": cleaned, "converted": converted},
        ).get_random_sample(9)

        _assert_rows_match_their_source(sample, converted)


class TestAFileThatNoLongerMatchesItsRecord:
    def test_a_row_count_mismatch_stops_the_sample(self, tmp_path):
        out = _aggregate(_converted(tmp_path), tmp_path / "day")
        path = out / "20200101.parquet"
        record = aggregated_marker(path)
        assert record is not None
        record["sources"][0]["rows"] = 4
        pl.read_parquet(path).write_parquet(
            path, metadata={AGGREGATE_MARKER_KEY: json.dumps(record)}
        )

        sampler = IndexedSampler(str(out), random_state=1, provenance=True)
        with pytest.raises(ValueError, match="holds 5 rows but its sources hold 6"):
            sampler.get_random_sample(9)

    def test_a_source_that_lost_rows_in_clean_stops_the_sample(self, tmp_path):
        converted = tmp_path / "converted"
        converted.mkdir()
        pl.DataFrame({"GlobalEventID": [1, None, 3], "Day": [20200101] * 3}).write_parquet(
            converted / "20200101000000.export.parquet"
        )
        cleaned = tmp_path / "cleaned"
        GDELTCleaner(
            str(converted), str(cleaned), columns_to_check=["GlobalEventID"]
        ).clean_all_files()
        processed, _ = GDELTAggregator(
            str(cleaned), str(tmp_path / "day"), period="day", source="cleaned",
            date_parser=_date_parser, max_workers=1,
        ).aggregate_all_periods()
        assert processed == 1

        sampler = IndexedSampler(str(tmp_path / "day"), random_state=1, provenance=True)
        with pytest.raises(ValueError, match="dropped rows"):
            sampler.get_random_sample(2)
