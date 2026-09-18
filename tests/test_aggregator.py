from datetime import date

import polars as pl
import pytest

import gdeltforge.aggregation.aggregator as aggregator_module
from gdeltforge.aggregation.aggregator import GDELTAggregator, run_aggregator


def _write_parquet(path, data):
    pl.DataFrame(data).write_parquet(path)


def _gdeltv2_date_parser(filename: str):
    """Minimal stand-in for scraper.py's parse_gdeltv2_file_date: a
    14-digit YYYYMMDDHHMMSS prefix, the real filename shape GKG 2.1/
    Mentions/events-15min source files carry. Reimplemented here (not
    imported) so these tests don't depend on scraper.py's own parsing
    quirks, only on GDELTAggregator's grouping logic given a parser."""
    raw = filename[:14]
    if not (raw.isdigit() and len(raw) == 14):
        return None, None
    try:
        d = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        return d, d
    except ValueError:
        return None, None


class TestPeriodGrouping:
    def test_groups_15_minute_files_by_day(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        # Two days, two files each.
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})
        _write_parquet(input_dir / "20200101003000.gkg.parquet", {"GKGRECORDID": [2]})
        _write_parquet(input_dir / "20200102000000.gkg.parquet", {"GKGRECORDID": [3]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day",
            date_parser=_gdeltv2_date_parser,
        )
        processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (2, 0)
        out_dir = tmp_path / "out"
        day1 = pl.read_parquet(out_dir / "20200101.parquet")
        day2 = pl.read_parquet(out_dir / "20200102.parquet")
        assert sorted(day1["GKGRECORDID"].to_list()) == [1, 2]
        assert day2["GKGRECORDID"].to_list() == [3]

    def test_groups_by_month(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})
        _write_parquet(input_dir / "20200131000000.gkg.parquet", {"GKGRECORDID": [2]})
        _write_parquet(input_dir / "20200201000000.gkg.parquet", {"GKGRECORDID": [3]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="month",
            date_parser=_gdeltv2_date_parser,
        )
        processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (2, 0)
        out_dir = tmp_path / "out"
        month1 = pl.read_parquet(out_dir / "202001.parquet")["GKGRECORDID"].to_list()
        assert sorted(month1) == [1, 2]
        assert pl.read_parquet(out_dir / "202002.parquet")["GKGRECORDID"].to_list() == [3]

    def test_groups_by_year(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})
        _write_parquet(input_dir / "20201231000000.gkg.parquet", {"GKGRECORDID": [2]})
        _write_parquet(input_dir / "20210101000000.gkg.parquet", {"GKGRECORDID": [3]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="year",
            date_parser=_gdeltv2_date_parser,
        )
        processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (2, 0)
        out_dir = tmp_path / "out"
        assert sorted(pl.read_parquet(out_dir / "2020.parquet")["GKGRECORDID"].to_list()) == [1, 2]
        assert pl.read_parquet(out_dir / "2021.parquet")["GKGRECORDID"].to_list() == [3]

    def test_unparseable_filenames_are_skipped_not_fatal(self, tmp_path, caplog):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})
        _write_parquet(input_dir / "not_a_gdelt_filename.parquet", {"GKGRECORDID": [2]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day",
            date_parser=_gdeltv2_date_parser,
        )
        with caplog.at_level("WARNING", logger="gdeltforge.aggregation.aggregator"):
            processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (1, 0)
        assert any("unparseable filename date" in r.message for r in caplog.records)

    def test_no_files_at_all_returns_zero_zero(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), date_parser=_gdeltv2_date_parser,
        )
        assert agg.aggregate_all_periods() == (0, 0)

    def test_invalid_period_raises(self, tmp_path):
        with pytest.raises(ValueError, match="period must be one of"):
            GDELTAggregator(str(tmp_path / "in"), str(tmp_path / "out"), period="hour")


class TestSchemaReconciliation:
    def test_a_column_missing_from_some_files_comes_back_null(self, tmp_path):
        # Reuses scan_dataset_reconciled directly, the same schema-drift
        # tolerance CalendarSampler/FilteredSampler/crossref already share.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(
            input_dir / "20200101000000.gkg.parquet",
            {"GKGRECORDID": [1], "V1THEMES": ["A"]},
        )
        _write_parquet(input_dir / "20200101003000.gkg.parquet", {"GKGRECORDID": [2]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day",
            date_parser=_gdeltv2_date_parser,
        )
        processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (1, 0)
        out = pl.read_parquet(tmp_path / "out" / "20200101.parquet")
        assert out.sort("GKGRECORDID")["V1THEMES"].to_list() == ["A", None]


class TestResumability:
    def test_a_previously_aggregated_period_is_skipped_on_rerun(self, tmp_path, caplog):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day",
            date_parser=_gdeltv2_date_parser,
        )
        agg.aggregate_all_periods()

        with caplog.at_level("DEBUG", logger="gdeltforge.aggregation.aggregator"):
            processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (0, 0)
        assert any("Skipping already aggregated period" in r.message for r in caplog.records)

    def test_a_new_contributing_file_forces_reprocessing(self, tmp_path):
        # Unlike convert/filter's one-marker-per-source-file model,
        # aggregation's marker is keyed to the OUTPUT file, fingerprinted
        # partly on the sorted set of contributing source filenames: a
        # period whose source-file-set changes (a backfilled 15-minute
        # file) must be reprocessed, not served stale output forever.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day",
            date_parser=_gdeltv2_date_parser,
        )
        agg.aggregate_all_periods()

        _write_parquet(input_dir / "20200101003000.gkg.parquet", {"GKGRECORDID": [2]})
        processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (1, 0)
        out = pl.read_parquet(tmp_path / "out" / "20200101.parquet")
        assert sorted(out["GKGRECORDID"].to_list()) == [1, 2]

    def test_a_changed_source_forces_reprocessing(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day", source="filtered",
            date_parser=_gdeltv2_date_parser,
        )
        agg.aggregate_all_periods()

        agg2 = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day", source="converted",
            date_parser=_gdeltv2_date_parser,
        )
        processed, failed = agg2.aggregate_all_periods()

        assert (processed, failed) == (1, 0)

    def test_force_bypasses_the_done_marker(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day",
            date_parser=_gdeltv2_date_parser,
        )
        agg.aggregate_all_periods()

        agg_force = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day", force=True,
            date_parser=_gdeltv2_date_parser,
        )
        processed, failed = agg_force.aggregate_all_periods()

        assert (processed, failed) == (1, 0)


class TestDryRun:
    def test_dry_run_processes_nothing(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day", dry_run=True,
            date_parser=_gdeltv2_date_parser,
        )
        processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (0, 0)
        assert not (tmp_path / "out" / "20200101.parquet").exists()


class TestDeleteSource:
    def test_delete_source_removes_every_contributing_file(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        f1 = input_dir / "20200101000000.gkg.parquet"
        f2 = input_dir / "20200101003000.gkg.parquet"
        _write_parquet(f1, {"GKGRECORDID": [1]})
        _write_parquet(f2, {"GKGRECORDID": [2]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day", delete_source=True,
            date_parser=_gdeltv2_date_parser,
        )
        processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (1, 0)
        assert not f1.exists()
        assert not f2.exists()
        assert (tmp_path / "out" / "20200101.parquet").exists()

    def test_default_leaves_source_files_untouched(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        f1 = input_dir / "20200101000000.gkg.parquet"
        _write_parquet(f1, {"GKGRECORDID": [1]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day",
            date_parser=_gdeltv2_date_parser,
        )
        agg.aggregate_all_periods()

        assert f1.exists()


class TestDateRangeFiltering:
    def test_start_end_date_narrow_which_files_are_read(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})
        _write_parquet(input_dir / "20200601000000.gkg.parquet", {"GKGRECORDID": [2]})

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day",
            start_date=date(2020, 3, 1), end_date=date(2020, 12, 31),
            date_parser=_gdeltv2_date_parser,
        )
        processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (1, 0)
        assert (tmp_path / "out" / "20200601.parquet").exists()
        assert not (tmp_path / "out" / "20200101.parquet").exists()


class TestCorruptFileHandling:
    def test_one_corrupt_period_does_not_abort_the_others(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "20200101000000.gkg.parquet", {"GKGRECORDID": [1]})
        (input_dir / "20200102000000.gkg.parquet").write_bytes(b"not a real parquet file")

        agg = GDELTAggregator(
            str(input_dir), str(tmp_path / "out"), period="day", max_workers=2,
            date_parser=_gdeltv2_date_parser,
        )
        processed, failed = agg.aggregate_all_periods()

        assert (processed, failed) == (1, 1)
        assert (tmp_path / "out" / "20200101.parquet").exists()
        assert not (tmp_path / "out" / "20200102.parquet").exists()


class TestRunAggregatorDatasetEligibility:
    def test_rejects_a_non_15_minute_dataset(self):
        with pytest.raises(ValueError, match="doesn't publish at 15-minute cadence"):
            run_aggregator({"paths": {}, "aggregation": {}}, dataset="gdelt_event")

    def test_accepts_gkg_v2(self, tmp_path, monkeypatch):
        captured = {}

        class FakeAggregator:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def aggregate_all_periods(self):
                return (0, 0)

        monkeypatch.setattr(aggregator_module, "GDELTAggregator", FakeAggregator)

        config = {
            "paths": {
                "gkg_v2_filtered_data_directory": str(tmp_path / "filtered"),
                "gkg_v2_aggregated_day_data_directory": str(tmp_path / "aggregated_day"),
            },
            "aggregation": {},
        }
        run_aggregator(config, dataset="gdelt_gkg_v2", period="day", source="filtered")

        assert captured["input_folder"] == str(tmp_path / "filtered")
        assert captured["output_folder"] == str(tmp_path / "aggregated_day")
        assert captured["compression"] == "zstd"

    def test_source_converted_uses_parquet_directory(self, tmp_path, monkeypatch):
        captured = {}

        class FakeAggregator:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def aggregate_all_periods(self):
                return (0, 0)

        monkeypatch.setattr(aggregator_module, "GDELTAggregator", FakeAggregator)

        config = {
            "paths": {
                "gkg_v2_parquet_data_directory": str(tmp_path / "converted"),
                "gkg_v2_aggregated_month_data_directory": str(tmp_path / "aggregated_month"),
            },
            "aggregation": {},
        }
        run_aggregator(config, dataset="gdelt_gkg_v2", period="month", source="converted")

        assert captured["input_folder"] == str(tmp_path / "converted")
        assert captured["source"] == "converted"
