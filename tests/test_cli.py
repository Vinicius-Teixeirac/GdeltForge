import argparse

import pandas as pd
import pytest

import gdeltforge.cli as cli


class TestRunScrapeCmd:
    def test_raises_when_downloads_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date: {
                "success": 2, "skipped": 0, "failed": ["20200101.export.CSV.zip"],
            },
        )
        args = argparse.Namespace(start_date=None, end_date=None)

        with pytest.raises(RuntimeError, match="1 failed download"):
            cli.run_scrape_cmd({}, args)

    def test_no_raise_when_nothing_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date: {"success": 5, "skipped": 0, "failed": []},
        )
        args = argparse.Namespace(start_date=None, end_date=None)

        cli.run_scrape_cmd({}, args)  # should not raise


class TestRunConvertCmd:
    def test_raises_when_conversions_failed(self, monkeypatch):
        monkeypatch.setattr(cli, "run_converter", lambda config: (["a.parquet"], ["bad.zip"]))

        with pytest.raises(RuntimeError, match="1 failed file"):
            cli.run_convert_cmd({})

    def test_no_raise_when_nothing_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_converter", lambda config: (["a.parquet", "b.parquet"], [])
        )

        cli.run_convert_cmd({})  # should not raise


class TestRunFilterCmd:
    def test_raises_when_filtering_failed(self, monkeypatch):
        monkeypatch.setattr(cli, "run_filter", lambda config: (8, 2))

        with pytest.raises(RuntimeError, match="2 failed file"):
            cli.run_filter_cmd({})

    def test_no_raise_when_nothing_failed(self, monkeypatch):
        monkeypatch.setattr(cli, "run_filter", lambda config: (10, 0))

        cli.run_filter_cmd({})  # should not raise


class TestRunSamplingCmdSource:
    """--source picks which config path the sampler reads from, without
    changing the sampling logic itself. Real sampler classes are stubbed
    out so this only checks which folder gets passed in."""

    @staticmethod
    def _config():
        return {
            "paths": {
                "filtered_data_directory": "/filtered",
                "filtered_historical_directory": "/filtered_hist",
                "parquet_data_directory": "/converted",
                "parquet_historical_directory": "/converted_hist",
            },
            "columns": {"gdelt_event": ["GlobalEventID"]},
        }

    def _run(self, tmp_path, monkeypatch, source):
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        captured = {}

        class FakeIndexedSampler:
            def __init__(self, folder_path, historical_folder, random_state, columns=None):
                captured["folder_path"] = folder_path
                captured["historical_folder"] = historical_folder
                captured["columns"] = columns

            def get_random_sample(self, n):
                return pd.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "IndexedSampler", FakeIndexedSampler)

        args = argparse.Namespace(
            mode="indexed", source=source, n=10, seed=42, out=str(tmp_path / "o.parquet"),
            columns=None,
        )
        cli.run_sampling_cmd(self._config(), args)
        return captured

    def test_default_source_is_filtered(self, tmp_path, monkeypatch):
        captured = self._run(tmp_path, monkeypatch, source="filtered")
        assert captured["folder_path"] == "/filtered"
        # historical_folder is None here since partitioning isn't enabled
        # in the test config; _historical_folder's own gating is covered
        # by exercising both source values, not by this specific path.
        assert captured["historical_folder"] is None

    def test_source_converted_uses_parquet_directory(self, tmp_path, monkeypatch):
        captured = self._run(tmp_path, monkeypatch, source="converted")
        assert captured["folder_path"] == "/converted"

    def test_columns_arg_reaches_indexed_sampler(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        captured = {}

        class FakeIndexedSampler:
            def __init__(self, folder_path, historical_folder, random_state, columns=None):
                captured["columns"] = columns

            def get_random_sample(self, n):
                return pd.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "IndexedSampler", FakeIndexedSampler)

        args = argparse.Namespace(
            mode="indexed", source="filtered", n=10, seed=42,
            out=str(tmp_path / "o.parquet"), columns=["GlobalEventID", "QuadClass"],
        )
        cli.run_sampling_cmd(self._config(), args)

        assert captured["columns"] == {"GlobalEventID", "QuadClass"}

    def test_columns_arg_reaches_daily_sampler(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        captured = {}

        class FakeDailySampler:
            def __init__(self, folder_path, historical_folder, random_state, columns=None):
                captured["columns"] = columns

            def get_daily_samples(self, samples_per_day):
                return pd.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "DailySampler", FakeDailySampler)

        args = argparse.Namespace(
            mode="daily", source="filtered", per_day=10, seed=42,
            out=str(tmp_path / "o.parquet"), columns=["GlobalEventID"],
        )
        cli.run_sampling_cmd(self._config(), args)

        assert captured["columns"] == {"GlobalEventID"}


class TestRunCodesCmd:
    def test_bare_lists_known_columns(self, capsys):
        cli.run_codes_cmd(argparse.Namespace(column=None, search=None))

        out = capsys.readouterr().out
        assert "Actor1CountryCode" in out
        assert "ActionGeo_CountryCode" in out

    def test_column_lists_its_codes(self, capsys):
        cli.run_codes_cmd(argparse.Namespace(column="ActionGeo_CountryCode", search=None))

        out = capsys.readouterr().out
        assert "US" in out
        assert "United States" in out

    def test_search_filters_to_matching_codes(self, capsys):
        cli.run_codes_cmd(
            argparse.Namespace(column="ActionGeo_CountryCode", search="korea")
        )

        out = capsys.readouterr().out
        assert "Korea, North" in out
        assert "Korea, South" in out
        assert "United States" not in out

    def test_unknown_column_raises(self):
        with pytest.raises(ValueError, match="no country-code reference list"):
            cli.run_codes_cmd(argparse.Namespace(column="NotAColumn", search=None))
