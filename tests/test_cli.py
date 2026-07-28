import argparse

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
