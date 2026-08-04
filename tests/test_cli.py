import argparse
import sys
from pathlib import Path

import pandas as pd
import pytest

import gdeltforge.cli as cli


class TestDatasetFlag:
    def test_every_cli_choice_maps_to_a_config_key(self):
        for choice in cli._DATASET_CHOICES:
            assert choice in cli._DATASET_CLI_TO_CONFIG

    def test_config_keys_match_dataset_path_key_s_known_datasets(self):
        # The CLI's --dataset choices and dataset_path_key's prefix table
        # must stay in lockstep, or a valid CLI choice would 500 on the
        # very first paths.* lookup.
        from gdeltforge.utils.config import _DATASET_PATH_PREFIXES

        assert set(cli._DATASET_CLI_TO_CONFIG.values()) == set(_DATASET_PATH_PREFIXES)

    def test_events_maps_to_gdelt_event(self):
        assert cli._DATASET_CLI_TO_CONFIG["events"] == "gdelt_event"

    def test_convert_filter_sample_subcommands_accept_dataset_flag(self):
        parser = cli.build_parser()

        for command in ("convert", "filter", "sample"):
            extra = ["--mode", "indexed"] if command == "sample" else []
            args = parser.parse_args([command, *extra])
            assert args.dataset == "events"

            args = parser.parse_args([command, "--dataset", "gkg-v2", *extra])
            assert args.dataset == "gkg-v2"

    def test_invalid_dataset_choice_is_rejected(self):
        parser = cli.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["convert", "--dataset", "not-a-real-dataset"])


class TestRunScrapeCmd:
    def test_raises_when_downloads_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date, dataset: {
                "success": 2, "skipped": 0, "failed": ["20200101.export.CSV.zip"],
            },
        )
        args = argparse.Namespace(dataset="events", start_date=None, end_date=None)

        with pytest.raises(RuntimeError, match="1 failed download"):
            cli.run_scrape_cmd({}, args)

    def test_no_raise_when_nothing_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date, dataset: {
                "success": 5, "skipped": 0, "failed": [],
            },
        )
        args = argparse.Namespace(dataset="events", start_date=None, end_date=None)

        cli.run_scrape_cmd({}, args)  # should not raise


class TestRunConvertCmd:
    def test_raises_when_conversions_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_converter", lambda config, dataset: (["a.parquet"], ["bad.zip"])
        )

        with pytest.raises(RuntimeError, match="1 failed file"):
            cli.run_convert_cmd({})

    def test_no_raise_when_nothing_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_converter", lambda config, dataset: (["a.parquet", "b.parquet"], [])
        )

        cli.run_convert_cmd({})  # should not raise


class TestRunFilterCmd:
    def test_raises_when_filtering_failed(self, monkeypatch):
        monkeypatch.setattr(cli, "run_filter", lambda config, dataset: (8, 2))

        with pytest.raises(RuntimeError, match="2 failed file"):
            cli.run_filter_cmd({})

    def test_no_raise_when_nothing_failed(self, monkeypatch):
        monkeypatch.setattr(cli, "run_filter", lambda config, dataset: (10, 0))

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
            dataset="events", mode="indexed", source=source, n=10, seed=42,
            out=str(tmp_path / "o.parquet"), columns=None,
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
            dataset="events", mode="indexed", source="filtered", n=10, seed=42,
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
            dataset="events", mode="daily", source="filtered", per_day=10, seed=42,
            out=str(tmp_path / "o.parquet"), columns=["GlobalEventID"],
        )
        cli.run_sampling_cmd(self._config(), args)

        assert captured["columns"] == {"GlobalEventID"}


class TestRunCrossrefCmd:
    """The join logic itself (crossref_events_gkg_v1/v2) has its own
    dedicated tests in test_crossref.py; these only check that the CLI
    resolves the right config paths/columns and dispatches to the right
    function, since both join functions are stubbed out here."""

    @staticmethod
    def _config():
        return {
            "paths": {
                "gkg_v1_filtered_data_directory": "/gkg_v1_filtered",
                "gkg_v1_parquet_data_directory": "/gkg_v1_converted",
                "gkg_v1_counts_filtered_data_directory": "/gkg_v1_counts_filtered",
                "gkg_v2_filtered_data_directory": "/gkg_v2_filtered",
                "mentions_filtered_data_directory": "/mentions_filtered",
            },
            "columns": {
                "gdelt_gkg_v1": ["Date", "EventIds"],
                "gdelt_gkg_v1_counts": ["Date", "EventIds"],
                "gdelt_gkg_v2": ["V2DOCUMENTIDENTIFIER"],
            },
        }

    @staticmethod
    def _events_path(tmp_path):
        events = tmp_path / "events.parquet"
        pd.DataFrame({"GlobalEventID": [1]}).to_parquet(events)
        return str(events)

    def _args(self, tmp_path, **overrides):
        defaults = dict(
            events=self._events_path(tmp_path), gkg_version="v1", source="filtered",
            columns=None, out=str(tmp_path / "o.parquet"),
        )
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_v1_reads_gkg_v1_filtered_folder(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None: captured.update(
                folder=folder, gkg_columns=cols, columns=columns
            ) or pd.DataFrame(),
        )

        cli.run_crossref_cmd(self._config(), self._args(tmp_path, gkg_version="v1"))

        assert captured["folder"] == "/gkg_v1_filtered"
        assert captured["gkg_columns"] == ["Date", "EventIds"]

    def test_v1_counts_reads_its_own_filtered_folder(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None: captured.update(folder=folder)
            or pd.DataFrame(),
        )

        cli.run_crossref_cmd(self._config(), self._args(tmp_path, gkg_version="v1-counts"))

        assert captured["folder"] == "/gkg_v1_counts_filtered"

    def test_v2_reads_both_mentions_and_gkg_v2_filtered_folders(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v2",
            lambda events_df, mentions_folder, gkg_folder, cols, columns=None: captured.update(
                mentions_folder=mentions_folder, gkg_folder=gkg_folder
            ) or pd.DataFrame(),
        )

        cli.run_crossref_cmd(self._config(), self._args(tmp_path, gkg_version="v2"))

        assert captured["mentions_folder"] == "/mentions_filtered"
        assert captured["gkg_folder"] == "/gkg_v2_filtered"

    def test_source_converted_uses_parquet_directory(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None: captured.update(folder=folder)
            or pd.DataFrame(),
        )

        cli.run_crossref_cmd(
            self._config(), self._args(tmp_path, gkg_version="v1", source="converted")
        )

        assert captured["folder"] == "/gkg_v1_converted"

    def test_columns_arg_becomes_a_set(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None: captured.update(columns=columns)
            or pd.DataFrame(),
        )

        cli.run_crossref_cmd(
            self._config(), self._args(tmp_path, gkg_version="v1", columns=["Date"])
        )

        assert captured["columns"] == {"Date"}

    def test_output_written_via_write_parquet_atomic(self, tmp_path, monkeypatch):
        written = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(
            cli, "write_parquet_atomic",
            lambda df, out: written.update(df=df, out=out),
        )
        expected = pd.DataFrame({"GlobalEventID": [1], "GKG_Date": [20130401]})
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None: expected,
        )

        out_path = str(tmp_path / "o.parquet")
        cli.run_crossref_cmd(self._config(), self._args(tmp_path, gkg_version="v1", out=out_path))

        assert written["out"] == Path(out_path)
        assert written["df"] is expected


class TestRunCodesCmd:
    def test_bare_lists_known_columns(self, capsys):
        cli.run_codes_cmd(argparse.Namespace(column=None, search=None))

        out = capsys.readouterr().out
        assert "Actor1CountryCode" in out
        assert "ActionGeo_CountryCode" in out

    def test_bare_lists_all_seven_code_families(self, capsys):
        cli.run_codes_cmd(argparse.Namespace(column=None, search=None))

        out = capsys.readouterr().out
        assert "Actor1EthnicCode" in out
        assert "Actor1KnownGroupCode" in out
        assert "Actor1Religion1Code" in out
        assert "Actor1Type1Code" in out
        assert "EventCode" in out
        assert "CAMEO actor-country" in out
        assert "FIPS geo-country" in out
        assert "CAMEO ethnic" in out
        assert "CAMEO known-group" in out
        assert "CAMEO religion" in out
        assert "CAMEO actor-type" in out
        assert "CAMEO event" in out

    def test_column_lists_its_codes(self, capsys):
        cli.run_codes_cmd(argparse.Namespace(column="ActionGeo_CountryCode", search=None))

        out = capsys.readouterr().out
        assert "US" in out
        assert "United States" in out

    def test_column_lists_codes_for_a_newly_covered_family(self, capsys):
        cli.run_codes_cmd(argparse.Namespace(column="Actor1KnownGroupCode", search=None))

        out = capsys.readouterr().out
        assert "PLO" in out
        assert "Palestine Liberation Organization" in out

    def test_column_lists_event_codes(self, capsys):
        cli.run_codes_cmd(argparse.Namespace(column="EventRootCode", search=None))

        out = capsys.readouterr().out
        assert "01" in out
        assert "MAKE PUBLIC STATEMENT" in out

    def test_search_filters_to_matching_codes(self, capsys):
        cli.run_codes_cmd(
            argparse.Namespace(column="ActionGeo_CountryCode", search="korea")
        )

        out = capsys.readouterr().out
        assert "Korea, North" in out
        assert "Korea, South" in out
        assert "United States" not in out

    def test_unknown_column_raises(self):
        with pytest.raises(ValueError, match="no CAMEO code reference list"):
            cli.run_codes_cmd(argparse.Namespace(column="NotAColumn", search=None))


class TestMainErrorHandling:
    """main() used to let any exception propagate as a raw traceback.
    It should now print a clean one-line message to stderr and exit
    non-zero, for any command, not just the one under test here."""

    def test_exception_prints_clean_message_and_exits_1(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["gdeltforge", "convert"])
        monkeypatch.setattr(cli, "load_config", lambda path: {})

        def boom(config, dataset):
            raise RuntimeError("3 failed file(s)")

        monkeypatch.setattr(cli, "run_convert_cmd", boom)

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 1
        err = capsys.readouterr().err
        assert err.strip() == "Error: 3 failed file(s)"
        assert "Traceback" not in err

    def test_keyboard_interrupt_prints_interrupted_and_exits_130(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["gdeltforge", "convert"])
        monkeypatch.setattr(cli, "load_config", lambda path: {})

        def interrupted(config, dataset):
            raise KeyboardInterrupt

        monkeypatch.setattr(cli, "run_convert_cmd", interrupted)

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 130
        assert capsys.readouterr().err.strip() == "Interrupted."

    def test_successful_command_does_not_exit(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["gdeltforge", "convert"])
        monkeypatch.setattr(cli, "load_config", lambda path: {})
        monkeypatch.setattr(cli, "run_convert_cmd", lambda config, dataset: None)

        cli.main()  # should return normally, not raise SystemExit

    def test_codes_command_errors_are_also_handled_cleanly(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["gdeltforge", "codes", "NotAColumn"])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 1
        assert "no CAMEO code reference list" in capsys.readouterr().err
