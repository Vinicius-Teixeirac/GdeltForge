import argparse
import re
import sys
from datetime import date
from pathlib import Path

import polars as pl
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

    def test_events_15min_is_a_valid_choice_mapping_to_its_own_config_key(self):
        # events and events-15min are deliberately distinct datasets
        # (different schema, different paths.* prefix), not the same
        # data at two speeds, confirm the new choice parses and maps
        # correctly end to end, same as the other five.
        from gdeltforge.utils.config import _DATASET_PATH_PREFIXES, dataset_path_key

        assert "events-15min" in cli._DATASET_CHOICES
        assert cli._DATASET_CLI_TO_CONFIG["events-15min"] == "gdelt_event_15min"
        assert _DATASET_PATH_PREFIXES["gdelt_event_15min"] == "event_15min_"
        assert dataset_path_key("gdelt_event_15min", "downloaded_data_directory") == (
            "event_15min_downloaded_data_directory"
        )

        parser = cli.build_parser()
        args = parser.parse_args(["scrape", "--dataset", "events-15min"])
        assert args.dataset == "events-15min"

    def test_scrape_convert_filter_sample_require_dataset_flag(self):
        # No default: a silent fallback to "events" is exactly the
        # footgun events-15min's existence made real: someone meaning
        # to opt into the finer, slower dataset who forgets --dataset
        # would otherwise get the daily archive instead, with no error.
        parser = cli.build_parser()

        for command in ("scrape", "convert", "filter", "sample"):
            extra = ["--mode", "indexed"] if command == "sample" else []
            with pytest.raises(SystemExit):
                parser.parse_args([command, *extra])

            args = parser.parse_args([command, "--dataset", "gkg-v2", *extra])
            assert args.dataset == "gkg-v2"

    def test_invalid_dataset_choice_is_rejected(self):
        parser = cli.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["convert", "--dataset", "not-a-real-dataset"])


class TestExportFormatFlag:
    def test_defaults_to_parquet(self):
        parser = cli.build_parser()

        args = parser.parse_args(["sample", "--dataset", "events", "--mode", "indexed"])
        assert args.export_format == "parquet"

        args = parser.parse_args(["crossref", "--events", "x.parquet", "--gkg-version", "v1"])
        assert args.export_format == "parquet"

    def test_csv_is_accepted(self):
        parser = cli.build_parser()

        args = parser.parse_args(
            ["sample", "--dataset", "events", "--mode", "indexed", "--export-format", "csv"]
        )
        assert args.export_format == "csv"

        args = parser.parse_args(
            ["crossref", "--events", "x.parquet", "--gkg-version", "v1",
             "--export-format", "csv"]
        )
        assert args.export_format == "csv"

    def test_invalid_choice_is_rejected(self):
        parser = cli.build_parser()

        with pytest.raises(SystemExit):
            parser.parse_args(
                ["sample", "--mode", "indexed", "--export-format", "feather"]
            )

        with pytest.raises(SystemExit):
            parser.parse_args(
                ["crossref", "--events", "x.parquet", "--gkg-version", "v1",
                 "--export-format", "feather"]
            )


class TestOutPathForExportFormat:
    def test_parquet_is_a_no_op_on_a_parquet_path(self):
        out = cli._out_path_for_export_format(Path("sample.parquet"), "parquet")
        assert out == Path("sample.parquet")

    def test_csv_rewrites_a_parquet_extension(self):
        out = cli._out_path_for_export_format(Path("sample.parquet"), "csv")
        assert out == Path("sample.csv")

    def test_csv_rewrites_whatever_extension_out_was_given(self):
        out = cli._out_path_for_export_format(Path("result.txt"), "csv")
        assert out == Path("result.csv")


class TestSampleDateFlags:
    def test_defaults_to_none(self):
        parser = cli.build_parser()
        args = parser.parse_args(["sample", "--dataset", "events", "--mode", "indexed"])
        assert args.start_date is None
        assert args.end_date is None

    def test_dates_are_accepted(self):
        parser = cli.build_parser()
        args = parser.parse_args(
            ["sample", "--dataset", "events", "--mode", "indexed",
             "--start-date", "2020-01-01", "--end-date", "2020-12-31"]
        )
        assert args.start_date == "2020-01-01"
        assert args.end_date == "2020-12-31"


class TestOrderFlag:
    def test_defaults_to_asc(self):
        parser = cli.build_parser()
        assert parser.parse_args(["scrape", "--dataset", "events"]).order == "asc"
        assert parser.parse_args(["convert", "--dataset", "events"]).order == "asc"
        assert parser.parse_args(["filter", "--dataset", "events"]).order == "asc"

    def test_desc_is_accepted(self):
        parser = cli.build_parser()
        assert parser.parse_args(
            ["scrape", "--dataset", "events", "--order", "desc"]
        ).order == "desc"
        assert parser.parse_args(
            ["convert", "--dataset", "events", "--order", "desc"]
        ).order == "desc"
        assert parser.parse_args(
            ["filter", "--dataset", "events", "--order", "desc"]
        ).order == "desc"

    def test_invalid_choice_is_rejected(self):
        parser = cli.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["scrape", "--order", "sideways"])
        with pytest.raises(SystemExit):
            parser.parse_args(["convert", "--order", "sideways"])
        with pytest.raises(SystemExit):
            parser.parse_args(["filter", "--order", "sideways"])

    def test_sample_and_crossref_have_no_order_flag(self):
        # --order only applies where a run has full control over its own
        # file discovery/processing order (scrape/convert/filter). sample
        # (indexed/filtered touch the whole file set regardless of order)
        # and crossref (same, full scan) deliberately don't get it.
        parser = cli.build_parser()
        assert not hasattr(
            parser.parse_args(["sample", "--dataset", "events", "--mode", "indexed"]), "order"
        )
        assert not hasattr(
            parser.parse_args(["crossref", "--events", "x.parquet", "--gkg-version", "v1"]),
            "order",
        )


class TestRunScrapeCmd:
    @staticmethod
    def _args(**overrides):
        defaults = dict(
            dataset="events", start_date=None, end_date=None, order="asc",
            verbose=False, quiet=False, force=False, dry_run=False,
        )
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_raises_when_downloads_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date, dataset, order="asc", verbose=False,
            quiet=False, force=False, dry_run=False: {
                "success": 2, "skipped": 0, "failed": ["20200101.export.CSV.zip"],
            },
        )
        args = self._args()

        with pytest.raises(RuntimeError, match="1 failed download"):
            cli.run_scrape_cmd({}, args)

    def test_no_raise_when_nothing_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date, dataset, order="asc", verbose=False,
            quiet=False, force=False, dry_run=False: {
                "success": 5, "skipped": 0, "failed": [],
            },
        )
        args = self._args()

        cli.run_scrape_cmd({}, args)  # should not raise

    def test_verbose_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date, dataset, order="asc", verbose=False,
            quiet=False, force=False, dry_run=False: (
                captured.update(verbose=verbose)
                or {"success": 0, "skipped": 0, "failed": []}
            ),
        )
        args = self._args(verbose=True)

        cli.run_scrape_cmd({}, args)

        assert captured == {"verbose": True}

    def test_quiet_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date, dataset, order="asc", verbose=False,
            quiet=False, force=False, dry_run=False: (
                captured.update(quiet=quiet)
                or {"success": 0, "skipped": 0, "failed": []}
            ),
        )
        args = self._args(quiet=True)

        cli.run_scrape_cmd({}, args)

        assert captured == {"quiet": True}

    def test_force_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date, dataset, order="asc", verbose=False,
            quiet=False, force=False, dry_run=False: (
                captured.update(force=force)
                or {"success": 0, "skipped": 0, "failed": []}
            ),
        )
        args = self._args(force=True)

        cli.run_scrape_cmd({}, args)

        assert captured == {"force": True}

    def test_order_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date, dataset, order="asc", verbose=False,
            quiet=False, force=False, dry_run=False: (
                captured.update(order=order)
                or {"success": 0, "skipped": 0, "failed": []}
            ),
        )
        args = self._args(order="desc")

        cli.run_scrape_cmd({}, args)

        assert captured == {"order": "desc"}

    def test_dry_run_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_scraping_pipeline",
            lambda config, start_date, end_date, dataset, order="asc", verbose=False,
            quiet=False, force=False, dry_run=False: (
                captured.update(dry_run=dry_run)
                or {"success": 0, "skipped": 0, "failed": []}
            ),
        )
        args = self._args(dry_run=True)

        cli.run_scrape_cmd({}, args)

        assert captured == {"dry_run": True}


class TestRunConvertCmd:
    @staticmethod
    def _args(**overrides):
        defaults = dict(
            dataset="events", start_date=None, end_date=None, order="asc",
            delete_source=False, verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False,
        )
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_raises_when_conversions_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_converter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False: (
                ["a.parquet"], ["bad.zip"]
            ),
        )
        args = self._args()

        with pytest.raises(RuntimeError, match="1 failed file"):
            cli.run_convert_cmd({}, args)

    def test_no_raise_when_nothing_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_converter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False: (
                ["a.parquet", "b.parquet"], []
            ),
        )
        args = self._args()

        cli.run_convert_cmd({}, args)  # should not raise

    def test_date_strings_are_parsed_and_passed_through(self, monkeypatch):
        captured = {}

        def fake_run_converter(
            config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False,
        ):
            captured["start_date"] = start_date
            captured["end_date"] = end_date
            return [], []

        monkeypatch.setattr(cli, "run_converter", fake_run_converter)
        args = self._args(start_date="2020-01-01", end_date="2020-12-31")

        cli.run_convert_cmd({}, args)

        assert captured == {"start_date": date(2020, 1, 1), "end_date": date(2020, 12, 31)}

    def test_invalid_date_string_raises_clearly(self):
        args = self._args(start_date="not-a-date")

        with pytest.raises(ValueError, match="Invalid date for --start-date"):
            cli.run_convert_cmd({}, args)

    def test_start_after_end_is_rejected(self):
        args = self._args(start_date="2020-12-31", end_date="2020-01-01")

        with pytest.raises(ValueError, match="must not be after"):
            cli.run_convert_cmd({}, args)

    def test_delete_source_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_converter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False: (
                captured.update(delete_source=delete_source) or ([], [])
            ),
        )
        args = self._args(delete_source=True)

        cli.run_convert_cmd({}, args)

        assert captured == {"delete_source": True}

    def test_verbose_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_converter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False: (
                captured.update(verbose=verbose) or ([], [])
            ),
        )
        args = self._args(verbose=True)

        cli.run_convert_cmd({}, args)

        assert captured == {"verbose": True}

    def test_quiet_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_converter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False: (
                captured.update(quiet=quiet) or ([], [])
            ),
        )
        args = self._args(quiet=True)

        cli.run_convert_cmd({}, args)

        assert captured == {"quiet": True}

    def test_force_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_converter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False: (
                captured.update(force=force) or ([], [])
            ),
        )
        args = self._args(force=True)

        cli.run_convert_cmd({}, args)

        assert captured == {"force": True}

    def test_order_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_converter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False: (
                captured.update(order=order) or ([], [])
            ),
        )
        args = self._args(order="desc")

        cli.run_convert_cmd({}, args)

        assert captured == {"order": "desc"}

    def test_dry_run_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_converter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False: (
                captured.update(dry_run=dry_run) or ([], [])
            ),
        )
        args = self._args(dry_run=True)

        cli.run_convert_cmd({}, args)

        assert captured == {"dry_run": True}

    def test_recover_unzipped_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_converter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
            recover_unzipped=False: (
                captured.update(recover_unzipped=recover_unzipped) or ([], [])
            ),
        )
        args = self._args(recover_unzipped=True)

        cli.run_convert_cmd({}, args)

        assert captured == {"recover_unzipped": True}


class TestRunFilterCmd:
    @staticmethod
    def _args(**overrides):
        defaults = dict(
            dataset="events", start_date=None, end_date=None, order="asc",
            delete_source=False, verbose=False, quiet=False, force=False, dry_run=False,
        )
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_raises_when_filtering_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_filter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False: (8, 2),
        )
        args = self._args()

        with pytest.raises(RuntimeError, match="2 failed file"):
            cli.run_filter_cmd({}, args)

    def test_no_raise_when_nothing_failed(self, monkeypatch):
        monkeypatch.setattr(
            cli, "run_filter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False: (10, 0),
        )
        args = self._args()

        cli.run_filter_cmd({}, args)  # should not raise

    def test_date_strings_are_parsed_and_passed_through(self, monkeypatch):
        captured = {}

        def fake_run_filter(
            config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False,
        ):
            captured["start_date"] = start_date
            captured["end_date"] = end_date
            return 0, 0

        monkeypatch.setattr(cli, "run_filter", fake_run_filter)
        args = self._args(start_date="2020-01-01", end_date="2020-12-31")

        cli.run_filter_cmd({}, args)

        assert captured == {"start_date": date(2020, 1, 1), "end_date": date(2020, 12, 31)}

    def test_start_after_end_is_rejected(self):
        args = self._args(start_date="2020-12-31", end_date="2020-01-01")

        with pytest.raises(ValueError, match="must not be after"):
            cli.run_filter_cmd({}, args)

    def test_delete_source_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_filter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False: (
                captured.update(delete_source=delete_source) or (0, 0)
            ),
        )
        args = self._args(delete_source=True)

        cli.run_filter_cmd({}, args)

        assert captured == {"delete_source": True}

    def test_verbose_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_filter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False: (
                captured.update(verbose=verbose) or (0, 0)
            ),
        )
        args = self._args(verbose=True)

        cli.run_filter_cmd({}, args)

        assert captured == {"verbose": True}

    def test_quiet_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_filter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False: (
                captured.update(quiet=quiet) or (0, 0)
            ),
        )
        args = self._args(quiet=True)

        cli.run_filter_cmd({}, args)

        assert captured == {"quiet": True}

    def test_force_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_filter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False: (
                captured.update(force=force) or (0, 0)
            ),
        )
        args = self._args(force=True)

        cli.run_filter_cmd({}, args)

        assert captured == {"force": True}

    def test_order_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_filter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False: (
                captured.update(order=order) or (0, 0)
            ),
        )
        args = self._args(order="desc")

        cli.run_filter_cmd({}, args)

        assert captured == {"order": "desc"}

    def test_dry_run_is_forwarded(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            cli, "run_filter",
            lambda config, dataset, start_date, end_date, order="asc", delete_source=False,
            verbose=False, quiet=False, force=False, dry_run=False: (
                captured.update(dry_run=dry_run) or (0, 0)
            ),
        )
        args = self._args(dry_run=True)

        cli.run_filter_cmd({}, args)

        assert captured == {"dry_run": True}


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
            def __init__(
                self, folder_path, historical_folder, random_state, columns=None,
                start_date=None, end_date=None, date_parser=None,
            ):
                captured["folder_path"] = folder_path
                captured["historical_folder"] = historical_folder
                captured["columns"] = columns

            def get_random_sample(self, n):
                return pl.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "IndexedSampler", FakeIndexedSampler)

        args = argparse.Namespace(
            dataset="events", mode="indexed", source=source, n=10, seed=42,
            out=str(tmp_path / "o.parquet"), columns=None, export_format="parquet",
            start_date=None, end_date=None,
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

    def test_explicit_null_partitioning_is_treated_as_disabled_not_a_crash(
        self, tmp_path, monkeypatch
    ):
        # converter.partitioning: (nothing typed under it yet) parses to
        # None, not {}; _historical_folder used to crash with "'NoneType'
        # object has no attribute 'get'" on part_cfg.get("enabled", False)
        # instead of falling through to "no historical folder", the same
        # as the key being absent entirely (see test_default_source_is_
        # filtered above).
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        captured = {}

        class FakeIndexedSampler:
            def __init__(
                self, folder_path, historical_folder, random_state, columns=None,
                start_date=None, end_date=None, date_parser=None,
            ):
                captured["historical_folder"] = historical_folder

            def get_random_sample(self, n):
                return pl.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "IndexedSampler", FakeIndexedSampler)

        config = self._config()
        config["converter"] = {"partitioning": None}
        args = argparse.Namespace(
            dataset="events", mode="indexed", source="filtered", n=10, seed=42,
            out=str(tmp_path / "o.parquet"), columns=None, export_format="parquet",
            start_date=None, end_date=None,
        )

        cli.run_sampling_cmd(config, args)

        assert captured["historical_folder"] is None

    def test_events_reduced_resolves_historical_folder_regardless_of_partitioning(
        self, tmp_path, monkeypatch
    ):
        # gdelt_event_reduced has no flat output mode at all (see
        # converter.py's dataset_is_always_historical), so its historical
        # directory must resolve here even with converter.partitioning
        # left unset, unlike every other dataset (confirmed below:
        # gdelt_event's own historical directory stays None under the
        # identical config).
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        captured = {}

        class FakeIndexedSampler:
            def __init__(
                self, folder_path, historical_folder, random_state, columns=None,
                start_date=None, end_date=None, date_parser=None,
            ):
                captured["historical_folder"] = historical_folder

            def get_random_sample(self, n):
                return pl.DataFrame({"Date": [19790101]})

        monkeypatch.setattr(cli, "IndexedSampler", FakeIndexedSampler)

        config = {
            "paths": {
                "event_reduced_parquet_data_directory": "/reduced_flat",
                "event_reduced_parquet_historical_directory": "/reduced_hist",
                "parquet_data_directory": "/converted",
                "parquet_historical_directory": "/converted_hist",
            },
            "columns": {"gdelt_event_reduced": ["Date"], "gdelt_event": ["GlobalEventID"]},
        }
        args = argparse.Namespace(
            dataset="events-reduced", mode="indexed", source="converted", n=10, seed=42,
            out=str(tmp_path / "o.parquet"), columns=None, export_format="parquet",
            start_date=None, end_date=None,
        )

        cli.run_sampling_cmd(config, args)
        assert captured["historical_folder"] == "/reduced_hist"

        # Same config, gdelt_event instead: no dataset_is_always_historical
        # bypass applies, so its historical directory stays unresolved.
        args.dataset = "events"
        cli.run_sampling_cmd(config, args)
        assert captured["historical_folder"] is None

    def test_columns_arg_reaches_indexed_sampler(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        captured = {}

        class FakeIndexedSampler:
            def __init__(
                self, folder_path, historical_folder, random_state, columns=None,
                start_date=None, end_date=None, date_parser=None,
            ):
                captured["columns"] = columns

            def get_random_sample(self, n):
                return pl.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "IndexedSampler", FakeIndexedSampler)

        args = argparse.Namespace(
            dataset="events", mode="indexed", source="filtered", n=10, seed=42,
            out=str(tmp_path / "o.parquet"), columns=["GlobalEventID", "QuadClass"],
            export_format="parquet", start_date=None, end_date=None,
        )
        cli.run_sampling_cmd(self._config(), args)

        assert captured["columns"] == {"GlobalEventID", "QuadClass"}

    def test_events_reduced_has_a_calendar_date_column_default(self, tmp_path, monkeypatch):
        # _CALENDAR_DATE_SPECS is keyed by every real --dataset choice;
        # events-reduced missing an entry there would KeyError the moment
        # --mode calendar/daily ran against it without an explicit
        # --date-column, rather than falling back to its real Date column
        # the way every other dataset already does.
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        captured = {}

        class FakeCalendarSampler:
            def __init__(
                self, folder_path, historical_folder, random_state, columns=None,
                date_column="Day", period="day",
                start_date=None, end_date=None, date_parser=None,
            ):
                captured["date_column"] = date_column

            def get_calendar_samples(self, samples_per_period):
                return pl.DataFrame({"Date": [19790101]})

        monkeypatch.setattr(cli, "CalendarSampler", FakeCalendarSampler)

        config = {
            "paths": {
                "event_reduced_parquet_data_directory": "/reduced_flat",
                "event_reduced_parquet_historical_directory": "/reduced_hist",
            },
            "columns": {"gdelt_event_reduced": ["Date"]},
        }
        args = argparse.Namespace(
            dataset="events-reduced", mode="calendar", source="converted",
            per_day=None, per_period=10, period=None, date_column=None, seed=42,
            out=str(tmp_path / "o.parquet"), columns=None,
            export_format="parquet", start_date=None, end_date=None,
        )

        cli.run_sampling_cmd(config, args)

        assert captured["date_column"] == "Date"

    def test_columns_arg_reaches_calendar_sampler(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        captured = {}

        class FakeCalendarSampler:
            def __init__(
                self, folder_path, historical_folder, random_state, columns=None,
                date_column="Day", period="day",
                start_date=None, end_date=None, date_parser=None,
            ):
                captured["columns"] = columns

            def get_calendar_samples(self, samples_per_period):
                return pl.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "CalendarSampler", FakeCalendarSampler)

        args = argparse.Namespace(
            dataset="events", mode="calendar", source="filtered",
            per_day=None, per_period=10, period=None, date_column=None, seed=42,
            out=str(tmp_path / "o.parquet"), columns=["GlobalEventID"],
            export_format="parquet", start_date=None, end_date=None,
        )
        cli.run_sampling_cmd(self._config(), args)

        assert captured["columns"] == {"GlobalEventID"}

    def test_export_format_csv_writes_a_real_csv_file(self, tmp_path, monkeypatch):
        # Not mocking write_parquet_atomic/write_dataframe_atomic here,
        # unlike every other test in this class: the whole point is to
        # verify the real file that lands on disk, not just that the
        # right function got called.
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)

        class FakeIndexedSampler:
            def __init__(
                self, folder_path, historical_folder, random_state, columns=None,
                start_date=None, end_date=None, date_parser=None,
            ):
                pass

            def get_random_sample(self, n):
                return pl.DataFrame({"GlobalEventID": [1, 2], "QuadClass": [1, 3]})

        monkeypatch.setattr(cli, "IndexedSampler", FakeIndexedSampler)

        args = argparse.Namespace(
            dataset="events", mode="indexed", source="filtered", n=10, seed=42,
            out=str(tmp_path / "o.parquet"), columns=None, export_format="csv",
            start_date=None, end_date=None,
        )
        cli.run_sampling_cmd(self._config(), args)

        out_csv = tmp_path / "o.csv"
        assert out_csv.exists()
        assert not (tmp_path / "o.parquet").exists()
        result = pl.read_csv(out_csv)
        assert result["GlobalEventID"].to_list() == [1, 2]
        assert result["QuadClass"].to_list() == [1, 3]


class TestRunSamplingCmdDateFiltering:
    """--start-date/--end-date reach the sampler classes, get validated the
    same way scrape/convert/filter/crossref already do, and (filtered mode
    only) log a warning when combined with --filter, since the two narrow
    the result independently rather than one replacing the other."""

    @staticmethod
    def _config():
        return {
            "paths": {
                "filtered_data_directory": "/filtered",
                "filtered_historical_directory": "/filtered_hist",
            },
            "columns": {"gdelt_event": ["GlobalEventID"]},
        }

    @staticmethod
    def _args(**overrides):
        defaults = dict(
            dataset="events", mode="indexed", source="filtered", n=10, seed=42,
            out="o.parquet", columns=None, export_format="parquet",
            filter=None, stratify=None, n_per_group=None,
            start_date=None, end_date=None,
        )
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_date_strings_are_parsed_and_passed_through(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)

        class FakeIndexedSampler:
            def __init__(
                self, folder_path, historical_folder, random_state, columns=None,
                start_date=None, end_date=None, date_parser=None,
            ):
                captured["start_date"] = start_date
                captured["end_date"] = end_date

            def get_random_sample(self, n):
                return pl.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "IndexedSampler", FakeIndexedSampler)

        cli.run_sampling_cmd(
            self._config(),
            self._args(
                out=str(tmp_path / "o.parquet"),
                start_date="2020-01-01", end_date="2020-12-31",
            ),
        )

        assert captured == {"start_date": date(2020, 1, 1), "end_date": date(2020, 12, 31)}

    def test_invalid_date_string_raises_clearly(self, tmp_path):
        with pytest.raises(ValueError, match="Invalid date for --start-date"):
            cli.run_sampling_cmd(
                self._config(),
                self._args(out=str(tmp_path / "o.parquet"), start_date="not-a-date"),
            )

    def test_start_after_end_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="must not be after"):
            cli.run_sampling_cmd(
                self._config(),
                self._args(
                    out=str(tmp_path / "o.parquet"),
                    start_date="2020-12-31", end_date="2020-01-01",
                ),
            )

    def test_warns_when_filtered_mode_combines_filter_and_dates(
        self, tmp_path, monkeypatch, caplog
    ):
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)

        class FakeFilteredSampler:
            def __init__(self, *args, **kwargs):
                pass

            def get_random_sample(self, n):
                return pl.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "FilteredSampler", FakeFilteredSampler)

        with caplog.at_level("WARNING", logger="gdeltforge.cli"):
            cli.run_sampling_cmd(
                self._config(),
                self._args(
                    mode="filtered", filter='{"QuadClass": [1]}',
                    out=str(tmp_path / "o.parquet"), start_date="2020-01-01",
                ),
            )

        assert any("both set" in r.message for r in caplog.records)

    def test_no_warning_when_only_dates_are_set(self, tmp_path, monkeypatch, caplog):
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)

        class FakeIndexedSampler:
            def __init__(self, *args, **kwargs):
                pass

            def get_random_sample(self, n):
                return pl.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "IndexedSampler", FakeIndexedSampler)

        with caplog.at_level("WARNING", logger="gdeltforge.cli"):
            cli.run_sampling_cmd(
                self._config(),
                self._args(out=str(tmp_path / "o.parquet"), start_date="2020-01-01"),
            )

        assert not any("both set" in r.message for r in caplog.records)

    def test_no_warning_when_only_filter_is_set(self, tmp_path, monkeypatch, caplog):
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)

        class FakeFilteredSampler:
            def __init__(self, *args, **kwargs):
                pass

            def get_random_sample(self, n):
                return pl.DataFrame({"GlobalEventID": [1]})

        monkeypatch.setattr(cli, "FilteredSampler", FakeFilteredSampler)

        with caplog.at_level("WARNING", logger="gdeltforge.cli"):
            cli.run_sampling_cmd(
                self._config(),
                self._args(
                    mode="filtered", filter='{"QuadClass": [1]}',
                    out=str(tmp_path / "o.parquet"),
                ),
            )

        assert not any("both set" in r.message for r in caplog.records)


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
        pl.DataFrame({"GlobalEventID": [1]}).write_parquet(events)
        return str(events)

    def _args(self, tmp_path, **overrides):
        defaults = dict(
            events=self._events_path(tmp_path), gkg_version="v1", source="filtered",
            columns=None, out=str(tmp_path / "o.parquet"), export_format="parquet",
            on_duplicate_document="all", collapse_duplicate_mentions=False,
            start_date=None, end_date=None,
        )
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_v1_reads_gkg_v1_filtered_folder(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None, start_date=None,
            end_date=None: captured.update(
                folder=folder, gkg_columns=cols, columns=columns
            ) or pl.DataFrame(),
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
            lambda events_df, folder, cols, columns=None, start_date=None,
            end_date=None: captured.update(folder=folder)
            or pl.DataFrame(),
        )

        cli.run_crossref_cmd(self._config(), self._args(tmp_path, gkg_version="v1-counts"))

        assert captured["folder"] == "/gkg_v1_counts_filtered"

    def test_v2_reads_both_mentions_and_gkg_v2_filtered_folders(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v2",
            lambda events_df, mentions_folder, gkg_folder, cols, columns=None,
            on_duplicate_document="all", dedupe_mentions=False, start_date=None,
            end_date=None: captured.update(
                mentions_folder=mentions_folder, gkg_folder=gkg_folder
            ) or pl.DataFrame(),
        )

        cli.run_crossref_cmd(self._config(), self._args(tmp_path, gkg_version="v2"))

        assert captured["mentions_folder"] == "/mentions_filtered"
        assert captured["gkg_folder"] == "/gkg_v2_filtered"

    def test_v2_forwards_on_duplicate_document_and_dedupe_mentions(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v2",
            lambda events_df, mentions_folder, gkg_folder, cols, columns=None,
            on_duplicate_document="all", dedupe_mentions=False, start_date=None,
            end_date=None: captured.update(
                on_duplicate_document=on_duplicate_document, dedupe_mentions=dedupe_mentions
            ) or pl.DataFrame(),
        )

        cli.run_crossref_cmd(
            self._config(),
            self._args(
                tmp_path, gkg_version="v2",
                on_duplicate_document="latest", collapse_duplicate_mentions=True,
            ),
        )

        assert captured["on_duplicate_document"] == "latest"
        assert captured["dedupe_mentions"] is True

    def test_auto_reads_gkg_v1_mentions_and_gkg_v2_folders_all_three(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_auto",
            lambda events_df, gkg_v1_folder, gkg_v1_cols, mentions_folder, gkg_v2_folder,
            gkg_v2_cols, on_duplicate_document="all", dedupe_mentions=False,
            start_date=None, end_date=None: captured.update(
                gkg_v1_folder=gkg_v1_folder, mentions_folder=mentions_folder,
                gkg_v2_folder=gkg_v2_folder,
            ) or pl.DataFrame(),
        )

        cli.run_crossref_cmd(self._config(), self._args(tmp_path, gkg_version="auto"))

        assert captured["gkg_v1_folder"] == "/gkg_v1_filtered"
        assert captured["mentions_folder"] == "/mentions_filtered"
        assert captured["gkg_v2_folder"] == "/gkg_v2_filtered"

    def test_auto_with_columns_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        with pytest.raises(ValueError, match="--columns isn't supported with --gkg-version auto"):
            cli.run_crossref_cmd(
                self._config(),
                self._args(tmp_path, gkg_version="auto", columns=["Themes"]),
            )

    def test_source_converted_uses_parquet_directory(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None, start_date=None,
            end_date=None: captured.update(folder=folder)
            or pl.DataFrame(),
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
            lambda events_df, folder, cols, columns=None, start_date=None,
            end_date=None: captured.update(columns=columns)
            or pl.DataFrame(),
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
        expected = pl.DataFrame({"GlobalEventID": [1], "GKG_Date": [20130401]})
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None, start_date=None,
            end_date=None: expected,
        )

        out_path = str(tmp_path / "o.parquet")
        cli.run_crossref_cmd(self._config(), self._args(tmp_path, gkg_version="v1", out=out_path))

        assert written["out"] == Path(out_path)
        assert written["df"] is expected

    def test_export_format_csv_writes_a_real_csv_file(self, tmp_path, monkeypatch):
        # Not mocking write_parquet_atomic/write_dataframe_atomic here,
        # unlike every other test in this class: the whole point is to
        # verify the real file that lands on disk, not just that the
        # right function got called.
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        expected = pl.DataFrame({"GlobalEventID": [1], "GKG_Date": [20130401]})
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None, start_date=None,
            end_date=None: expected,
        )

        cli.run_crossref_cmd(
            self._config(),
            self._args(tmp_path, gkg_version="v1", export_format="csv"),
        )

        out_csv = tmp_path / "o.csv"
        assert out_csv.exists()
        assert not (tmp_path / "o.parquet").exists()
        result = pl.read_csv(out_csv)
        assert result["GlobalEventID"].to_list() == [1]
        assert result["GKG_Date"].to_list() == [20130401]

    def test_date_strings_are_parsed_and_passed_through(self, tmp_path, monkeypatch):
        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None, start_date=None,
            end_date=None: captured.update(
                start_date=start_date, end_date=end_date
            ) or pl.DataFrame(),
        )

        cli.run_crossref_cmd(
            self._config(),
            self._args(tmp_path, gkg_version="v1", start_date="2020-01-01", end_date="2020-12-31"),
        )

        assert captured == {"start_date": date(2020, 1, 1), "end_date": date(2020, 12, 31)}

    def test_invalid_date_string_raises_clearly(self, tmp_path):
        with pytest.raises(ValueError, match="Invalid date for --start-date"):
            cli.run_crossref_cmd(
                self._config(), self._args(tmp_path, gkg_version="v1", start_date="not-a-date"),
            )

    def test_start_after_end_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="must not be after"):
            cli.run_crossref_cmd(
                self._config(),
                self._args(
                    tmp_path, gkg_version="v1",
                    start_date="2020-12-31", end_date="2020-01-01",
                ),
            )

    def test_events_pointed_at_a_directory_with_a_done_marker_works(self, tmp_path, monkeypatch):
        # Real scenario: --events pointed directly at convert/filter
        # output (skipping sample) instead of a single sample.parquet
        # file. Those directories always carry .done resumability
        # markers as real siblings of the data; this must read the real
        # files and ignore the marker, not crash trying to parse it as
        # parquet.
        events_dir = tmp_path / "events"
        events_dir.mkdir()
        pl.DataFrame({"GlobalEventID": [1001]}).write_parquet(
            events_dir / "20260811.export.parquet"
        )
        (events_dir / "20260811.export.parquet.done").write_text("some-fingerprint")

        captured = {}
        monkeypatch.setattr(cli, "ensure_exists", lambda path, desc: path)
        monkeypatch.setattr(cli, "write_parquet_atomic", lambda df, out: None)
        monkeypatch.setattr(
            cli, "crossref_events_gkg_v1",
            lambda events_df, folder, cols, columns=None, start_date=None,
            end_date=None: captured.update(
                n_events=len(events_df), ids=events_df["GlobalEventID"].to_list()
            ) or pl.DataFrame(),
        )

        cli.run_crossref_cmd(
            self._config(), self._args(tmp_path, gkg_version="v1", events=str(events_dir)),
        )

        assert captured == {"n_events": 1, "ids": [1001]}


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
        monkeypatch.setattr(sys, "argv", ["gdeltforge", "convert", "--dataset", "events"])
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
        monkeypatch.setattr(sys, "argv", ["gdeltforge", "convert", "--dataset", "events"])
        monkeypatch.setattr(cli, "load_config", lambda path: {})

        def interrupted(config, dataset):
            raise KeyboardInterrupt

        monkeypatch.setattr(cli, "run_convert_cmd", interrupted)

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 130
        assert capsys.readouterr().err.strip() == "Interrupted."

    def test_successful_command_does_not_exit(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["gdeltforge", "convert", "--dataset", "events"])
        monkeypatch.setattr(cli, "load_config", lambda path: {})
        monkeypatch.setattr(cli, "run_convert_cmd", lambda config, dataset: None)

        cli.main()  # should return normally, not raise SystemExit

    def test_codes_command_errors_are_also_handled_cleanly(self, monkeypatch, capsys):
        monkeypatch.setattr(sys, "argv", ["gdeltforge", "codes", "NotAColumn"])

        with pytest.raises(SystemExit) as exc_info:
            cli.main()

        assert exc_info.value.code == 1
        assert "no CAMEO code reference list" in capsys.readouterr().err


class TestCliReferenceDocsSync:
    """
    docs/cli-reference.md is hand-maintained, not generated from argparse,
    so it can silently drift from --help. That's exactly what happened
    with crossref's --on-duplicate-document/--keep-duplicate-mentions:
    both landed in argparse and CHANGELOG.md but not in this file, and
    nothing caught it. These tests read the real parser and the real doc
    file and cross-check them in both directions, so a new, renamed, or
    removed flag or subcommand fails the suite instead of drifting
    silently again.
    """

    DOCS_PATH = Path(__file__).resolve().parent.parent / "docs" / "cli-reference.md"

    @classmethod
    def _docs_text(cls):
        return cls.DOCS_PATH.read_text(encoding="utf-8")

    @classmethod
    def _section(cls, text, heading):
        # Text between "## <heading>" and the next "## " heading (or end
        # of file), so one subcommand's table can't be credited with
        # another's flags.
        pattern = re.compile(
            rf"^## {re.escape(heading)}\s*$(.*?)(?=^## |\Z)", re.MULTILINE | re.DOTALL
        )
        match = pattern.search(text)
        assert match, f"No '## {heading}' section found in {cls.DOCS_PATH.name}"
        return match.group(1)

    @staticmethod
    def _documented_tokens(section_text):
        # Every markdown table row whose first cell is a code span, e.g.
        # "| `--on-duplicate-document {latest,earliest,all}` | ... |" or
        # "| `column` | Positional, optional. ... |". Deliberately only
        # table rows, not any backtick-wrapped text in prose: several
        # sections reference another command's flag in passing (e.g.
        # crossref's prose mentioning `--gkg-version`'s own modes), and
        # counting those would hide a real missing row behind an
        # incidental prose mention.
        tokens = {}
        for line in section_text.splitlines():
            row = re.match(r"^\|\s*`([^`]+)`", line)
            if row:
                full = row.group(1)
                tokens[full.split()[0]] = full
        return tokens

    @staticmethod
    def _real_flags(subparser):
        # The option strings/positional dest argparse actually accepts
        # for this (sub)parser, excluding the auto-added -h/--help, plus
        # a name -> Action map for the choices cross-check below.
        flags = {}
        for action in subparser._actions:
            if action.dest == "help":
                continue
            names = action.option_strings or [action.dest]
            for name in names:
                flags[name] = action
        return flags

    @classmethod
    def _subcommands(cls):
        parser = cli.build_parser()
        subparsers_action = next(
            a for a in parser._actions if isinstance(a, argparse._SubParsersAction)
        )
        return parser, subparsers_action.choices

    def test_every_subcommand_has_its_own_section(self):
        _, subcommands = self._subcommands()
        text = self._docs_text()
        for name in subcommands:
            assert re.search(rf"^## `gdeltforge {re.escape(name)}`\s*$", text, re.MULTILINE), (
                f"docs/cli-reference.md has no '## `gdeltforge {name}`' section"
            )

    def test_no_stale_command_sections(self):
        _, subcommands = self._subcommands()
        text = self._docs_text()
        documented = set(re.findall(r"^## `gdeltforge ([\w-]+)`\s*$", text, re.MULTILINE))
        stale = documented - set(subcommands)
        assert not stale, (
            f"cli-reference.md documents 'gdeltforge {stale}' but no such subcommand exists"
        )

    def test_every_subcommand_listed_in_the_command_table(self):
        _, subcommands = self._subcommands()
        # The top-of-file "| Command | Description |" table, everything
        # before the first real section heading.
        table_section = self._docs_text().split("## Global options")[0]
        for name in subcommands:
            assert f"`{name}`" in table_section, (
                f"'{name}' is missing from the command table at the top of cli-reference.md"
            )

    def test_global_config_flag_is_documented(self):
        parser, _ = self._subcommands()
        section = self._section(self._docs_text(), "Global options")
        documented = set(re.findall(r"`(--[\w-]+)", section))
        real_config_action = next(a for a in parser._actions if a.dest == "config")
        assert set(real_config_action.option_strings) <= documented

    def test_every_real_flag_is_documented(self):
        _, subcommands = self._subcommands()
        text = self._docs_text()
        for name, subparser in subcommands.items():
            documented = self._documented_tokens(self._section(text, f"`gdeltforge {name}`"))
            real = self._real_flags(subparser)
            missing = set(real) - set(documented)
            assert not missing, (
                f"gdeltforge {name}: {sorted(missing)} accepted by argparse but not "
                f"documented in cli-reference.md"
            )

    def test_no_stale_flags_documented(self):
        # The opposite drift: a flag renamed or removed in argparse that
        # is still sitting in the docs table, describing behavior that
        # no longer exists.
        _, subcommands = self._subcommands()
        text = self._docs_text()
        for name, subparser in subcommands.items():
            documented = self._documented_tokens(self._section(text, f"`gdeltforge {name}`"))
            real = self._real_flags(subparser)
            stale = set(documented) - set(real)
            assert not stale, (
                f"gdeltforge {name}: {sorted(stale)} documented in cli-reference.md but not "
                f"accepted by argparse (renamed or removed?)"
            )

    def test_documented_choices_match_argparse_choices(self):
        # Where a documented flag spells out a `{a,b,c}` choice set (the
        # same rendering argparse's own usage/help text uses), it must
        # be the same set of values as the real choices= argparse
        # enforces, not just the same flag name. Order-insensitive:
        # argparse's choices= is a list for iteration, not a promise
        # about display order, and sample's own choices=["indexed",
        # "filtered", "daily"] already documents in a different order
        # ("indexed,daily,filtered") without that being a real bug.
        _, subcommands = self._subcommands()
        text = self._docs_text()
        for name, subparser in subcommands.items():
            documented = self._documented_tokens(self._section(text, f"`gdeltforge {name}`"))
            real = self._real_flags(subparser)
            for flag, full_token in documented.items():
                action = real.get(flag)
                if action is None or not action.choices:
                    continue
                doc_choices_match = re.search(r"\{([^}]+)\}", full_token)
                if not doc_choices_match:
                    continue
                documented_choices = set(doc_choices_match.group(1).split(","))
                real_choices = {str(c) for c in action.choices}
                assert documented_choices == real_choices, (
                    f"gdeltforge {name} {flag}: docs list choices {documented_choices}, "
                    f"argparse actually accepts {real_choices}"
                )


class TestVersionFlag:
    """--version is registered as a custom Action specifically so it can
    print the full banner on a real terminal but plain text when piped,
    matching the brand system's "never on piped output" rule. Like the
    built-in action="version", it must fire and exit before argparse's
    own required-subcommand check ever runs.
    """

    def test_works_without_a_subcommand(self, capsys):
        parser = cli.build_parser()

        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--version"])

        assert exc_info.value.code == 0

    def test_prints_plain_text_when_not_a_tty(self, monkeypatch, capsys):
        monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
        parser = cli.build_parser()

        with pytest.raises(SystemExit):
            parser.parse_args(["--version"])

        out = capsys.readouterr().out.strip()
        assert out == f"gdeltforge {cli.__version__}"

    def test_prints_the_full_banner_on_a_real_terminal(self, monkeypatch, capsys):
        monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
        parser = cli.build_parser()

        with pytest.raises(SystemExit):
            parser.parse_args(["--version"])

        out = capsys.readouterr().out
        assert "GdeltForge" in out
        assert cli.__version__ in out
        assert cli._TAGLINE.lower() in out

    def test_normal_commands_still_parse_with_version_now_registered(self):
        parser = cli.build_parser()
        args = parser.parse_args(["convert", "--dataset", "events"])
        assert args.command == "convert"
