import logging
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

import gdeltforge.conversion.converter as converter_module
from gdeltforge.conversion.converter import GDELTConverter, run_converter


def _make_config(tmp_path, **converter_overrides):
    cfg = {
        "paths": {
            "downloaded_data_directory": str(tmp_path / "raw"),
            "unzipped_data_directory": str(tmp_path / "csv"),
            "parquet_data_directory": str(tmp_path / "parquet"),
        },
        "converter": {
            "keep_unzipped": False,
            "file_pattern": "*.zip",
        },
        "columns": {"gdelt_event": ["GlobalEventID", "Day"]},
        "columns_numeric": {"gdelt_event": ["GlobalEventID", "Day"]},
    }
    cfg["converter"].update(converter_overrides)
    return cfg


class TestDetectFileType:
    @pytest.fixture
    def converter(self, tmp_path):
        return GDELTConverter(_make_config(tmp_path))

    @pytest.mark.parametrize(
        "filename,expected",
        [
            ("20200315.export.CSV.zip", "daily"),
            ("202003.zip", "monthly"),
            ("2020.zip", "yearly"),
            # GKG 2.1/Mentions' real 15-minute cadence: a 14-digit prefix,
            # not 8. Regression guard for a gap found for real: this used
            # to fall through every pattern (including _DAILY_PAT, whose
            # 8-digits-then-a-dot requirement a 14-digit prefix never
            # satisfies) and come back "unknown" had anything ever called
            # _detect_file_type on one, which nothing did in practice
            # (see process_single_file's own "flat" shortcut).
            ("20150219080000.mentions.CSV.zip", "quarter_hourly"),
            ("GDELT.MASTERREDUCEDV2.1979-2013.zip", "unknown"),
        ],
    )
    def test_cases(self, converter, filename, expected):
        assert converter._detect_file_type(filename) == expected


class TestPartitionRuleRouting:
    """Whether a file goes to the historical (Hive-partitioned) path is
    decided by whether converter.partitioning.rules actually defines an
    entry for its detected file_type, not by comparing file_type against
    a hardcoded "daily" string. Real configs only ever define rules for
    yearly/monthly, so daily and quarter_hourly files must still flat
    write when partitioning is enabled for the dataset, the same as they
    do when it's off entirely, and without a spurious "no partition rule"
    warning firing for every single one of them."""

    def test_a_daily_file_flat_writes_without_a_warning_when_only_yearly_has_a_rule(
        self, tmp_path, caplog
    ):
        cfg = _make_config(
            tmp_path,
            partitioning={"enabled": True, "rules": [{"file_type": "yearly", "by": ["Year"]}]},
        )
        cfg["paths"]["parquet_historical_directory"] = str(tmp_path / "historical")
        zip_path = _write_flat_zip(tmp_path / "raw")

        with caplog.at_level("WARNING"):
            outputs = GDELTConverter(cfg).process_single_file(str(zip_path))

        assert len(outputs) == 1
        assert (tmp_path / "parquet" / "20200101.export.parquet").exists()
        assert not any("No partition rule" in r.message for r in caplog.records)

    def test_a_quarter_hourly_file_flat_writes_without_a_warning(self, tmp_path, caplog):
        cfg = _make_config(
            tmp_path,
            partitioning={"enabled": True, "rules": [{"file_type": "yearly", "by": ["Year"]}]},
        )
        cfg["paths"]["parquet_historical_directory"] = str(tmp_path / "historical")
        zip_path = _write_flat_zip(
            tmp_path / "raw",
            filename="20150219080000.mentions.CSV.zip",
            rows="1\t20150219\n",
        )

        with caplog.at_level("WARNING"):
            outputs = GDELTConverter(cfg).process_single_file(str(zip_path))

        assert len(outputs) == 1
        assert (tmp_path / "parquet" / "20150219080000.mentions.parquet").exists()
        assert not any("No partition rule" in r.message for r in caplog.records)


class TestMaxWorkersConfig:
    def test_defaults_to_none_so_executor_uses_cpu_count(self, tmp_path):
        converter = GDELTConverter(_make_config(tmp_path))
        assert converter.max_workers is None

    def test_explicit_value_is_respected(self, tmp_path):
        converter = GDELTConverter(_make_config(tmp_path, max_workers=2))
        assert converter.max_workers == 2


class TestMaxWorkersByDataset:
    def test_falls_back_to_the_scalar_default_when_unset(self, tmp_path):
        converter = GDELTConverter(_make_config(tmp_path, max_workers=4))
        assert converter.max_workers == 4

    def test_dataset_override_takes_precedence(self, tmp_path):
        cfg = _make_config(tmp_path)
        cfg["columns"]["gdelt_gkg_v2"] = ["DocumentIdentifier"]
        cfg["columns_numeric"]["gdelt_gkg_v2"] = []
        cfg["paths"]["gkg_v2_downloaded_data_directory"] = str(tmp_path / "gkg_raw")
        cfg["paths"]["gkg_v2_unzipped_data_directory"] = str(tmp_path / "gkg_csv")
        cfg["paths"]["gkg_v2_parquet_data_directory"] = str(tmp_path / "gkg_parquet")
        cfg["converter"]["max_workers"] = 4
        cfg["converter"]["max_workers_by_dataset"] = {"gdelt_gkg_v2": 8}

        events = GDELTConverter(cfg)
        gkg = GDELTConverter(cfg, dataset="gdelt_gkg_v2")

        assert events.max_workers == 4
        assert gkg.max_workers == 8

    def test_missing_key_does_not_error(self, tmp_path):
        # Configs written before this option existed have no
        # max_workers_by_dataset key at all.
        converter = GDELTConverter(_make_config(tmp_path, max_workers=2))
        assert converter.max_workers == 2


class TestOutputColumnsConfig:
    def test_defaults_to_none_so_every_column_is_parsed(self, tmp_path):
        converter = GDELTConverter(_make_config(tmp_path))
        assert converter.output_columns is None

    def test_explicit_value_is_respected(self, tmp_path):
        converter = GDELTConverter(_make_config(tmp_path, output_columns={
            "gdelt_event": ["Day"],
        }))
        assert converter.output_columns == ["Day"]

    def test_projects_columns_during_csv_parsing(self, tmp_path):
        # Proves the pruning actually reaches pandas' read_csv (usecols),
        # not just that the config value is stored: the resulting parquet
        # must carry only the configured subset, in COLUMN_NAMES' relative
        # order, with numeric coercion still applied to whichever of those
        # columns is in columns_numeric.
        cfg = _make_config(tmp_path, output_columns={"gdelt_event": ["Day"]})
        csv_path = tmp_path / "20200101.export.csv"
        csv_path.write_text("1\t20200101\n2\t20200102\n")
        zip_path = tmp_path / "20200101.export.CSV.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.write(csv_path, arcname="20200101.export.csv")

        converter = GDELTConverter(cfg)
        outputs = converter.process_single_file(str(zip_path))

        df = pd.read_parquet(outputs[0])
        assert list(df.columns) == ["Day"]
        assert df["Day"].tolist() == [20200101, 20200102]

    def test_a_configured_column_not_in_this_dataset_is_skipped_not_fatal(self, tmp_path):
        cfg = _make_config(
            tmp_path, output_columns={"gdelt_event": ["Day", "DoesNotExist"]}
        )
        csv_path = tmp_path / "20200101.export.csv"
        csv_path.write_text("1\t20200101\n")
        zip_path = tmp_path / "20200101.export.CSV.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.write(csv_path, arcname="20200101.export.csv")

        converter = GDELTConverter(cfg)
        outputs = converter.process_single_file(str(zip_path))

        df = pd.read_parquet(outputs[0])
        assert list(df.columns) == ["Day"]


class TestCompressionConfig:
    def test_defaults_to_zstd(self, tmp_path):
        # Matching filter.compression's default (see test_filter.py's
        # TestCompressionConfig): measured ~30% smaller than snappy on
        # real GDELT Events data at comparable or faster write speed, and
        # lossless, so there's no accuracy tradeoff to weigh.
        converter = GDELTConverter(_make_config(tmp_path))
        assert converter.compression == "zstd"

    def test_default_codec_is_used_on_write(self, tmp_path):
        zip_path = _write_flat_zip(tmp_path / "raw")
        converter = GDELTConverter(_make_config(tmp_path))
        outputs = converter.process_single_file(str(zip_path))

        metadata = pq.ParquetFile(outputs[0]).metadata
        codec = metadata.row_group(0).column(0).compression
        assert codec.lower() == "zstd"

    def test_explicit_codec_overrides_the_default(self, tmp_path):
        cfg = _make_config(tmp_path, compression={"gdelt_event": "snappy"})
        zip_path = _write_flat_zip(tmp_path / "raw")
        converter = GDELTConverter(cfg)
        outputs = converter.process_single_file(str(zip_path))

        metadata = pq.ParquetFile(outputs[0]).metadata
        codec = metadata.row_group(0).column(0).compression
        assert codec.lower() == "snappy"

    def test_a_changed_compression_forces_reprocessing(self, tmp_path):
        cfg = _make_config(tmp_path, compression={"gdelt_event": "snappy"})
        _write_flat_zip(tmp_path / "raw", rows="1\t20200101\n")

        GDELTConverter(cfg).process_all_files()
        out_path = tmp_path / "parquet" / "20200101.export.parquet"
        codec = pq.ParquetFile(out_path).metadata.row_group(0).column(0).compression
        assert codec.lower() == "snappy"

        # Rerun with a different codec must not be skipped by the marker
        # left above, and must actually rewrite with the new codec rather
        # than leaving the stale snappy file in place.
        cfg2 = _make_config(tmp_path, compression={"gdelt_event": "zstd"})
        outputs, failed = GDELTConverter(cfg2).process_all_files()

        assert failed == []
        assert len(outputs) == 1
        codec = pq.ParquetFile(out_path).metadata.row_group(0).column(0).compression
        assert codec.lower() == "zstd"


class TestRunConverterWarnsAboutCrossrefJoinKey:
    """run_converter shares the same output_columns/crossref hazard as
    run_filter (see test_filter.py's TestCrossrefJoinKeyWarning) and the
    exact same fix: warn at configure time via
    gdeltforge.crossref.crossref.warn_if_output_columns_drops_join_key,
    shared between both wrappers rather than reimplemented. The warning
    fires from config resolution alone, before any zip is processed, so
    these tests don't need a real zip fixture."""

    def test_warns_when_output_columns_omits_the_join_key(self, tmp_path, caplog):
        # _make_config's own columns.gdelt_event is ["GlobalEventID", "Day"];
        # pruning to just "Day" drops the join key.
        cfg = _make_config(tmp_path, output_columns={"gdelt_event": ["Day"]})

        with caplog.at_level("WARNING"):
            run_converter(cfg)

        assert any(
            "GlobalEventID" in r.message and "crossref" in r.message for r in caplog.records
        )

    def test_no_warning_when_the_join_key_is_kept(self, tmp_path, caplog):
        cfg = _make_config(
            tmp_path, output_columns={"gdelt_event": ["GlobalEventID", "Day"]}
        )

        with caplog.at_level("WARNING"):
            run_converter(cfg)

        assert not any("crossref" in r.message for r in caplog.records)

    def test_no_warning_when_output_columns_is_unset(self, tmp_path, caplog):
        cfg = _make_config(tmp_path)

        with caplog.at_level("WARNING"):
            run_converter(cfg)

        assert not any("crossref" in r.message for r in caplog.records)


class TestRunConverterWarnsAboutDeleteSource:
    """Same shared warning as run_filter (see test_filter.py's own
    version), fired from config resolution alone, before any zip is
    processed."""

    def test_warns_when_delete_source_and_output_columns_are_both_active(self, tmp_path, caplog):
        cfg = _make_config(tmp_path, output_columns={"gdelt_event": ["GlobalEventID"]})

        with caplog.at_level("WARNING"):
            run_converter(cfg, delete_source=True)

        assert any(
            "delete_source" in r.message and "output_columns" in r.message
            for r in caplog.records
        )

    def test_no_warning_when_delete_source_is_false(self, tmp_path, caplog):
        cfg = _make_config(tmp_path, output_columns={"gdelt_event": ["GlobalEventID"]})

        with caplog.at_level("WARNING"):
            run_converter(cfg, delete_source=False)

        assert not any("delete_source" in r.message for r in caplog.records)

    def test_no_warning_when_output_columns_is_unset(self, tmp_path, caplog):
        cfg = _make_config(tmp_path)

        with caplog.at_level("WARNING"):
            run_converter(cfg, delete_source=True)

        assert not any("delete_source" in r.message for r in caplog.records)


class TestVerboseLogging:
    """--verbose raises this module's own logger to DEBUG, revealing the
    per-file "Processing ZIP"/"Skipping already converted" lines that are
    DEBUG-level (invisible) by default. logger.setLevel is a real,
    process-wide mutation on a singleton (logging.getLogger caches by
    name), so every test here restores INFO afterward regardless of
    outcome, rather than leaking state into whichever test runs next."""

    def test_off_by_default_logger_level_is_unchanged(self, tmp_path):
        converter_module.logger.setLevel(logging.INFO)
        _write_flat_zip(tmp_path / "raw")

        run_converter(_make_config(tmp_path))

        assert converter_module.logger.level == logging.INFO

    def test_verbose_lowers_the_logger_to_debug(self, tmp_path):
        _write_flat_zip(tmp_path / "raw")
        try:
            run_converter(_make_config(tmp_path), verbose=True)
            assert converter_module.logger.level == logging.DEBUG
        finally:
            converter_module.logger.setLevel(logging.INFO)

    def test_verbose_reveals_the_per_file_processing_line(self, tmp_path, capfd):
        # process_single_file runs inside a ProcessPoolExecutor worker, a
        # genuinely separate process, so its output reaches the terminal
        # via the inherited stderr file descriptor, not this process's
        # own Python logging records -- caplog (in-process record capture)
        # can't see it at all; capfd (OS file-descriptor capture) can.
        _write_flat_zip(tmp_path / "raw")
        try:
            run_converter(_make_config(tmp_path), verbose=True)
            assert "Processing ZIP" in capfd.readouterr().err
        finally:
            converter_module.logger.setLevel(logging.INFO)


class TestQuietLogging:
    """--quiet raises this module's own logger to WARNING, suppressing
    the setup/summary lines run_converter otherwise always logs at INFO.
    Mutually exclusive with --verbose at the CLI; this module doesn't
    enforce that itself, so it isn't re-tested here."""

    def test_quiet_raises_the_logger_to_warning(self, tmp_path):
        _write_flat_zip(tmp_path / "raw")
        try:
            run_converter(_make_config(tmp_path), quiet=True)
            assert converter_module.logger.level == logging.WARNING
        finally:
            converter_module.logger.setLevel(logging.INFO)

    def test_quiet_suppresses_the_summary_line(self, tmp_path, capfd):
        # Same cross-process reasoning as TestVerboseLogging's own
        # capfd-based test: process_single_file runs inside a
        # ProcessPoolExecutor worker, so its output must be checked via
        # the inherited stderr file descriptor, not in-process caplog.
        _write_flat_zip(tmp_path / "raw")
        try:
            run_converter(_make_config(tmp_path), quiet=True)
            assert "Conversion complete" not in capfd.readouterr().err
        finally:
            converter_module.logger.setLevel(logging.INFO)


class TestDatasetParameter:
    def test_defaults_to_events_for_backward_compatibility(self, tmp_path):
        converter = GDELTConverter(_make_config(tmp_path))
        assert converter.dataset == "gdelt_event"
        assert converter.COLUMN_NAMES == ["GlobalEventID", "Day"]

    def test_non_events_dataset_reads_its_own_columns_and_paths(self, tmp_path):
        cfg = _make_config(tmp_path)
        cfg["columns"]["gdelt_gkg_v2"] = ["DocumentIdentifier", "V21Date"]
        cfg["columns_numeric"]["gdelt_gkg_v2"] = ["V21Date"]
        cfg["paths"]["gkg_v2_downloaded_data_directory"] = str(tmp_path / "gkg_raw")
        cfg["paths"]["gkg_v2_unzipped_data_directory"] = str(tmp_path / "gkg_csv")
        cfg["paths"]["gkg_v2_parquet_data_directory"] = str(tmp_path / "gkg_parquet")

        converter = GDELTConverter(cfg, dataset="gdelt_gkg_v2")

        assert converter.COLUMN_NAMES == ["DocumentIdentifier", "V21Date"]
        assert converter.NUMERIC_COLUMNS == ["V21Date"]
        assert converter.input_folder == tmp_path / "gkg_raw"

    def test_missing_dataset_path_key_raises_clearly(self, tmp_path):
        cfg = _make_config(tmp_path)
        cfg["columns"]["gdelt_gkg_v2"] = ["DocumentIdentifier"]
        cfg["columns_numeric"]["gdelt_gkg_v2"] = []
        # No gkg_v2_*_directory paths configured.

        with pytest.raises(KeyError):
            GDELTConverter(cfg, dataset="gdelt_gkg_v2")


def _write_gkg_v1_zip(tmp_path, filename="20130401.gkg.csv.zip"):
    """A real ZIP containing a literal GKG 1.0 header line followed by one
    tab-separated data row, matching a real downloaded file byte for byte
    in shape (confirmed 2026-08-04 against a live 20200101.gkg.csv.zip):
    a header row Events/GKG 2.1/Mentions don't have, and a comma-delimited
    (not semicolon) EventIds field."""
    csv_path = tmp_path / "20130401.gkg.csv"
    header = "\t".join(["DATE", "NUMARTS", "COUNTS", "THEMES", "CAMEOEVENTIDS"])
    row = "\t".join([
        "20130401", "5", "KILL#10#1", "TAX_FNCACT;GENERAL_GOVERNMENT",
        "123456,789012",
    ])
    csv_path.write_text(header + "\n" + row + "\n")

    zip_path = tmp_path / filename
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(csv_path, arcname="20130401.gkg.csv")
    return zip_path


class TestConvertsNonEventsSchemaEndToEnd:
    """Regression coverage for the actual CSV -> Parquet conversion path on
    a non-Events schema, not just constructor wiring (see
    TestDatasetParameter above): tab-split, column naming, numeric
    coercion, and header-row handling all need to work correctly against a
    real GKG-shaped file, and the comma-delimited EventIds field must
    survive as a raw string rather than being coerced or truncated."""

    @staticmethod
    def _config(tmp_path):
        return {
            "paths": {
                "gkg_v1_downloaded_data_directory": str(tmp_path / "raw"),
                "gkg_v1_unzipped_data_directory": str(tmp_path / "csv"),
                "gkg_v1_parquet_data_directory": str(tmp_path / "parquet"),
            },
            "converter": {"keep_unzipped": False, "file_pattern": "*.zip"},
            "columns": {
                "gdelt_gkg_v1": ["Date", "NumArticles", "Counts", "Themes", "EventIds"],
            },
            "columns_numeric": {"gdelt_gkg_v1": ["Date", "NumArticles"]},
        }

    def test_process_single_file_parses_gkg_v1_row_correctly(self, tmp_path):
        cfg = self._config(tmp_path)
        zip_path = _write_gkg_v1_zip(tmp_path)

        converter = GDELTConverter(cfg, dataset="gdelt_gkg_v1")
        outputs = converter.process_single_file(str(zip_path))

        assert len(outputs) == 1
        df = pd.read_parquet(outputs[0])
        # Exactly one row: the header line must be consumed as a header,
        # not misread as a second, garbage data row.
        assert len(df) == 1
        assert list(df.columns) == ["Date", "NumArticles", "Counts", "Themes", "EventIds"]
        assert df["Date"].iloc[0] == 20130401
        assert df["NumArticles"].iloc[0] == 5
        # Numeric coercion must be scoped to columns_numeric only: EventIds
        # is a comma-delimited list, not a scalar, and must survive untouched.
        assert df["EventIds"].iloc[0] == "123456,789012"
        assert df["Themes"].iloc[0] == "TAX_FNCACT;GENERAL_GOVERNMENT"

    def test_run_converter_wrapper_processes_a_non_events_dataset(self, tmp_path):
        cfg = self._config(tmp_path)
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_gkg_v1_zip(raw_dir)

        outputs, failed = run_converter(cfg, dataset="gdelt_gkg_v1")

        assert failed == []
        assert len(outputs) == 1
        out_df = pd.read_parquet(outputs[0])
        assert len(out_df) == 1
        assert out_df["EventIds"].iloc[0] == "123456,789012"

    def test_events_schema_stays_headerless(self, tmp_path):
        # Regression guard for _DATASETS_WITH_HEADER_ROW: Events (and, by
        # the same code path, GKG 2.1/Mentions) must never have their
        # first real data row swallowed as a phantom header.
        cfg = _make_config(tmp_path)
        csv_path = tmp_path / "20200101.export.csv"
        # columns are ["GlobalEventID", "Day"], in that order.
        csv_path.write_text("1\t20200101\n2\t20200102\n")
        zip_path = tmp_path / "20200101.export.CSV.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.write(csv_path, arcname="20200101.export.csv")

        converter = GDELTConverter(cfg)
        outputs = converter.process_single_file(str(zip_path))

        df = pd.read_parquet(outputs[0])
        assert len(df) == 2
        assert df["Day"].tolist() == [20200101, 20200102]


def _write_flat_zip(raw_dir, filename="20200101.export.CSV.zip", rows="1\t20200101\n"):
    """A minimal Events-shaped zip (GlobalEventID, Day) for exercising the
    flat/daily conversion path end to end, same shape used throughout this
    file's other tests."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_name = filename.removesuffix(".zip")
    if not csv_name.lower().endswith(".csv"):
        csv_name += ".csv"
    csv_path = raw_dir / csv_name
    csv_path.write_text(rows)
    zip_path = raw_dir / filename
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(csv_path, arcname=csv_name)
    csv_path.unlink()
    return zip_path


class TestConversionResumability:
    """Regression coverage for the .done marker mechanism: found for real
    against a live 30,137-file Mentions batch that flat/daily conversions
    had no resumability at all (unlike scrape's skip-already-downloaded
    behavior) -- two independent runs each died around the same ~51% mark
    after 30+ minutes having made no net progress relaunch to relaunch,
    because every attempt reprocessed every zip from file 1, needlessly
    overwriting output that was already correct. The marker mechanism
    already existed for historical (Hive-partitioned) files but was never
    itself covered by a test; both paths are covered here now.

    The marker also carries a fingerprint of output_columns, the one
    converter setting a user plausibly reruns with a different value:
    without that check, a rerun after changing it would be skipped by a
    marker from the old value and silently serve output shaped by it."""

    def test_flat_daily_conversion_creates_a_done_marker(self, tmp_path):
        cfg = _make_config(tmp_path)
        zip_path = _write_flat_zip(tmp_path / "raw")

        converter = GDELTConverter(cfg)
        outputs, failed = converter.process_all_files()

        assert failed == []
        assert len(outputs) == 1
        assert converter._is_done(zip_path)

    def test_a_previously_converted_flat_zip_is_skipped_on_rerun(self, tmp_path, caplog):
        cfg = _make_config(tmp_path)
        zip_path = _write_flat_zip(tmp_path / "raw")
        converter = GDELTConverter(cfg)
        converter.process_all_files()

        # get_logger sets an explicit INFO level on this module's own
        # named logger at import time, so a bare caplog.at_level("DEBUG")
        # (root-only) never reaches it; the logger name must be given
        # explicitly to actually lower its effective level.
        with caplog.at_level("DEBUG", logger="gdeltforge.conversion.converter"):
            outputs, failed = converter.process_all_files()

        assert failed == []
        assert outputs == []
        assert any(
            "Skipping already converted" in r.message and zip_path.name in r.message
            for r in caplog.records
        )

    def test_a_changed_output_columns_forces_reprocessing(self, tmp_path):
        cfg = _make_config(tmp_path, output_columns={"gdelt_event": ["GlobalEventID", "Day"]})
        cfg["columns"]["gdelt_event"] = ["GlobalEventID", "Day"]
        cfg["columns_numeric"]["gdelt_event"] = ["GlobalEventID", "Day"]
        _write_flat_zip(tmp_path / "raw", rows="1\t20200101\n")

        GDELTConverter(cfg).process_all_files()
        out_path = tmp_path / "parquet" / "20200101.export.parquet"
        assert list(pd.read_parquet(out_path).columns) == ["GlobalEventID", "Day"]

        # Rerun with a narrower output_columns must not be skipped by the
        # marker left above, and must actually reproduce the narrower
        # output rather than leaving the stale two-column file in place.
        cfg2 = _make_config(tmp_path, output_columns={"gdelt_event": ["Day"]})
        cfg2["columns"] = cfg["columns"]
        cfg2["columns_numeric"] = cfg["columns_numeric"]
        outputs, failed = GDELTConverter(cfg2).process_all_files()

        assert failed == []
        assert len(outputs) == 1
        assert list(pd.read_parquet(out_path).columns) == ["Day"]

    def test_a_zip_that_still_errors_is_not_marked_done(self, tmp_path, monkeypatch):
        # A failed conversion must stay eligible for retry on the next run,
        # not get silently marked complete.
        cfg = _make_config(tmp_path)
        zip_path = _write_flat_zip(tmp_path / "raw")
        converter = GDELTConverter(cfg)

        def boom(self, zip_path):
            raise RuntimeError("simulated crash mid-conversion")

        monkeypatch.setattr(GDELTConverter, "process_single_file", boom)
        outputs, failed = converter.process_all_files()

        assert outputs == []
        assert failed == [zip_path.name]
        assert not converter._is_done(zip_path)

    def test_historical_partitioned_conversion_creates_a_done_marker(self, tmp_path):
        cfg = _make_config(
            tmp_path,
            partitioning={"enabled": True, "rules": [{"file_type": "yearly", "by": ["Year"]}]},
        )
        cfg["columns"]["gdelt_event"] = ["GlobalEventID", "Year", "Day"]
        cfg["columns_numeric"]["gdelt_event"] = ["GlobalEventID", "Year", "Day"]
        cfg["paths"]["parquet_historical_directory"] = str(tmp_path / "historical")
        zip_path = _write_flat_zip(
            tmp_path / "raw", filename="2020.zip", rows="1\t2020\t20200101\n"
        )

        converter = GDELTConverter(cfg)
        outputs, failed = converter.process_all_files()

        assert failed == []
        assert len(outputs) == 1
        assert converter._is_done(zip_path)

    def test_a_previously_converted_historical_zip_is_skipped_on_rerun(self, tmp_path, caplog):
        cfg = _make_config(
            tmp_path,
            partitioning={"enabled": True, "rules": [{"file_type": "yearly", "by": ["Year"]}]},
        )
        cfg["columns"]["gdelt_event"] = ["GlobalEventID", "Year", "Day"]
        cfg["columns_numeric"]["gdelt_event"] = ["GlobalEventID", "Year", "Day"]
        cfg["paths"]["parquet_historical_directory"] = str(tmp_path / "historical")
        zip_path = _write_flat_zip(
            tmp_path / "raw", filename="2020.zip", rows="1\t2020\t20200101\n"
        )
        converter = GDELTConverter(cfg)
        converter.process_all_files()

        # get_logger sets an explicit INFO level on this module's own
        # named logger at import time, so a bare caplog.at_level("DEBUG")
        # (root-only) never reaches it; the logger name must be given
        # explicitly to actually lower its effective level.
        with caplog.at_level("DEBUG", logger="gdeltforge.conversion.converter"):
            outputs, failed = converter.process_all_files()

        assert failed == []
        assert outputs == []
        assert any(
            "Skipping already converted" in r.message and zip_path.name in r.message
            for r in caplog.records
        )


class TestDeleteSource:
    """delete_source (CLI: --delete-source) removes the source zip once
    its parquet output is confirmed written and marked done, so a full
    historical pull doesn't need to hold the raw archive and the
    converted output at once. Off by default."""

    def test_off_by_default_source_zip_survives(self, tmp_path):
        zip_path = _write_flat_zip(tmp_path / "raw")
        GDELTConverter(_make_config(tmp_path)).process_all_files()

        assert zip_path.exists()

    def test_deletes_the_source_zip_after_a_successful_conversion(self, tmp_path):
        zip_path = _write_flat_zip(tmp_path / "raw")
        outputs, failed = GDELTConverter(
            _make_config(tmp_path), delete_source=True
        ).process_all_files()

        assert failed == []
        assert len(outputs) == 1
        assert not zip_path.exists()
        assert Path(outputs[0]).exists()

    def test_never_deletes_a_zip_that_failed_to_convert(self, tmp_path, monkeypatch):
        zip_path = _write_flat_zip(tmp_path / "raw")
        converter = GDELTConverter(_make_config(tmp_path), delete_source=True)

        def boom(self, zip_path):
            raise RuntimeError("simulated crash mid-conversion")

        monkeypatch.setattr(GDELTConverter, "process_single_file", boom)
        outputs, failed = converter.process_all_files()

        assert outputs == []
        assert failed == [zip_path.name]
        assert zip_path.exists()

    def test_deletion_failure_is_logged_not_fatal(self, tmp_path, monkeypatch, caplog):
        # The conversion itself already succeeded; a failure to delete the
        # source afterward (permissions, already gone) must not be
        # reported as a conversion failure. Only the zip's own unlink is
        # made to fail here, not Path.unlink generally: process_single_file
        # also unlinks the intermediate CSV (keep_unzipped=False), and
        # that one must still succeed normally.
        zip_path = _write_flat_zip(tmp_path / "raw")
        real_unlink = Path.unlink

        def selective_unlink(self, *args, **kwargs):
            if self.suffix == ".zip":
                raise OSError("locked")
            return real_unlink(self, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", selective_unlink)

        with caplog.at_level("WARNING"):
            outputs, failed = GDELTConverter(
                _make_config(tmp_path), delete_source=True
            ).process_all_files()

        assert failed == []
        assert len(outputs) == 1
        assert zip_path.exists()
        assert any(
            "Could not delete source zip" in r.message and zip_path.name in r.message
            for r in caplog.records
        )


class TestSaveParquetAtomicity:
    """_save_parquet/_save_historical_parquet used to write straight to
    their final path. Found for real: a process killed mid-write while
    converting a real ~3,000-file GKG 2.1 batch left two truncated,
    corrupt parquet files sitting at their final paths, with nothing in
    the pipeline able to detect or clean them up later (filter/sample
    would just fail confusingly whenever they got read). Both now write
    through a temp-file-then-rename, so a kill leaves either a complete
    file or nothing, matching the pattern already used for downloads and
    sample output."""

    def test_save_parquet_leaves_no_file_on_write_failure(self, tmp_path, monkeypatch):
        cfg = _make_config(tmp_path)
        converter = GDELTConverter(cfg)

        def boom(self, path, **kwargs):
            Path(path).write_bytes(b"partial write before a simulated crash")
            raise OSError("simulated crash mid-write")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

        df = pd.DataFrame({"GlobalEventID": [1], "Day": [20200101]})
        result = converter._save_parquet(df, "20200101")

        assert result is None
        out_path = tmp_path / "parquet" / "20200101.parquet"
        assert not out_path.exists()
        assert not out_path.with_name(out_path.name + ".tmp").exists()

    def test_save_historical_parquet_leaves_no_file_on_write_failure(self, tmp_path, monkeypatch):
        cfg = _make_config(
            tmp_path,
            partitioning={
                "enabled": True,
                "rules": [{"file_type": "yearly", "by": ["Year"]}],
            },
        )
        cfg["paths"]["parquet_historical_directory"] = str(tmp_path / "historical")
        converter = GDELTConverter(cfg)

        def boom(table, path, **kwargs):
            Path(path).write_bytes(b"partial write before a simulated crash")
            raise OSError("simulated crash mid-write")

        monkeypatch.setattr(converter_module.pq, "write_table", boom)

        df = pd.DataFrame({"GlobalEventID": [1], "Day": [20200101], "Year": [2020]})
        with pytest.raises(OSError):
            converter._save_historical_parquet(df, Path("2020.zip"), "yearly")

        out_path = tmp_path / "historical" / "Year=2020" / "2020.parquet"
        assert not out_path.exists()
        assert not out_path.with_name(out_path.name + ".tmp").exists()
