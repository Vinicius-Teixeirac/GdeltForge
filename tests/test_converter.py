import json
import logging
import subprocess
import sys
import zipfile
from pathlib import Path

import polars as pl
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

    def test_explicit_null_is_treated_as_partitioning_disabled(self, tmp_path):
        # converter.partitioning: (nothing typed under it yet) parses to
        # None, not {}; used to crash with "'NoneType' object has no
        # attribute 'get'" on part_cfg.get("enabled", False) before ever
        # reaching a real file.
        cfg = _make_config(tmp_path, partitioning=None)
        zip_path = _write_flat_zip(tmp_path / "raw")

        outputs = GDELTConverter(cfg).process_single_file(str(zip_path))

        assert len(outputs) == 1
        assert (tmp_path / "parquet" / "20200101.export.parquet").exists()


class TestIntegerDtypePreservation:
    """Regression coverage for the real bug this was found from: GDELT's
    own raw archive can carry a blank value for a genuinely integer field
    (a real, live 20130901.export.CSV.zip has a blank DATEADDED for every
    row that day), and pd.to_numeric's own NaN handling forces the whole
    column to float64 the instant that happens, since plain int64 can't
    represent NaN at all. Previously only Year/MonthYear/Day were cast
    back to nullable Int64 afterward; every other integer-semantic
    columns_numeric entry, DATEADDED included, was left exposed."""

    @staticmethod
    def _config(tmp_path, **converter_overrides):
        cfg = {
            "paths": {
                "downloaded_data_directory": str(tmp_path / "raw"),
                "unzipped_data_directory": str(tmp_path / "csv"),
                "parquet_data_directory": str(tmp_path / "parquet"),
            },
            "converter": {"keep_unzipped": False, "file_pattern": "*.zip"},
            "columns": {
                "gdelt_event": ["GlobalEventID", "Day", "Year", "DATEADDED", "GoldsteinScale"],
            },
            "columns_numeric": {
                "gdelt_event": ["GlobalEventID", "Day", "Year", "DATEADDED", "GoldsteinScale"],
            },
        }
        cfg["converter"].update(converter_overrides)
        return cfg

    def test_a_blank_value_anywhere_no_longer_leaks_float64_for_that_column(
        self, tmp_path
    ):
        cfg = self._config(tmp_path)
        zip_path = _write_flat_zip(
            tmp_path / "raw",
            rows=(
                "1\t20200101\t2020\t20200101000000\t-2.5\n"
                # Row 2's DATEADDED is blank: the real shape GDELT's own
                # archive carries for every row on 20130901/20130902, 2013.
                "2\t20200101\t2020\t\t3.0\n"
            ),
        )

        converter = GDELTConverter(cfg)
        outputs = converter.process_single_file(str(zip_path))

        df = pl.read_parquet(outputs[0])
        assert df["DATEADDED"].dtype == pl.Int64
        assert df["DATEADDED"].to_list() == [20200101000000, None]
        # A genuinely fractional column must stay float64, not also get
        # swept up into the integer cast just for sitting in the same
        # columns_numeric list.
        assert df["GoldsteinScale"].dtype == pl.Float64
        assert df["GoldsteinScale"].to_list() == [-2.5, 3.0]

    def test_a_clean_file_with_no_blanks_is_unaffected(self, tmp_path):
        # The common case: no missing values anywhere; the cast to Int64
        # is applied unconditionally based on column membership rather
        # than any runtime dtype inspection, producing the same clean
        # output whether or not nulls are present.
        cfg = self._config(tmp_path)
        zip_path = _write_flat_zip(
            tmp_path / "raw",
            rows=(
                "1\t20200101\t2020\t20200101000000\t-2.5\n"
                "2\t20200101\t2020\t20200101010000\t3.0\n"
            ),
        )

        converter = GDELTConverter(cfg)
        outputs = converter.process_single_file(str(zip_path))

        df = pl.read_parquet(outputs[0])
        assert df["DATEADDED"].to_list() == [20200101000000, 20200101010000]

    def test_historical_hive_writes_get_the_same_protection(self, tmp_path):
        # _save_historical_parquet used to only cast the partition (`by`)
        # columns; every other integer column, DATEADDED included, could
        # still leak float64 in a historical write even after the flat-
        # write path was fixed.
        cfg = self._config(
            tmp_path,
            partitioning={"enabled": True, "rules": [{"file_type": "yearly", "by": ["Year"]}]},
        )
        cfg["paths"]["parquet_historical_directory"] = str(tmp_path / "historical")
        zip_path = _write_flat_zip(
            tmp_path / "raw",
            filename="1979.zip",
            rows=(
                "1\t19790101\t1979\t19790101000000\t-2.5\n"
                "2\t19790101\t1979\t\t3.0\n"
            ),
        )

        converter = GDELTConverter(cfg)
        outputs = converter.process_single_file(str(zip_path))

        assert len(outputs) == 1
        df = pl.read_parquet(outputs[0])
        assert df["DATEADDED"].dtype == pl.Int64
        assert df["DATEADDED"].to_list() == [19790101000000, None]
        # Year has no blank value in this file; the cast to Int64 leaves
        # it unchanged regardless, since it was already a clean integer
        # column with nothing to coerce.
        assert df["Year"].to_list() == [1979, 1979]


class TestBlankStringFieldsBecomeNull:
    """Regression coverage for a real bug found by a full content-equality
    diff against pandas' own output on a 10M-row convert fixture: without
    null_values=[""], pl.read_csv treats a QUOTED empty field ("") as a
    genuine empty-string value rather than null, diverging from pandas'
    read_csv default of nulling it. A bare, unquoted empty field (nothing
    between two tabs) is already null by polars' own default, confirmed
    directly; only the quoted form needs null_values=[""] to match pandas.

    The benchmark fixture that surfaced this builds its synthetic CSV via
    a polars DataFrame's own write_csv, which writes a real (non-null)
    empty-string value as a quoted "" specifically to distinguish it from
    a null on round-trip, confirmed directly by inspecting write_csv's
    raw output bytes. Real GDELT archives are plain, unquoted tab-
    separated text (confirmed against GDELT's own documentation: files
    carry a .csv extension but are tab-delimited with no field quoting),
    so a genuinely blank field there is the already-correctly-nulled bare
    form, not this quoted one. The fix is still the right, more robust
    contract either way, matching pandas' behavior for both forms rather
    than depending on which shape a given source happens to use.

    This silently broke columns_to_check's documented contract
    (configuration.md: "rows with a NaN/null value in any of these
    columns are dropped") for any string column fed a quoted-empty
    source, since filter.py's own null-check (pl.col(c).is_null()) never
    saw a "" value as missing."""

    def test_a_quoted_empty_field_becomes_null_not_empty_string(self, tmp_path):
        cfg = {
            "paths": {
                "downloaded_data_directory": str(tmp_path / "raw"),
                "unzipped_data_directory": str(tmp_path / "csv"),
                "parquet_data_directory": str(tmp_path / "parquet"),
            },
            "converter": {"keep_unzipped": False, "file_pattern": "*.zip"},
            "columns": {"gdelt_event": ["GlobalEventID", "Day", "Actor1EthnicCode"]},
            "columns_numeric": {"gdelt_event": ["GlobalEventID", "Day"]},
        }
        zip_path = _write_flat_zip(
            tmp_path / "raw",
            rows=(
                "1\t20200101\tKUR\n"
                # A literal quoted empty string, the exact shape polars'
                # own write_csv produces for a real (non-null)
                # empty-string DataFrame value.
                '2\t20200101\t""\n'
            ),
        )

        outputs = GDELTConverter(cfg).process_single_file(str(zip_path))
        df = pl.read_parquet(outputs[0])

        assert df["Actor1EthnicCode"].to_list() == ["KUR", None]
        assert df["Actor1EthnicCode"].null_count() == 1
        assert (df["Actor1EthnicCode"] == "").sum() == 0

    def test_a_bare_empty_field_was_already_null_before_the_fix(self, tmp_path):
        # Confirms the narrower, already-correct case stays correct: this
        # is what real GDELT's own unquoted blank fields look like, and
        # it must not regress now that null_values=[""] is also set.
        cfg = {
            "paths": {
                "downloaded_data_directory": str(tmp_path / "raw"),
                "unzipped_data_directory": str(tmp_path / "csv"),
                "parquet_data_directory": str(tmp_path / "parquet"),
            },
            "converter": {"keep_unzipped": False, "file_pattern": "*.zip"},
            "columns": {"gdelt_event": ["GlobalEventID", "Day", "Actor1EthnicCode"]},
            "columns_numeric": {"gdelt_event": ["GlobalEventID", "Day"]},
        }
        zip_path = _write_flat_zip(
            tmp_path / "raw",
            rows="1\t20200101\tKUR\n2\t20200101\t\n",
        )

        outputs = GDELTConverter(cfg).process_single_file(str(zip_path))
        df = pl.read_parquet(outputs[0])

        assert df["Actor1EthnicCode"].to_list() == ["KUR", None]
        assert df["Actor1EthnicCode"].null_count() == 1


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

    def test_explicit_null_falls_back_to_the_scalar_default(self, tmp_path):
        # converter.max_workers_by_dataset: (nothing typed under it yet)
        # parses to None, not {}; used to crash with "'NoneType' object
        # has no attribute 'get'" before reaching the scalar default.
        converter = GDELTConverter(
            _make_config(tmp_path, max_workers=4, max_workers_by_dataset=None)
        )
        assert converter.max_workers == 4


class TestOutputColumnsConfig:
    def test_defaults_to_none_so_every_column_is_parsed(self, tmp_path):
        converter = GDELTConverter(_make_config(tmp_path))
        assert converter.output_columns is None

    def test_explicit_value_is_respected(self, tmp_path):
        converter = GDELTConverter(_make_config(tmp_path, output_columns={
            "gdelt_event": ["Day"],
        }))
        assert converter.output_columns == ["Day"]

    def test_explicit_null_is_treated_the_same_as_unset(self, tmp_path):
        # converter.output_columns: (nothing typed under it yet) parses to
        # None, not {}; used to crash with "'NoneType' object has no
        # attribute 'get'" instead of falling through to "parse every
        # column", the same as the key being absent entirely.
        converter = GDELTConverter(_make_config(tmp_path, output_columns=None))
        assert converter.output_columns is None

    def test_projects_columns_during_csv_parsing(self, tmp_path):
        # Proves the pruning actually reaches polars' read_csv (columns),
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

        df = pl.read_parquet(outputs[0])
        assert list(df.columns) == ["Day"]
        assert df["Day"].to_list() == [20200101, 20200102]

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

        df = pl.read_parquet(outputs[0])
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

    def test_explicit_null_falls_back_to_the_default(self, tmp_path):
        # converter.compression: (nothing typed under it yet) parses to
        # None, not {}; used to crash with "'NoneType' object has no
        # attribute 'get'" instead of falling through to zstd.
        converter = GDELTConverter(_make_config(tmp_path, compression=None))
        assert converter.compression == "zstd"

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

    def test_explicit_null_output_columns_does_not_crash_config_resolution(self, tmp_path):
        # run_converter reads converter.output_columns independently from
        # GDELTConverter.__init__ (its own warn-before-processing check);
        # converter.output_columns: (nothing typed under it yet) parses to
        # None, not {}, and used to crash here too before a single file
        # was ever processed.
        cfg = _make_config(tmp_path, output_columns=None)
        run_converter(cfg)

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
    name), so every test here that mutates this process's own logger
    restores INFO afterward regardless of outcome, rather than leaking
    state into whichever test runs next. test_verbose_reveals_the_per_
    file_processing_line is the exception: it runs entirely in its own
    subprocess (see its own docstring for why) and never touches this
    process's logger at all."""

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

    def test_verbose_reveals_the_per_file_processing_line(self, tmp_path):
        # process_single_file runs inside a ProcessPoolExecutor worker,
        # and that worker's own logging output turns out not to be
        # reliably observable from within the SAME pytest process by any
        # means tried: ProcessPoolExecutor's start method differs by
        # platform (fork on Linux, spawn on Windows). capfd (OS-level
        # stderr capture) misses the worker's write entirely under fork,
        # confirmed empirically not a timing issue, since polling it
        # for a full 2 seconds in real Linux CI still found nothing,
        # apparently because get_logger's StreamHandler is constructed
        # once at module-import time, and a forked worker inherits that
        # *exact* handler object (fork does not re-run imports), stream
        # reference and all, rather than a live one a per-test capfd
        # redirect would see. A dynamically attached logging.FileHandler
        # fixes that (fork inherits it along with everything else) but
        # breaks the opposite way under spawn: a spawned worker re-runs
        # the module import fresh, in its own process, so it never sees
        # a handler added to the parent's logger after that import ran.
        #
        # Sidesteps both entirely by not relying on any of that: this
        # runs run_converter in a genuinely separate, ordinary Python
        # subprocess (independent of pytest's own capture machinery and
        # of ProcessPoolExecutor's platform-specific start method) and
        # captures ITS stdout/stderr the standard way: a pipe
        # subprocess.run sets up at OS-level process-creation time, in
        # place before that outer process (or anything it forks/spawns
        # in turn) ever starts, so its own worker's writes land in the
        # same captured pipe regardless of platform.
        _write_flat_zip(tmp_path / "raw")
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(_make_config(tmp_path)))
        script_path = tmp_path / "run_verbose_convert.py"
        script_path.write_text(
            "import json, sys\n"
            "from gdeltforge.conversion.converter import run_converter\n"
            # Windows' spawn start method re-imports this script as
            # __main__ in each worker process; without this guard,
            # run_converter's own ProcessPoolExecutor submission would
            # re-execute at that re-import too, recursively.
            "if __name__ == '__main__':\n"
            "    config = json.loads(open(sys.argv[1]).read())\n"
            "    run_converter(config, verbose=True)\n"
        )

        result = subprocess.run(
            [sys.executable, str(script_path), str(config_path)],
            capture_output=True, text=True, timeout=60,
        )

        assert "Processing ZIP" in result.stderr, result.stderr


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
        df = pl.read_parquet(outputs[0])
        # Exactly one row: the header line must be consumed as a header,
        # not misread as a second, garbage data row.
        assert len(df) == 1
        assert list(df.columns) == ["Date", "NumArticles", "Counts", "Themes", "EventIds"]
        assert df["Date"][0] == 20130401
        assert df["NumArticles"][0] == 5
        # Numeric coercion must be scoped to columns_numeric only: EventIds
        # is a comma-delimited list, not a scalar, and must survive untouched.
        assert df["EventIds"][0] == "123456,789012"
        assert df["Themes"][0] == "TAX_FNCACT;GENERAL_GOVERNMENT"

    def test_run_converter_wrapper_processes_a_non_events_dataset(self, tmp_path):
        cfg = self._config(tmp_path)
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_gkg_v1_zip(raw_dir)

        outputs, failed = run_converter(cfg, dataset="gdelt_gkg_v1")

        assert failed == []
        assert len(outputs) == 1
        out_df = pl.read_parquet(outputs[0])
        assert len(out_df) == 1
        assert out_df["EventIds"][0] == "123456,789012"

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

        df = pl.read_parquet(outputs[0])
        assert len(df) == 2
        assert df["Day"].to_list() == [20200101, 20200102]


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
    behavior): two independent runs each died around the same ~51% mark
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
        assert list(pl.read_parquet(out_path).columns) == ["GlobalEventID", "Day"]

        # Rerun with a narrower output_columns must not be skipped by the
        # marker left above, and must actually reproduce the narrower
        # output rather than leaving the stale two-column file in place.
        cfg2 = _make_config(tmp_path, output_columns={"gdelt_event": ["Day"]})
        cfg2["columns"] = cfg["columns"]
        cfg2["columns_numeric"] = cfg["columns_numeric"]
        outputs, failed = GDELTConverter(cfg2).process_all_files()

        assert failed == []
        assert len(outputs) == 1
        assert list(pl.read_parquet(out_path).columns) == ["Day"]

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


def _write_zip_with_raw_csv_bytes(raw_dir, filename: str, csv_bytes: bytes) -> Path:
    """Like _write_flat_zip, but writes raw bytes for the CSV content
    instead of write_text, so a genuinely invalid UTF-8 byte can be
    injected: write_text always encodes as valid UTF-8 itself."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_name = filename.removesuffix(".zip")
    if not csv_name.lower().endswith(".csv"):
        csv_name += ".csv"
    csv_path = raw_dir / csv_name
    csv_path.write_bytes(csv_bytes)
    zip_path = raw_dir / filename
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(csv_path, arcname=csv_name)
    csv_path.unlink()
    return zip_path


class TestUnicodeDecodeRecovery:
    """_read_csv used to catch every exception, including
    UnicodeDecodeError, and return an empty DataFrame; process_single_file's
    `if df.empty: continue` then treated that as "nothing to write," so the
    zip still got marked done with zero output and never appeared in the
    run's failed count. Found for real against a live 373,615-file GKG 2.1
    conversion where ~6.7% of files (25,160 of them) hit this silently.

    Both halves of the fix are covered here: a decode error is recovered
    from via encoding_errors="replace" rather than silently dropped, and
    any other genuine read failure now correctly fails the zip instead of
    marking it done."""

    def test_invalid_utf8_byte_is_recovered_not_dropped(self, tmp_path):
        cfg = _make_config(tmp_path)
        # 0xff on its own is not valid UTF-8 (not a valid single-byte or
        # leading multi-byte sequence), exactly the shape of error real
        # GKG 2.1 files hit from non-English source articles. Checked via
        # process_all_files' return values (failed/outputs), not log
        # content: those cross the ProcessPoolExecutor worker boundary
        # cleanly regardless of platform, unlike log records (see
        # test_invalid_utf8_byte_logs_a_warning below for why that half
        # needs a real subprocess instead of caplog/capfd).
        _write_zip_with_raw_csv_bytes(
            tmp_path / "raw", "20200101.export.CSV.zip",
            b"1\t2020010\xff1\n2\t20200102\n",
        )

        outputs, failed = GDELTConverter(cfg).process_all_files()

        assert failed == []
        assert len(outputs) == 1
        # Both rows survived: the whole file wasn't discarded just
        # because one byte in it couldn't be decoded.
        out = pl.read_parquet(outputs[0])
        assert len(out) == 2

    def test_invalid_utf8_byte_logs_a_warning(self, tmp_path):
        # process_single_file runs inside a ProcessPoolExecutor worker;
        # that worker's own logging output isn't reliably observable from
        # within the same pytest process by caplog OR capfd, on either
        # start method (fork or spawn), see
        # TestVerboseLogging.test_verbose_reveals_the_per_file_processing_line's
        # docstring for the full story. Same sidestep here: a genuine
        # subprocess, whose stdout/stderr pipe is set up at OS level
        # before anything it forks/spawns in turn ever starts.
        _write_zip_with_raw_csv_bytes(
            tmp_path / "raw", "20200101.export.CSV.zip",
            b"1\t2020010\xff1\n2\t20200102\n",
        )
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(_make_config(tmp_path)))
        script_path = tmp_path / "run_convert.py"
        script_path.write_text(
            "import json, sys\n"
            "from gdeltforge.conversion.converter import run_converter\n"
            "if __name__ == '__main__':\n"
            "    config = json.loads(open(sys.argv[1]).read())\n"
            "    run_converter(config)\n"
        )

        result = subprocess.run(
            [sys.executable, str(script_path), str(config_path)],
            capture_output=True, text=True, timeout=60,
        )

        assert "not valid UTF-8" in result.stderr, result.stderr

    def test_a_genuine_read_failure_raises_naming_the_csv(self, tmp_path, monkeypatch):
        # Exercised directly against process_single_file, not through
        # process_all_files' ProcessPoolExecutor: Windows spawn re-imports
        # this module fresh in each worker, so a monkeypatch made here in
        # the parent process wouldn't reach a spawned worker anyway. This
        # is exactly the boundary the fix lives at regardless.
        cfg = _make_config(tmp_path)
        zip_path = _write_flat_zip(tmp_path / "raw")
        converter = GDELTConverter(cfg)

        def _boom(csv_path):
            raise ValueError("simulated unrecoverable read failure")

        monkeypatch.setattr(converter, "_read_csv", _boom)

        with pytest.raises(RuntimeError, match="could not be processed"):
            converter.process_single_file(str(zip_path))


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

    def test_also_deletes_the_zip_s_own_done_marker(self, tmp_path):
        # The marker sits next to the zip, not the parquet output; once
        # the zip is gone it gates nothing (process_all_files' own glob
        # can never find a deleted zip again), so leaving it behind is
        # just an orphaned file --delete-source's whole point was to
        # avoid accumulating.
        zip_path = _write_flat_zip(tmp_path / "raw")
        marker_path = zip_path.with_name(zip_path.name + ".done")

        GDELTConverter(_make_config(tmp_path), delete_source=True).process_all_files()

        assert not marker_path.exists()

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


class TestBareCsvInput:
    """converter.file_pattern set to match a bare .csv (rather than the
    ZIP archives every real scrape produces) used to fail unconditionally:
    process_single_file always called unzip_file, which opened the input
    with zipfile.ZipFile regardless of its actual extension, raising a
    confusing BadZipFile for a CSV that never was a ZIP. is_bare_csv skips
    straight to _read_csv for a .csv-suffixed input instead."""

    @staticmethod
    def _write_bare_csv(raw_dir, filename="20200101.export.csv", rows="1\t20200101\n"):
        raw_dir.mkdir(parents=True, exist_ok=True)
        csv_path = raw_dir / filename
        csv_path.write_text(rows)
        return csv_path

    def test_converts_successfully_instead_of_raising_bad_zip_file(self, tmp_path):
        self._write_bare_csv(tmp_path / "raw", rows="1\t20200101\n2\t20200102\n")
        cfg = _make_config(tmp_path, file_pattern="*.csv")

        outputs, failed = GDELTConverter(cfg).process_all_files()

        assert failed == []
        assert len(outputs) == 1
        df = pl.read_parquet(outputs[0])
        assert df["Day"].to_list() == [20200101, 20200102]

    def test_source_csv_survives_by_default(self, tmp_path):
        # keep_unzipped=False (the default) must not delete a bare-csv
        # input: it's the source itself, not a scratch copy unzip_file
        # extracted, so only --delete-source is allowed to remove it.
        csv_path = self._write_bare_csv(tmp_path / "raw")
        cfg = _make_config(tmp_path, file_pattern="*.csv", keep_unzipped=False)

        outputs, failed = GDELTConverter(cfg).process_all_files()

        assert failed == []
        assert csv_path.exists()

    def test_delete_source_still_removes_the_bare_csv(self, tmp_path):
        csv_path = self._write_bare_csv(tmp_path / "raw")
        cfg = _make_config(tmp_path, file_pattern="*.csv")

        outputs, failed = GDELTConverter(cfg, delete_source=True).process_all_files()

        assert failed == []
        assert len(outputs) == 1
        assert not csv_path.exists()

    def test_stays_flat_even_when_partitioning_is_enabled(self, tmp_path):
        # _detect_file_type's patterns all require a literal .zip suffix,
        # so a bare .csv input always comes back "unknown" there and
        # never matches a partitioning.rules entry, regardless of what
        # its own name looks like.
        self._write_bare_csv(tmp_path / "raw", filename="1979.csv")
        cfg = _make_config(
            tmp_path, file_pattern="*.csv",
            partitioning={"enabled": True, "rules": [{"file_type": "yearly", "by": ["Year"]}]},
        )
        cfg["paths"]["parquet_historical_directory"] = str(tmp_path / "historical")

        outputs, failed = GDELTConverter(cfg).process_all_files()

        assert failed == []
        assert len(outputs) == 1
        assert (tmp_path / "parquet" / "1979.parquet").exists()
        assert list((tmp_path / "historical").rglob("*.parquet")) == []

    def test_process_single_file_called_directly_on_a_bare_csv(self, tmp_path):
        csv_path = self._write_bare_csv(tmp_path / "raw")
        converter = GDELTConverter(_make_config(tmp_path, file_pattern="*.csv"))

        outputs = converter.process_single_file(str(csv_path))

        assert len(outputs) == 1
        assert pl.read_parquet(outputs[0])["GlobalEventID"].to_list() == [1]


class TestForce:
    """force (CLI: --force) bypasses the .done marker check in
    process_all_files, so a zip already marked done is reprocessed and
    its output overwritten instead of skipped. Off by default."""

    def test_off_by_default_a_done_zip_is_skipped(self, tmp_path):
        _write_flat_zip(tmp_path / "raw")
        GDELTConverter(_make_config(tmp_path)).process_all_files()

        outputs, failed = GDELTConverter(_make_config(tmp_path)).process_all_files()

        assert failed == []
        assert outputs == []

    def test_force_reprocesses_a_zip_already_marked_done(self, tmp_path):
        zip_path = _write_flat_zip(tmp_path / "raw")
        GDELTConverter(_make_config(tmp_path)).process_all_files()

        outputs, failed = GDELTConverter(
            _make_config(tmp_path), force=True
        ).process_all_files()

        assert failed == []
        assert len(outputs) == 1
        assert Path(outputs[0]).exists()
        assert zip_path.exists()  # force alone does not imply delete_source


class TestDryRun:
    """dry_run (CLI: --dry-run) reports what would be converted without
    processing anything: no worker is submitted, no output is written,
    no .done marker is created. Off by default."""

    def test_dry_run_writes_nothing_and_marks_nothing_done(self, tmp_path):
        zip_path = _write_flat_zip(tmp_path / "raw")
        converter = GDELTConverter(_make_config(tmp_path), dry_run=True)

        outputs, failed = converter.process_all_files()

        assert outputs == []
        assert failed == []
        assert not converter._is_done(zip_path)
        assert list((tmp_path / "parquet").glob("*.parquet")) == []

    def test_dry_run_reports_the_would_be_processed_count_at_info(self, tmp_path, caplog):
        _write_flat_zip(tmp_path / "raw")

        with caplog.at_level("INFO", logger="gdeltforge.conversion.converter"):
            GDELTConverter(_make_config(tmp_path), dry_run=True).process_all_files()

        assert any(
            "[dry run] Would convert 1 zip file(s)" in r.message for r in caplog.records
        )

    def test_dry_run_sees_force_s_effect_on_the_skip_list(self, tmp_path, caplog):
        # A zip already marked done is invisible to a plain dry run (it
        # would be skipped for real too), but --force --dry-run together
        # must preview it as something that WOULD be reprocessed.
        _write_flat_zip(tmp_path / "raw")
        GDELTConverter(_make_config(tmp_path)).process_all_files()

        with caplog.at_level("INFO", logger="gdeltforge.conversion.converter"):
            outputs, failed = GDELTConverter(
                _make_config(tmp_path), dry_run=True
            ).process_all_files()
        assert outputs == []
        assert failed == []
        assert any("Nothing to convert" in r.message for r in caplog.records)

        caplog.clear()
        with caplog.at_level("INFO", logger="gdeltforge.conversion.converter"):
            GDELTConverter(_make_config(tmp_path), force=True, dry_run=True).process_all_files()

        assert any(
            "[dry run] Would convert 1 zip file(s)" in r.message for r in caplog.records
        )


class TestOrder:
    """order (CLI: --order) controls which zip is submitted to the worker
    pool first; verified through dry_run's own per-file preview log, the
    one place the resulting order is directly observable without mocking
    ProcessPoolExecutor's internal submission order too."""

    @staticmethod
    def _write_three_zips(tmp_path):
        _write_flat_zip(tmp_path / "raw", filename="20200601.export.CSV.zip")
        _write_flat_zip(tmp_path / "raw", filename="20200101.export.CSV.zip")
        _write_flat_zip(tmp_path / "raw", filename="20191231.export.CSV.zip")

    def test_default_order_is_ascending(self, tmp_path, caplog):
        self._write_three_zips(tmp_path)

        with caplog.at_level("DEBUG", logger="gdeltforge.conversion.converter"):
            GDELTConverter(_make_config(tmp_path), dry_run=True).process_all_files()

        would_convert = [
            r.message for r in caplog.records if r.message.startswith("[dry run]   ")
        ]
        assert would_convert == [
            "[dry run]   20191231.export.CSV.zip",
            "[dry run]   20200101.export.CSV.zip",
            "[dry run]   20200601.export.CSV.zip",
        ]

    def test_desc_orders_newest_first(self, tmp_path, caplog):
        self._write_three_zips(tmp_path)

        with caplog.at_level("DEBUG", logger="gdeltforge.conversion.converter"):
            GDELTConverter(_make_config(tmp_path), order="desc", dry_run=True).process_all_files()

        would_convert = [
            r.message for r in caplog.records if r.message.startswith("[dry run]   ")
        ]
        assert would_convert == [
            "[dry run]   20200601.export.CSV.zip",
            "[dry run]   20200101.export.CSV.zip",
            "[dry run]   20191231.export.CSV.zip",
        ]


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

        def boom(self, file, **kwargs):
            Path(file).write_bytes(b"partial write before a simulated crash")
            raise OSError("simulated crash mid-write")

        monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)

        df = pl.DataFrame({"GlobalEventID": [1], "Day": [20200101]})
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

        def boom(self, file, **kwargs):
            Path(file).write_bytes(b"partial write before a simulated crash")
            raise OSError("simulated crash mid-write")

        monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)

        df = pl.DataFrame({"GlobalEventID": [1], "Day": [20200101], "Year": [2020]})
        with pytest.raises(OSError):
            converter._save_historical_parquet(df, Path("2020.zip"), "yearly")

        out_path = tmp_path / "historical" / "Year=2020" / "2020.parquet"
        assert not out_path.exists()
        assert not out_path.with_name(out_path.name + ".tmp").exists()


_REDUCED_COLUMNS = [
    "Date", "Source", "Target", "CAMEOCode", "NumEvents", "NumArts",
    "QuadClass", "Goldstein", "SourceGeoType", "SourceGeoLat", "SourceGeoLong",
    "TargetGeoType", "TargetGeoLat", "TargetGeoLong", "ActionGeoType",
    "ActionGeoLat", "ActionGeoLong",
]
_REDUCED_NUMERIC_COLUMNS = [
    c for c in _REDUCED_COLUMNS if c not in ("Source", "Target", "CAMEOCode")
]


def _reduced_config(tmp_path, **converter_overrides):
    cfg = {
        "paths": {
            "event_reduced_downloaded_data_directory": str(tmp_path / "raw"),
            "event_reduced_unzipped_data_directory": str(tmp_path / "csv"),
            "event_reduced_parquet_data_directory": str(tmp_path / "parquet"),
            "event_reduced_parquet_historical_directory": str(tmp_path / "historical"),
        },
        "converter": {"keep_unzipped": False, "file_pattern": "*.zip"},
        "columns": {"gdelt_event_reduced": _REDUCED_COLUMNS},
        "columns_numeric": {"gdelt_event_reduced": _REDUCED_NUMERIC_COLUMNS},
    }
    cfg["converter"].update(converter_overrides)
    return cfg


def _write_reduced_zip(raw_dir, rows, filename="GDELT.MASTERREDUCEDV2.1979-2013.zip"):
    """A real header row followed by tab-separated data rows, matching
    GDELT.MASTERREDUCEDV2.1979-2013.zip's own single .TXT member (confirmed
    against the real file: a header line, no per-file date in the name at
    all, unlike every other dataset this converter handles)."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    txt_name = "GDELT.MASTERREDUCEDV2.1979-2013.txt"
    txt_path = raw_dir / txt_name
    txt_path.write_text("\t".join(_REDUCED_COLUMNS) + "\n" + "\n".join(rows) + "\n")
    zip_path = raw_dir / filename
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(txt_path, arcname=txt_name)
    txt_path.unlink()
    return zip_path


def _reduced_row(
    date, source="USA", target="GBR", cameo="010", num_events=5, num_arts=3, quad=1,
    goldstein=1.5,
):
    return "\t".join([
        date, source, target, cameo, str(num_events), str(num_arts), str(quad),
        str(goldstein), "1", "10.0", "-20.0", "1", "11.0", "-21.0", "1", "12.0", "-22.0",
    ])


class TestProcessReducedFile:
    """gdelt_event_reduced's dedicated conversion path: unlike every other
    dataset, its single file is too large to read whole (see
    _EVENT_REDUCED_CHUNK_SIZE) and carries no date in its filename, so its
    Year partition key has to come from the file's own Date column instead."""

    def test_partitions_rows_by_year_derived_from_the_date_column(self, tmp_path):
        zip_path = _write_reduced_zip(
            tmp_path / "raw",
            rows=[_reduced_row("19790101"), _reduced_row("20131231")],
        )
        converter = GDELTConverter(_reduced_config(tmp_path), dataset="gdelt_event_reduced")

        outputs = converter.process_reduced_file(str(zip_path))

        assert sorted(Path(p).relative_to(tmp_path / "historical").as_posix() for p in outputs) == [
            "Year=1979/GDELT.MASTERREDUCEDV2.1979-2013.part00000.parquet",
            "Year=2013/GDELT.MASTERREDUCEDV2.1979-2013.part00000.parquet",
        ]
        year_1979_dir = tmp_path / "historical" / "Year=1979"
        df_1979 = pl.read_parquet(
            year_1979_dir / "GDELT.MASTERREDUCEDV2.1979-2013.part00000.parquet"
        )
        assert df_1979["Date"].to_list() == [19790101]
        # Year is directory-only: it must never leak into the written columns.
        assert "Year" not in df_1979.columns
        assert "_Year" not in df_1979.columns

    def test_numeric_columns_are_cast_and_leading_zero_codes_stay_strings(self, tmp_path):
        zip_path = _write_reduced_zip(
            tmp_path / "raw",
            rows=[_reduced_row("19790101", cameo="043", goldstein=-3.5)],
        )
        converter = GDELTConverter(_reduced_config(tmp_path), dataset="gdelt_event_reduced")

        outputs = converter.process_reduced_file(str(zip_path))

        df = pl.read_parquet(outputs[0])
        assert df["Date"][0] == 19790101
        assert df["NumEvents"][0] == 5
        assert df["Goldstein"][0] == -3.5
        # CAMEOCode's leading zero must survive: it's excluded from
        # columns_numeric on purpose.
        assert df["CAMEOCode"][0] == "043"

    def test_unparseable_date_is_dropped_with_a_warning(self, tmp_path, caplog):
        zip_path = _write_reduced_zip(
            tmp_path / "raw",
            rows=[_reduced_row("19790101"), _reduced_row("notadate")],
        )
        converter = GDELTConverter(_reduced_config(tmp_path), dataset="gdelt_event_reduced")

        with caplog.at_level("WARNING", logger="gdeltforge.conversion.converter"):
            outputs = converter.process_reduced_file(str(zip_path))

        assert len(outputs) == 1
        df = pl.read_parquet(outputs[0])
        assert df["Date"].to_list() == [19790101]
        assert any("dropping 1 row" in r.message for r in caplog.records)

    def test_reruns_overwrite_the_same_deterministic_part_files(self, tmp_path):
        zip_path = _write_reduced_zip(
            tmp_path / "raw",
            rows=[_reduced_row("19790101")],
        )
        converter = GDELTConverter(_reduced_config(tmp_path), dataset="gdelt_event_reduced")
        converter.process_reduced_file(str(zip_path))

        # A second, independent extraction (e.g. --force) must overwrite the
        # same part filenames rather than accumulating new ones alongside them.
        zip_path = _write_reduced_zip(
            tmp_path / "raw",
            rows=[_reduced_row("19790101"), _reduced_row("19790615")],
        )
        outputs = converter.process_reduced_file(str(zip_path))

        assert len(outputs) == 1
        df = pl.read_parquet(outputs[0])
        assert df["Date"].to_list() == [19790101, 19790615]

    def test_chunked_read_still_partitions_correctly_across_chunk_boundaries(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(converter_module, "_EVENT_REDUCED_CHUNK_SIZE", 1)
        zip_path = _write_reduced_zip(
            tmp_path / "raw",
            rows=[_reduced_row("19790101"), _reduced_row("19790615"), _reduced_row("20131231")],
        )
        converter = GDELTConverter(_reduced_config(tmp_path), dataset="gdelt_event_reduced")

        outputs = converter.process_reduced_file(str(zip_path))

        # 3 rows at chunk_size=1 is 3 chunks; two land in Year=1979, each its
        # own part file since each chunk is written independently.
        assert sorted(Path(p).name for p in outputs) == [
            "GDELT.MASTERREDUCEDV2.1979-2013.part00000.parquet",
            "GDELT.MASTERREDUCEDV2.1979-2013.part00001.parquet",
            "GDELT.MASTERREDUCEDV2.1979-2013.part00002.parquet",
        ]
        all_dates = sorted(
            date for p in outputs for date in pl.read_parquet(p)["Date"].to_list()
        )
        assert all_dates == [19790101, 19790615, 20131231]

    def test_rejects_output_columns_that_exclude_date(self, tmp_path):
        cfg = _reduced_config(
            tmp_path, output_columns={"gdelt_event_reduced": ["Source", "Target"]}
        )

        with pytest.raises(ValueError, match="Date"):
            GDELTConverter(cfg, dataset="gdelt_event_reduced")

    def test_historical_folder_is_required_even_when_partitioning_is_disabled(self, tmp_path):
        cfg = _reduced_config(tmp_path)
        del cfg["paths"]["event_reduced_parquet_historical_directory"]
        # partitioning.enabled isn't set at all here, matching the default:
        # gdelt_event_reduced must still require its historical directory.

        with pytest.raises(ValueError, match="historical"):
            GDELTConverter(cfg, dataset="gdelt_event_reduced")

    def test_process_all_files_dispatches_to_the_reduced_worker(self, tmp_path):
        _write_reduced_zip(tmp_path / "raw", rows=[_reduced_row("19790101")])
        converter = GDELTConverter(_reduced_config(tmp_path), dataset="gdelt_event_reduced")

        outputs, failed = converter.process_all_files()

        assert failed == []
        assert len(outputs) == 1
        assert "Year=1979" in outputs[0]
