import zipfile

import pandas as pd
import pytest

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
            ("GDELT.MASTERREDUCEDV2.1979-2013.zip", "unknown"),
        ],
    )
    def test_cases(self, converter, filename, expected):
        assert converter._detect_file_type(filename) == expected


class TestMaxWorkersConfig:
    def test_defaults_to_none_so_executor_uses_cpu_count(self, tmp_path):
        converter = GDELTConverter(_make_config(tmp_path))
        assert converter.max_workers is None

    def test_explicit_value_is_respected(self, tmp_path):
        converter = GDELTConverter(_make_config(tmp_path, max_workers=2))
        assert converter.max_workers == 2


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
    """A real ZIP containing one tab-separated GKG 1.0-shaped row, including
    a semicolon-delimited list field (EventIds): the kind of value that
    would silently corrupt if the converter's dataset generalization ever
    started assuming a single-value column instead of treating it as an
    opaque string."""
    csv_path = tmp_path / "20130401.gkg.csv"
    row = "\t".join([
        "20130401", "5", "KILL#10#1", "TAX_FNCACT;GENERAL_GOVERNMENT",
        "123456;789012",
    ])
    csv_path.write_text(row + "\n")

    zip_path = tmp_path / filename
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(csv_path, arcname="20130401.gkg.csv")
    return zip_path


class TestConvertsNonEventsSchemaEndToEnd:
    """Regression coverage for the actual CSV -> Parquet conversion path on
    a non-Events schema, not just constructor wiring (see
    TestDatasetParameter above): tab-split, column naming, and numeric
    coercion all need to work correctly against a real GKG-shaped row, and
    a semicolon-delimited field must survive as a raw string rather than
    being coerced or truncated."""

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
        assert list(df.columns) == ["Date", "NumArticles", "Counts", "Themes", "EventIds"]
        assert df["Date"].iloc[0] == 20130401
        assert df["NumArticles"].iloc[0] == 5
        # Numeric coercion must be scoped to columns_numeric only: EventIds
        # is a semicolon list, not a scalar, and must survive untouched.
        assert df["EventIds"].iloc[0] == "123456;789012"
        assert df["Themes"].iloc[0] == "TAX_FNCACT;GENERAL_GOVERNMENT"

    def test_run_converter_wrapper_processes_a_non_events_dataset(self, tmp_path):
        cfg = self._config(tmp_path)
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_gkg_v1_zip(raw_dir)

        outputs, failed = run_converter(cfg, dataset="gdelt_gkg_v1")

        assert failed == []
        assert len(outputs) == 1
        assert pd.read_parquet(outputs[0])["EventIds"].iloc[0] == "123456;789012"
