import zipfile
from pathlib import Path

import pandas as pd
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
