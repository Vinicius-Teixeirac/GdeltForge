import pytest

from gdeltforge.conversion.converter import GDELTConverter


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
