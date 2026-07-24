import pytest

from conversion.converter import GDELTConverter


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
        "columns_numeric": ["GlobalEventID", "Day"],
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
