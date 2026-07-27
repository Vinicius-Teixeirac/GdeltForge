import pandas as pd

from gdeltforge.filtering.filter import GDELTFilter


def _write_parquet(path, data):
    pd.DataFrame(data).to_parquet(path)


class TestFilterSingleFile:
    def test_drops_rows_with_nan_in_checked_columns(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {
            "GlobalEventID": [1, 2, 3, 4],
            "Actor1Name": ["A", None, "C", "D"],
            "QuadClass": [1, 2, None, 4],
        })

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["Actor1Name", "QuadClass"])
        out_path = tmp_path / "out" / "data_filtered.parquet"
        rows_before, rows_after = filt.filter_single_file(src, out_path)

        assert rows_before == 4
        assert rows_after == 2  # rows 2 and 3 each have one NaN in a checked column

        result = pd.read_parquet(out_path)
        assert sorted(result["GlobalEventID"].tolist()) == [1, 4]

    def test_missing_columns_are_skipped_not_fatal(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, None]})

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass", "DoesNotExist"])
        rows_before, rows_after = filt.filter_single_file(src, tmp_path / "out" / "o.parquet")

        # Only QuadClass (the column that actually exists) is enforced.
        assert rows_before == 2
        assert rows_after == 1

    def test_empty_file_returns_zero_zero(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "empty.parquet"
        _write_parquet(src, {"GlobalEventID": pd.Series([], dtype="int64")})

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["GlobalEventID"])
        rows_before, rows_after = filt.filter_single_file(src, tmp_path / "out" / "o.parquet")

        assert (rows_before, rows_after) == (0, 0)


class TestFilterAllFiles:
    def test_aggregates_across_flat_files(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, None]})
        _write_parquet(
            input_dir / "b.parquet", {"GlobalEventID": [3, 4, 5], "QuadClass": [1, 2, 3]}
        )

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        processed, failed = filt.filter_all_files()

        assert processed == 2
        assert failed == 0

    def test_counts_a_corrupt_file_as_failed(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        (input_dir / "bad.parquet").write_bytes(b"not a real parquet file")

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        processed, failed = filt.filter_all_files()

        assert processed == 0
        assert failed == 1

    def test_preserves_historical_directory_structure(self, tmp_path):
        flat_in = tmp_path / "flat_in"
        hist_in = tmp_path / "hist_in"
        flat_in.mkdir()
        hist_in.mkdir()

        part_dir = hist_in / "Year=1979"
        part_dir.mkdir()
        _write_parquet(part_dir / "1979.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTFilter(
            str(flat_in), str(tmp_path / "flat_out"), ["QuadClass"],
            historical_input_folder=str(hist_in),
            historical_output_folder=str(tmp_path / "hist_out"),
        )
        processed, failed = filt.filter_all_files()

        assert (processed, failed) == (1, 0)
        assert (tmp_path / "hist_out" / "Year=1979" / "1979_filtered.parquet").exists()


class TestValidateColumns:
    def test_reports_existing_and_missing_columns(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1], "QuadClass": [1]})

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass", "Nope"])
        result = filt.validate_columns()

        assert result["existing_columns"] == ["QuadClass"]
        assert result["missing_columns"] == ["Nope"]

    def test_no_files_returns_error(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        result = filt.validate_columns()

        assert "error" in result
