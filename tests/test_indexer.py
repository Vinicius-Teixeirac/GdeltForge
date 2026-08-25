import numpy as np
import pandas as pd
import pytest

from gdeltforge.sampling.indexer import FileIndex


def _write_parquet(path, n_rows, start_id=0):
    df = pd.DataFrame({"GlobalEventID": range(start_id, start_id + n_rows)})
    df.to_parquet(path)


class TestFileIndexBuild:
    def test_total_rows_and_counts(self, tmp_path):
        _write_parquet(tmp_path / "a.parquet", 5)
        _write_parquet(tmp_path / "b.parquet", 3)
        _write_parquet(tmp_path / "c.parquet", 7)

        index = FileIndex(tmp_path)

        assert index.total_rows == 15
        assert len(index.files) == 3
        assert sorted(index.counts) == [3, 5, 7]

    def test_empty_folder_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            FileIndex(tmp_path)

    def test_accepts_explicit_file_list(self, tmp_path):
        f1 = tmp_path / "x.parquet"
        f2 = tmp_path / "y.parquet"
        _write_parquet(f1, 4)
        _write_parquet(f2, 6)

        index = FileIndex([f1, f2])

        assert index.total_rows == 10

    def test_a_corrupt_parquet_file_raises_a_clear_error_not_a_bare_arrow_one(
        self, tmp_path
    ):
        # Depending on where the corrupt file lands in the glob order,
        # this can surface either at dataset construction (schema
        # inference reads at least the first file) or at the later
        # fragment.metadata access; both are wrapped in clearer_dataset_
        # errors, so either way this raises the same clear RuntimeError.
        _write_parquet(tmp_path / "a.parquet", 5)
        (tmp_path / "b.parquet").write_text("not actually parquet content")

        with pytest.raises(RuntimeError, match=r"reading 2 parquet file\(s\)"):
            FileIndex(tmp_path)


class TestLookup:
    def test_resolves_correct_file_and_relative_row(self, tmp_path):
        _write_parquet(tmp_path / "file_a.parquet", 5)  # global rows 0-4
        _write_parquet(tmp_path / "file_b.parquet", 3)  # global rows 5-7

        index = FileIndex(tmp_path)

        assert index.lookup(0) == (0, 0)
        assert index.lookup(4) == (0, 4)
        assert index.lookup(5) == (1, 0)
        assert index.lookup(7) == (1, 2)


class TestGroupIndicesByFile:
    def test_groups_spanning_multiple_files(self, tmp_path):
        _write_parquet(tmp_path / "file_a.parquet", 5)  # global rows 0-4
        _write_parquet(tmp_path / "file_b.parquet", 5)  # global rows 5-9

        index = FileIndex(tmp_path)
        result = index.group_indices_by_file(np.array([0, 2, 4, 5, 8, 9]))

        assert len(result) == 2
        (_, rows_a), (_, rows_b) = sorted(result.items())
        assert rows_a == [0, 2, 4]
        assert rows_b == [0, 3, 4]
