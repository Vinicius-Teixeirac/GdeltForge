import logging
import os
import threading
import time
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gdeltforge.sampling import indexer as indexer_module
from gdeltforge.sampling.indexer import INDEX_SUFFIX, FileIndex, index_path_for
from gdeltforge.sampling.samplers import CalendarSampler, FilteredSampler, IndexedSampler


def _write_parquet(path, n_rows, start_id=0):
    df = pl.DataFrame({"GlobalEventID": range(start_id, start_id + n_rows)})
    df.write_parquet(path)


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


def _write_days(folder, days=("20200101", "20200102", "20200103"), rows=50):
    """One events-like file per day, several row groups each."""
    folder.mkdir(parents=True, exist_ok=True)
    for k, day in enumerate(days):
        pl.DataFrame({
            "GlobalEventID": range(k * rows, (k + 1) * rows),
            "Day": [int(day)] * rows,
            "QuadClass": [(i % 4) + 1 for i in range(rows)],
        }).write_parquet(folder / f"{day}.export.parquet", row_group_size=16)


def _footers_read(monkeypatch):
    """Record every footer FileIndex reads."""
    read = []
    original = indexer_module._read_footer

    def spy(f):
        read.append(Path(f).name)
        return original(f)

    monkeypatch.setattr(indexer_module, "_read_footer", spy)
    return read


class TestSavedIndex:
    def test_a_second_index_reads_no_footer_and_matches_the_first(self, tmp_path, monkeypatch):
        data = tmp_path / "data"
        _write_days(data)
        saved = tmp_path / "index" / "events.index"
        read = _footers_read(monkeypatch)

        first = FileIndex(data, saved=saved)
        assert sorted(read) == sorted(f.name for f in data.glob("*.parquet"))
        assert saved.exists()

        read.clear()
        second = FileIndex(data, saved=saved)
        assert read == []
        assert second.files == first.files
        assert second.counts == first.counts == [50, 50, 50]
        assert second.row_groups == first.row_groups
        assert second.schemas == first.schemas
        assert second.schema_names == first.schema_names

    def test_row_groups_and_schemas_are_the_files_own(self, tmp_path):
        data = tmp_path / "data"
        _write_days(data)
        index = FileIndex(data, saved=tmp_path / "x.index")
        for f in index.files:
            metadata = pq.ParquetFile(f).metadata
            assert index.row_groups[f] == tuple(
                metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)
            )
            assert index.schemas[f] == dict(pl.read_parquet_schema(f).items())
        assert len(index.row_groups[index.files[0]]) == 4  # 50 rows, groups of 16

    def test_only_a_changed_or_new_file_is_read_again(self, tmp_path, monkeypatch):
        data = tmp_path / "data"
        _write_days(data)
        saved = tmp_path / "events.index"
        FileIndex(data, saved=saved)

        changed = data / "20200102.export.parquet"
        pl.DataFrame(
            {"GlobalEventID": range(7), "Day": [20200102] * 7, "QuadClass": [1] * 7}
        ).write_parquet(changed)
        stat = changed.stat()
        os.utime(changed, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000_000))
        _write_days(data, days=("20200104",), rows=5)

        read = _footers_read(monkeypatch)
        index = FileIndex(data, saved=saved)
        assert sorted(read) == ["20200102.export.parquet", "20200104.export.parquet"]
        assert index.counts == [50, 7, 50, 5]

        read.clear()
        FileIndex(data, saved=saved)
        assert read == []

    def test_files_with_the_same_columns_share_one_schema_object(self, tmp_path):
        # A cleaned file's own metadata names its source, so every file's
        # footer metadata differs; the schemas are still shared.
        data = tmp_path / "data"
        data.mkdir()
        for day in ("20200101", "20200102"):
            table = pa.table({"GlobalEventID": [1, 2]}).replace_schema_metadata(
                {"gdeltforge:clean": day}
            )
            pq.write_table(table, data / f"{day}.export.parquet")
        saved = tmp_path / "events.index"
        for index in (FileIndex(data, saved=saved), FileIndex(data, saved=saved)):
            a, b = (index.schemas[f] for f in index.files)
            assert a is b

    def test_schema_names_cover_every_file(self, tmp_path):
        data = tmp_path / "data"
        data.mkdir()
        pl.DataFrame({"a": [1]}).write_parquet(data / "1.parquet")
        pl.DataFrame({"a": [1], "b": ["x"]}).write_parquet(data / "2.parquet")
        assert FileIndex(data).schema_names == ["a", "b"]

    def test_an_unreadable_saved_index_is_rebuilt_with_a_warning(self, tmp_path, caplog):
        data = tmp_path / "data"
        _write_days(data)
        saved = tmp_path / "events.index"
        saved.write_text("not an index")

        with caplog.at_level(logging.WARNING):
            index = FileIndex(data, saved=saved)
        assert "Could not read the file index" in caplog.text
        assert index.total_rows == 150
        assert pq.read_table(saved).num_rows == 3

    def test_a_saved_index_of_another_version_is_rebuilt(self, tmp_path, monkeypatch, caplog):
        data = tmp_path / "data"
        _write_days(data)
        saved = tmp_path / "events.index"
        monkeypatch.setattr(indexer_module, "_INDEX_VERSION", "0")
        FileIndex(data, saved=saved)
        monkeypatch.undo()

        read = _footers_read(monkeypatch)
        with caplog.at_level(logging.WARNING):
            FileIndex(data, saved=saved)
        assert "has version '0'" in caplog.text
        assert len(read) == 3

    def test_a_failed_save_warns_and_the_index_still_works(self, tmp_path, monkeypatch, caplog):
        data = tmp_path / "data"
        _write_days(data)

        def full_disk(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(indexer_module.pq, "write_table", full_disk)
        saved = tmp_path / "events.index"
        with caplog.at_level(logging.WARNING):
            index = FileIndex(data, saved=saved)
        assert "Could not save the file index" in caplog.text
        assert index.total_rows == 150
        assert not saved.exists()
        assert list(tmp_path.glob("*.tmp")) == []

    def test_a_saved_index_inside_a_data_directory_is_refused(self, tmp_path):
        data = tmp_path / "data"
        _write_days(data)
        with pytest.raises(ValueError, match="outside every data directory"):
            FileIndex(data, saved=data / "events.index")
        with pytest.raises(ValueError, match="outside every data directory"):
            index_path_for(data / "index", [tmp_path / "other", data])

    def test_index_path_for_differs_per_set_of_roots(self, tmp_path):
        a, b = tmp_path / "events" / "cleaned", tmp_path / "gkg_v2" / "cleaned"
        hist = tmp_path / "events" / "cleaned_historical"
        paths = {
            index_path_for(tmp_path / "index", roots)
            for roots in ([a], [b], [a, hist])
        }
        assert len(paths) == 3
        assert all(p.parent == tmp_path / "index" for p in paths)
        assert all(p.name.startswith(("events_cleaned-", "gkg_v2_cleaned-")) for p in paths)
        assert all(p.suffix == INDEX_SUFFIX for p in paths)

    def test_footer_reads_stay_under_the_scan_limit(self, tmp_path, monkeypatch):
        data = tmp_path / "data"
        _write_days(data, days=tuple(f"202001{d:02d}" for d in range(1, 21)), rows=3)
        monkeypatch.setenv("POLARS_MAX_CONCURRENT_SCANS", "2")
        lock = threading.Lock()
        in_flight, peak = 0, 0
        original = indexer_module._read_footer

        def slow(f):
            nonlocal in_flight, peak
            with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            time.sleep(0.01)
            try:
                return original(f)
            finally:
                with lock:
                    in_flight -= 1

        monkeypatch.setattr(indexer_module, "_read_footer", slow)
        FileIndex(data)
        assert peak == 2


class TestSamplersWithASavedIndex:
    """Every sampler draws the same rows with a saved index as without,
    and reads no footer on a second run over unchanged files."""

    def _draws(self, data, saved):
        indexed = IndexedSampler(
            str(data), random_state=7, file_index=saved
        ).get_random_sample(40)
        calendar = CalendarSampler(
            str(data), random_state=7, file_index=saved
        ).get_calendar_samples(5)
        filtered = FilteredSampler(
            str(data), gdelt_columns=["GlobalEventID", "Day", "QuadClass"],
            filter_dict={"QuadClass": [1, 2]}, random_state=7, file_index=saved,
        )
        return (
            indexed, calendar, filtered.get_random_sample(20),
            filtered.get_stratified_sample("QuadClass", 3),
        )

    def test_same_rows_with_and_without_and_no_footer_read_twice(self, tmp_path, monkeypatch):
        data = tmp_path / "data"
        _write_days(data)
        without = self._draws(data, None)
        saved = tmp_path / "events.index"
        first = self._draws(data, saved)
        read = _footers_read(monkeypatch)
        second = self._draws(data, saved)

        assert read == []
        for a, b, c in zip(without, first, second, strict=True):
            assert a.equals(b) and b.equals(c)
