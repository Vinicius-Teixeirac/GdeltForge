import logging
from pathlib import Path

import pandas as pd
import pytest

from gdeltforge.utils.io import (
    config_fingerprint,
    is_marked_done,
    mark_done,
    read_parquet_path,
    write_parquet_atomic,
)


class TestWriteParquetAtomic:
    def test_writes_file_and_leaves_no_tmp_behind(self, tmp_path):
        out = tmp_path / "sample.parquet"
        df = pd.DataFrame({"GlobalEventID": [1, 2, 3]})

        write_parquet_atomic(df, out)

        assert out.exists()
        assert pd.read_parquet(out)["GlobalEventID"].tolist() == [1, 2, 3]
        assert not (tmp_path / "sample.parquet.tmp").exists()

    def test_warns_and_overwrites_leftover_tmp_from_interrupted_run(self, tmp_path, caplog):
        out = tmp_path / "sample.parquet"
        tmp_path_leftover = tmp_path / "sample.parquet.tmp"
        tmp_path_leftover.write_bytes(b"partial garbage from a killed run")

        df = pd.DataFrame({"GlobalEventID": [1, 2, 3]})
        with caplog.at_level(logging.WARNING):
            write_parquet_atomic(df, out)

        assert "leftover incomplete file" in caplog.text
        assert pd.read_parquet(out)["GlobalEventID"].tolist() == [1, 2, 3]
        assert not tmp_path_leftover.exists()

    def test_extra_kwargs_are_passed_through_to_to_parquet(self, tmp_path, monkeypatch):
        out = tmp_path / "sample.parquet"
        captured = {}

        real_to_parquet = pd.DataFrame.to_parquet

        def spy(self, path, **kwargs):
            captured.update(kwargs)
            return real_to_parquet(self, path, **kwargs)

        monkeypatch.setattr(pd.DataFrame, "to_parquet", spy)

        write_parquet_atomic(
            pd.DataFrame({"a": [1]}), out, engine="pyarrow", compression="snappy", index=False,
        )

        assert captured == {"engine": "pyarrow", "compression": "snappy", "index": False}

    def test_cleans_up_tmp_and_reraises_on_write_failure(self, tmp_path, monkeypatch):
        out = tmp_path / "sample.parquet"

        def boom(self, path, *args, **kwargs):
            Path(path).write_bytes(b"partial write before failure")
            raise OSError("disk full")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

        with pytest.raises(OSError):
            write_parquet_atomic(pd.DataFrame({"a": [1]}), out)

        assert not out.exists()
        assert not (tmp_path / "sample.parquet.tmp").exists()


class TestReadParquetPath:
    def test_reads_a_single_file_directly(self, tmp_path):
        f = tmp_path / "sample.parquet"
        pd.DataFrame({"GlobalEventID": [1, 2, 3]}).to_parquet(f)

        result = read_parquet_path(f)

        assert result["GlobalEventID"].tolist() == [1, 2, 3]

    def test_reads_every_parquet_file_in_a_directory(self, tmp_path):
        pd.DataFrame({"GlobalEventID": [1, 2]}).to_parquet(tmp_path / "a.parquet")
        pd.DataFrame({"GlobalEventID": [3, 4, 5]}).to_parquet(tmp_path / "b.parquet")

        result = read_parquet_path(tmp_path)

        assert sorted(result["GlobalEventID"].tolist()) == [1, 2, 3, 4, 5]

    def test_ignores_done_resumability_markers_in_a_directory(self, tmp_path):
        # The real bug: convert/filter's own .done markers (mark_done above
        # writes <name>.done as a real sibling of the data) sit in exactly
        # these directories by design, and pandas' own directory read has
        # no notion of that convention, so it tries to parse the marker as
        # a parquet file and fails. A directory pointed at real convert/
        # filter output always has these; this must not choke on them.
        f = tmp_path / "20260811.export.parquet"
        pd.DataFrame({"GlobalEventID": [1, 2]}).to_parquet(f)
        mark_done(f, "some-fingerprint")
        assert (tmp_path / "20260811.export.parquet.done").exists()

        result = read_parquet_path(tmp_path)

        assert result["GlobalEventID"].tolist() == [1, 2]

    def test_empty_directory_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No parquet files"):
            read_parquet_path(tmp_path)

    def test_directory_of_only_done_markers_raises_file_not_found(self, tmp_path):
        # A directory can genuinely have markers with no real data left,
        # e.g. every source file got removed after conversion; this must
        # not silently return an empty-looking success either.
        (tmp_path / "20260811.export.parquet.done").write_text("fingerprint")

        with pytest.raises(FileNotFoundError, match="No parquet files"):
            read_parquet_path(tmp_path)


class TestConfigFingerprint:
    def test_same_fields_in_different_kwarg_order_produce_the_same_string(self):
        a = config_fingerprint(columns_to_check=["X"], output_columns=None)
        b = config_fingerprint(output_columns=None, columns_to_check=["X"])

        assert a == b

    def test_a_reordered_list_produces_the_same_string(self):
        a = config_fingerprint(columns_to_check=["A", "B", "C"])
        b = config_fingerprint(columns_to_check=["C", "A", "B"])

        assert a == b

    def test_a_changed_list_membership_produces_a_different_string(self):
        a = config_fingerprint(columns_to_check=["A", "B"])
        b = config_fingerprint(columns_to_check=["A", "C"])

        assert a != b

    def test_none_is_distinct_from_an_empty_list(self):
        a = config_fingerprint(output_columns=None)
        b = config_fingerprint(output_columns=[])

        assert a != b

    def test_a_scalar_value_is_rendered_directly(self):
        a = config_fingerprint(compression="zstd")
        b = config_fingerprint(compression="snappy")

        assert a != b
        assert "zstd" in a


class TestDoneMarker:
    def test_a_file_with_no_marker_is_not_done(self, tmp_path):
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")

        assert not is_marked_done(src, "fp-1")

    def test_marking_done_makes_it_done_under_the_same_fingerprint(self, tmp_path):
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")

        mark_done(src, "fp-1")

        assert is_marked_done(src, "fp-1")
        assert (tmp_path / "20200101.zip.done").read_text() == "fp-1"

    def test_a_marker_from_a_different_fingerprint_is_not_done(self, tmp_path):
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")

        mark_done(src, "fp-old")

        assert not is_marked_done(src, "fp-new")

    def test_a_preexisting_empty_marker_is_not_done(self, tmp_path):
        # Regression guard for the pre-fingerprint marker format (an empty
        # touch()ed file): must be treated as not-done under the new
        # content-comparison scheme, forcing one harmless reprocess rather
        # than silently trusting a marker that predates fingerprinting.
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")
        (tmp_path / "20200101.zip.done").touch()

        assert not is_marked_done(src, "fp-1")
