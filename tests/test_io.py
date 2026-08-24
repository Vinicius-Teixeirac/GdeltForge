import logging
from pathlib import Path

import pandas as pd
import pytest

from gdeltforge.utils.io import (
    config_fingerprint,
    delete_done_marker,
    is_marked_done,
    mark_done,
    read_parquet_path,
    warn_if_delete_source_drops_recoverable_data,
    write_dataframe_atomic,
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


class TestWriteDataframeAtomic:
    """write_dataframe_atomic generalizes write_parquet_atomic to
    sample/crossref's --export-format. export_format="parquet" (the
    default) delegates straight to write_parquet_atomic; export_format=
    "csv" is new code with its own atomic tmp-then-rename coverage,
    mirroring TestWriteParquetAtomic's own shape above."""

    def test_parquet_delegates_to_write_parquet_atomic(self, tmp_path):
        out = tmp_path / "sample.parquet"
        df = pd.DataFrame({"GlobalEventID": [1, 2, 3]})

        write_dataframe_atomic(df, out, export_format="parquet")

        assert out.exists()
        assert pd.read_parquet(out)["GlobalEventID"].tolist() == [1, 2, 3]
        assert not (tmp_path / "sample.parquet.tmp").exists()

    def test_csv_writes_a_real_readable_file(self, tmp_path):
        out = tmp_path / "sample.csv"
        df = pd.DataFrame({"GlobalEventID": [1, 2, 3], "QuadClass": [1, 2, 3]})

        write_dataframe_atomic(df, out, export_format="csv")

        assert out.exists()
        result = pd.read_csv(out)
        assert result["GlobalEventID"].tolist() == [1, 2, 3]
        assert result["QuadClass"].tolist() == [1, 2, 3]
        assert not (tmp_path / "sample.csv.tmp").exists()

    def test_csv_writes_without_a_pandas_index_column(self, tmp_path):
        out = tmp_path / "sample.csv"
        df = pd.DataFrame({"GlobalEventID": [1, 2, 3]})

        write_dataframe_atomic(df, out, export_format="csv")

        assert list(pd.read_csv(out).columns) == ["GlobalEventID"]

    def test_csv_warns_and_overwrites_leftover_tmp_from_interrupted_run(self, tmp_path, caplog):
        out = tmp_path / "sample.csv"
        tmp_path_leftover = tmp_path / "sample.csv.tmp"
        tmp_path_leftover.write_bytes(b"partial garbage from a killed run")

        df = pd.DataFrame({"GlobalEventID": [1, 2, 3]})
        with caplog.at_level(logging.WARNING):
            write_dataframe_atomic(df, out, export_format="csv")

        assert "leftover incomplete file" in caplog.text
        assert pd.read_csv(out)["GlobalEventID"].tolist() == [1, 2, 3]
        assert not tmp_path_leftover.exists()

    def test_csv_cleans_up_tmp_and_reraises_on_write_failure(self, tmp_path, monkeypatch):
        out = tmp_path / "sample.csv"

        def boom(self, path, *args, **kwargs):
            Path(path).write_bytes(b"partial write before failure")
            raise OSError("disk full")

        monkeypatch.setattr(pd.DataFrame, "to_csv", boom)

        with pytest.raises(OSError):
            write_dataframe_atomic(pd.DataFrame({"a": [1]}), out, export_format="csv")

        assert not out.exists()
        assert not (tmp_path / "sample.csv.tmp").exists()

    def test_unsupported_format_raises_clearly(self, tmp_path):
        out = tmp_path / "sample.json"
        with pytest.raises(ValueError, match="Unsupported export format: 'json'"):
            write_dataframe_atomic(pd.DataFrame({"a": [1]}), out, export_format="json")

        assert not out.exists()


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


class TestDeleteDoneMarker:
    """--delete-source deletes the source zip/parquet but used to leave
    its .done marker behind: the marker is written next to the source,
    not the output, and a deleted source can never be found by
    process_all_files'/filter_all_files' own glob again on a later run,
    so the marker becomes permanently vestigial the instant its source
    is gone, just an orphaned file accumulating in a directory
    --delete-source's whole point was to shrink."""

    def test_removes_an_existing_marker(self, tmp_path):
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")
        mark_done(src, "fp-1")
        assert (tmp_path / "20200101.zip.done").exists()

        delete_done_marker(src)

        assert not (tmp_path / "20200101.zip.done").exists()

    def test_no_marker_present_is_not_an_error(self, tmp_path):
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")

        delete_done_marker(src)  # should not raise


class TestWarnIfDeleteSourceDropsRecoverableData:
    """Core logic shared by convert.py's run_converter and filter.py's
    run_filter; each module's own tests only need to prove they call this
    with the right arguments, not re-verify the logic itself."""

    def test_warns_when_delete_source_and_narrowing_are_both_active(self, caplog):
        with caplog.at_level(logging.WARNING):
            warn_if_delete_source_drops_recoverable_data(
                logging.getLogger("test"), "filter", True, narrowing=["columns_to_check"]
            )
        assert any(
            "columns_to_check" in r.message and "filter" in r.message for r in caplog.records
        )

    def test_no_warning_when_delete_source_is_false(self, caplog):
        with caplog.at_level(logging.WARNING):
            warn_if_delete_source_drops_recoverable_data(
                logging.getLogger("test"), "filter", False, narrowing=["columns_to_check"]
            )
        assert caplog.records == []

    def test_no_warning_when_nothing_narrows_the_output(self, caplog):
        with caplog.at_level(logging.WARNING):
            warn_if_delete_source_drops_recoverable_data(
                logging.getLogger("test"), "filter", True, narrowing=[]
            )
        assert caplog.records == []

    def test_lists_every_active_narrowing_setting(self, caplog):
        with caplog.at_level(logging.WARNING):
            warn_if_delete_source_drops_recoverable_data(
                logging.getLogger("test"), "filter", True,
                narrowing=["columns_to_check", "output_columns", "float32_columns"],
            )
        message = caplog.records[0].message
        assert "columns_to_check" in message
        assert "output_columns" in message
        assert "float32_columns" in message
