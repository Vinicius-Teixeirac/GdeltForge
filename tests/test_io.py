import logging
from pathlib import Path

import pandas as pd
import pytest

from gdeltforge.utils.io import (
    clearer_dataset_errors,
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
        # writes them as a dot-prefixed sibling of the data) sit in exactly
        # these directories by design. Now dot-prefixed specifically so a
        # bare pandas/pyarrow directory read skips them on its own; this
        # explicit glob is a second layer on top of that, not the only
        # thing preventing the marker from being parsed as a parquet file.
        f = tmp_path / "20260811.export.parquet"
        pd.DataFrame({"GlobalEventID": [1, 2]}).to_parquet(f)
        mark_done(f, "some-fingerprint")
        assert (tmp_path / ".20260811.export.parquet.done").exists()

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
        assert (tmp_path / ".20200101.zip.done").read_text() == "fp-1"

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
        # Also exercises the legacy (non-dot-prefixed) marker path below,
        # since this old-format marker was never dot-prefixed either.
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")
        (tmp_path / "20200101.zip.done").touch()

        assert not is_marked_done(src, "fp-1")

    def test_a_legacy_non_dot_prefixed_marker_is_still_recognized(self, tmp_path):
        # Real installations already have markers written under the old,
        # non-dot-prefixed name; upgrading gdeltforge must not make every
        # already-processed file look undone and force a mass reprocess.
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")
        (tmp_path / "20200101.zip.done").write_text("fp-1")

        assert is_marked_done(src, "fp-1")

    def test_a_legacy_marker_is_migrated_to_the_dot_prefixed_name(self, tmp_path):
        # The first is_marked_done check after upgrading should clean the
        # old marker up rather than leaving it (and its eventual new
        # sibling) both present forever.
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")
        legacy = tmp_path / "20200101.zip.done"
        legacy.write_text("fp-1")

        assert is_marked_done(src, "fp-1")

        assert not legacy.exists()
        assert (tmp_path / ".20200101.zip.done").read_text() == "fp-1"

    def test_a_legacy_marker_with_a_stale_fingerprint_is_not_done_and_not_migrated(
        self, tmp_path
    ):
        # A legacy marker from a differently-configured run must still
        # force reprocessing, the same as a current-format one would --
        # migration only happens on an actual match.
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")
        legacy = tmp_path / "20200101.zip.done"
        legacy.write_text("fp-old")

        assert not is_marked_done(src, "fp-new")

        assert legacy.exists()
        assert not (tmp_path / ".20200101.zip.done").exists()

    def test_a_dot_prefixed_marker_takes_priority_over_a_legacy_one(self, tmp_path):
        # If both happen to exist (e.g. mid-migration), the current-format
        # marker is authoritative; the legacy one is never even read.
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")
        (tmp_path / "20200101.zip.done").write_text("fp-old")
        (tmp_path / ".20200101.zip.done").write_text("fp-new")

        assert is_marked_done(src, "fp-new")
        assert not is_marked_done(src, "fp-old")


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
        assert (tmp_path / ".20200101.zip.done").exists()

        delete_done_marker(src)

        assert not (tmp_path / ".20200101.zip.done").exists()

    def test_no_marker_present_is_not_an_error(self, tmp_path):
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")

        delete_done_marker(src)  # should not raise

    def test_removes_a_legacy_marker_too(self, tmp_path):
        # An installation mid-migration could have either naming still
        # present; --delete-source must not leave either one orphaned.
        src = tmp_path / "20200101.zip"
        src.write_bytes(b"data")
        (tmp_path / "20200101.zip.done").write_text("fp-1")

        delete_done_marker(src)

        assert not (tmp_path / "20200101.zip.done").exists()


class TestClearerDatasetErrors:
    """clearer_dataset_errors wraps a pyarrow.dataset read so a bare,
    low-level ArrowInvalid/OSError, e.g. "Could not open Parquet input
    source '<path>': ...", gets an actionable message on top, naming
    what was being read and the likely causes, instead of surfacing as
    a mystery "generic pyarrow error." Confirmed the real shape of this
    against a genuinely corrupt file, not assumed."""

    def test_an_arrow_error_is_wrapped_with_context(self):
        import pyarrow as pa

        with pytest.raises(RuntimeError, match=r"reading 3 parquet file\(s\)") as exc_info:
            with clearer_dataset_errors("3 parquet file(s)"):
                raise pa.ArrowInvalid("Could not open Parquet input source 'x': bad magic bytes")
        assert "Common causes" in str(exc_info.value)

    def test_the_original_exception_is_chained_not_discarded(self):
        import pyarrow as pa

        original = pa.ArrowInvalid("bad magic bytes")
        with pytest.raises(RuntimeError) as exc_info:
            with clearer_dataset_errors("1 parquet file(s)"):
                raise original

        assert exc_info.value.__cause__ is original

    def test_an_os_error_is_also_wrapped(self):
        with pytest.raises(RuntimeError, match="reading a dataset"):
            with clearer_dataset_errors("a dataset"):
                raise OSError("disk read failed")

    def test_file_not_found_error_passes_through_unwrapped(self):
        # FileNotFoundError is an OSError subclass, but gdeltforge's own
        # "no parquet files matched" checks (empty glob, a date range
        # excluding every file) raise it deliberately before ever
        # touching pyarrow: that's already a clear, correct error and
        # must not be reclassified as a generic pyarrow read failure.
        # Real regression: the first version of this wrapper caught bare
        # OSError, which silently also caught FileNotFoundError.
        with pytest.raises(FileNotFoundError, match="no files matched"):
            with clearer_dataset_errors("a dataset"):
                raise FileNotFoundError("no files matched")

    def test_an_unrelated_exception_passes_through_unwrapped(self):
        # Only the two exception types pyarrow/OS-level read failures
        # actually raise are caught; anything else (a real bug in the
        # caller's own code, e.g.) must not be masked as a data problem.
        with pytest.raises(ValueError, match="not a dataset problem"):
            with clearer_dataset_errors("something"):
                raise ValueError("not a dataset problem")

    def test_no_exception_is_a_no_op(self):
        with clearer_dataset_errors("something"):
            result = 1 + 1
        assert result == 2


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
