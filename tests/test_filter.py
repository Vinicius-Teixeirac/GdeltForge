import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import gdeltforge.filtering.filter as filter_module
from gdeltforge.filtering.filter import GDELTFilter, run_filter


def _write_parquet(path, data):
    pd.DataFrame(data).to_parquet(path)


class TestMaxWorkersConfig:
    def test_defaults_to_none_so_executor_uses_cpu_count(self, tmp_path):
        filt = GDELTFilter(str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"])
        assert filt.max_workers is None

    def test_explicit_value_is_respected(self, tmp_path):
        filt = GDELTFilter(
            str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"], max_workers=2
        )
        assert filt.max_workers == 2


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

    def test_empty_columns_to_check_is_a_no_op_not_an_error(self, tmp_path):
        # The bundled default config ships columns_to_check: [] for every
        # dataset deliberately, documented as a no-op (dropna against an
        # empty column list drops nothing). This must still write the file
        # with every row kept, not silently skip writing it: existing_
        # columns is trivially empty whenever columns_to_check itself is,
        # which used to be indistinguishable from "columns were configured
        # but none exist in this file's schema" below.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {
            "GlobalEventID": [1, 2, 3],
            "Actor1Name": ["A", None, "C"],
        })

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), [])
        out_path = tmp_path / "out" / "data_filtered.parquet"
        rows_before, rows_after = filt.filter_single_file(src, out_path)

        assert (rows_before, rows_after) == (3, 3)
        assert out_path.exists()
        result = pd.read_parquet(out_path)
        assert sorted(result["GlobalEventID"].tolist()) == [1, 2, 3]

    def test_configured_columns_all_missing_still_skips_writing(self, tmp_path, caplog):
        # Distinct from the empty-columns_to_check case above: here the
        # caller actually configured filter columns, and none of them
        # exist in this file's schema, a real signal something's
        # misconfigured (e.g. a typo), not a no-op. Must still bail out
        # without writing, same as before this fix.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2]})

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["DoesNotExist"])
        out_path = tmp_path / "out" / "data_filtered.parquet"
        with caplog.at_level("ERROR"):
            rows_before, rows_after = filt.filter_single_file(src, out_path)

        assert (rows_before, rows_after) == (2, 2)
        assert not out_path.exists()
        assert any("None of the filter columns exist" in r.message for r in caplog.records)

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

    def test_empty_columns_to_check_still_writes_every_file(self, tmp_path):
        # Batch-level version of TestFilterSingleFile's equivalent test:
        # the real regression was the summary claiming every file
        # "processed successfully" while filter_single_file quietly wrote
        # nothing, so this checks actual files on disk, not just the
        # returned counts.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2]})
        _write_parquet(input_dir / "b.parquet", {"GlobalEventID": [3, 4, 5]})

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), [])
        processed, failed = filt.filter_all_files()

        assert (processed, failed) == (2, 0)
        out_dir = tmp_path / "out"
        assert (out_dir / "a_filtered.parquet").exists()
        assert (out_dir / "b_filtered.parquet").exists()
        assert len(pd.read_parquet(out_dir / "a_filtered.parquet")) == 2
        assert len(pd.read_parquet(out_dir / "b_filtered.parquet")) == 3

    def test_counts_a_corrupt_file_as_failed(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        (input_dir / "bad.parquet").write_bytes(b"not a real parquet file")

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        processed, failed = filt.filter_all_files()

        assert processed == 0
        assert failed == 1

    def test_one_corrupt_file_does_not_abort_the_others(self, tmp_path):
        # Now that files run across a worker pool (see TestMaxWorkersConfig),
        # this is the same guarantee test_counts_a_corrupt_file_as_failed
        # checks in isolation, but proven alongside files that must still
        # succeed, mirroring the scraper/converter batch-isolation tests.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(
            input_dir / "good.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, None]}
        )
        (input_dir / "bad.parquet").write_bytes(b"not a real parquet file")

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"], max_workers=2)
        processed, failed = filt.filter_all_files()

        assert (processed, failed) == (1, 1)
        out = pd.read_parquet(tmp_path / "out" / "good_filtered.parquet")
        assert out["GlobalEventID"].tolist() == [1]

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


class TestFilterResumability:
    """Same .done marker mechanism as GDELTConverter (see test_converter.py's
    TestConversionResumability), plus the config-fingerprint check that
    mechanism didn't originally have: filter has several settings a user
    plausibly reruns with a different value (columns_to_check most of
    all), and each one changes what the filtered output actually
    contains, so a marker from a differently-configured run must not
    cause a resumed run to skip reprocessing that file."""

    def test_a_previously_filtered_file_is_skipped_on_rerun(self, tmp_path, caplog):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, None]})

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        filt.filter_all_files()

        # get_logger sets an explicit INFO level on this module's own
        # named logger at import time, so a bare caplog.at_level("DEBUG")
        # (root-only) never reaches it; the logger name must be given
        # explicitly to actually lower its effective level.
        with caplog.at_level("DEBUG", logger="gdeltforge.filtering.filter"):
            processed, failed = filt.filter_all_files()

        assert (processed, failed) == (0, 0)
        assert any(
            "Skipping already filtered" in r.message and "a.parquet" in r.message
            for r in caplog.records
        )

    def test_a_changed_columns_to_check_forces_reprocessing(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(
            input_dir / "a.parquet",
            {"GlobalEventID": [1, 2, 3], "QuadClass": [1, None, 3], "Actor1Name": [None, "B", "C"]},
        )

        GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"]).filter_all_files()
        out_path = tmp_path / "out" / "a_filtered.parquet"
        # QuadClass alone: only row 2 (index 1) has a NaN there.
        assert sorted(pd.read_parquet(out_path)["GlobalEventID"].tolist()) == [1, 3]

        # Rerun with a different columns_to_check must not be skipped by
        # the marker left above, and must actually re-filter by the new
        # criteria rather than leaving the stale QuadClass-only output.
        processed, failed = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["Actor1Name"]
        ).filter_all_files()

        assert (processed, failed) == (1, 0)
        assert sorted(pd.read_parquet(out_path)["GlobalEventID"].tolist()) == [2, 3]

    def test_a_changed_output_columns_forces_reprocessing(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(
            input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, 2]}
        )

        GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"], output_columns=["GlobalEventID"]
        ).filter_all_files()
        out_path = tmp_path / "out" / "a_filtered.parquet"
        assert list(pd.read_parquet(out_path).columns) == ["GlobalEventID"]

        processed, failed = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"], output_columns=None
        ).filter_all_files()

        assert (processed, failed) == (1, 0)
        assert list(pd.read_parquet(out_path).columns) == ["GlobalEventID", "QuadClass"]

    def test_an_unchanged_config_across_reordered_columns_still_skips(self, tmp_path, caplog):
        # columns_to_check=["A", "B"] and ["B", "A"] enforce the same set;
        # config_fingerprint sorts list fields, so this must still skip.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(
            input_dir / "a.parquet", {"GlobalEventID": [1], "A": [1], "B": [1]}
        )

        GDELTFilter(str(input_dir), str(tmp_path / "out"), ["A", "B"]).filter_all_files()

        with caplog.at_level("DEBUG", logger="gdeltforge.filtering.filter"):
            processed, failed = GDELTFilter(
                str(input_dir), str(tmp_path / "out"), ["B", "A"]
            ).filter_all_files()

        assert (processed, failed) == (0, 0)
        assert any("Skipping already filtered" in r.message for r in caplog.records)

    def test_a_file_that_still_errors_is_not_marked_done(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        (input_dir / "bad.parquet").write_bytes(b"not a real parquet file")

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        filt.filter_all_files()

        assert not filter_module.is_marked_done(
            input_dir / "bad.parquet", filt._config_fingerprint
        )


class TestDeleteSource:
    """delete_source (CLI: --delete-source) removes the source (unfiltered,
    converted) parquet once its filtered output is confirmed written and
    marked done, so a full historical pull doesn't need to hold both
    copies at once. Off by default."""

    def test_off_by_default_source_survives(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "a.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2]})

        GDELTFilter(str(input_dir), str(tmp_path / "out"), []).filter_all_files()

        assert src.exists()

    def test_deletes_the_source_after_a_successful_filter(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "a.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2]})

        processed, failed = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), [], delete_source=True
        ).filter_all_files()

        assert (processed, failed) == (1, 0)
        assert not src.exists()
        assert (tmp_path / "out" / "a_filtered.parquet").exists()

    def test_never_deletes_a_file_that_failed_to_filter(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        bad = input_dir / "bad.parquet"
        bad.write_bytes(b"not a real parquet file")

        processed, failed = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), [], delete_source=True
        ).filter_all_files()

        assert (processed, failed) == (0, 1)
        assert bad.exists()

    def test_deletion_failure_is_logged_not_fatal(self, tmp_path, monkeypatch, caplog):
        # The filter itself already succeeded; a failure to delete the
        # source afterward (permissions, already gone) must not be
        # reported as a filter failure.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "a.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2]})

        real_unlink = Path.unlink

        def selective_unlink(self, *args, **kwargs):
            if self == src:
                raise OSError("locked")
            return real_unlink(self, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", selective_unlink)

        with caplog.at_level("WARNING"):
            processed, failed = GDELTFilter(
                str(input_dir), str(tmp_path / "out"), [], delete_source=True
            ).filter_all_files()

        assert (processed, failed) == (1, 0)
        assert src.exists()
        assert any(
            "Could not delete source parquet" in r.message and "a.parquet" in r.message
            for r in caplog.records
        )


class TestForce:
    """force (CLI: --force) bypasses the is_marked_done check in
    filter_all_files, so a file already marked done is reprocessed and
    its filtered output overwritten instead of skipped. Off by default."""

    def test_off_by_default_a_done_file_is_skipped(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"]).filter_all_files()
        processed, failed = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"]
        ).filter_all_files()

        assert (processed, failed) == (0, 0)

    def test_force_reprocesses_a_file_already_marked_done(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "a.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"]).filter_all_files()
        processed, failed = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"], force=True
        ).filter_all_files()

        assert (processed, failed) == (1, 0)
        assert src.exists()  # force alone does not imply delete_source


class TestOutputColumns:
    def test_defaults_to_none_and_keeps_every_column(self, tmp_path):
        filt = GDELTFilter(str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"])
        assert filt.output_columns is None

    def test_projects_to_the_configured_subset(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {
            "GlobalEventID": [1, 2],
            "Actor1Name": ["A", "B"],
            "QuadClass": [1, 2],
        })

        filt = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            output_columns=["GlobalEventID", "QuadClass"],
        )
        out_path = tmp_path / "out" / "data_filtered.parquet"
        filt.filter_single_file(src, out_path)

        result = pd.read_parquet(out_path)
        assert list(result.columns) == ["GlobalEventID", "QuadClass"]

    def test_a_configured_column_missing_from_the_file_is_skipped_not_fatal(self, tmp_path):
        # Mirrors columns_to_check's existing/missing split: schema drift
        # (a column absent from one file) shouldn't crash the whole run.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            output_columns=["GlobalEventID", "DoesNotExist"],
        )
        out_path = tmp_path / "out" / "data_filtered.parquet"
        filt.filter_single_file(src, out_path)

        result = pd.read_parquet(out_path)
        assert list(result.columns) == ["GlobalEventID"]

    def test_row_filtering_is_unaffected_by_column_projection(self, tmp_path):
        # Row-drop decisions must still be based on columns_to_check even
        # when one of them is projected out of the final output.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {
            "GlobalEventID": [1, 2, 3],
            "Actor1Name": ["A", None, "C"],
            "QuadClass": [1, 2, 3],
        })

        filt = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["Actor1Name"],
            output_columns=["GlobalEventID", "QuadClass"],
        )
        out_path = tmp_path / "out" / "data_filtered.parquet"
        rows_before, rows_after = filt.filter_single_file(src, out_path)

        assert (rows_before, rows_after) == (3, 2)
        result = pd.read_parquet(out_path)
        assert sorted(result["GlobalEventID"].tolist()) == [1, 3]
        assert "Actor1Name" not in result.columns


class TestCompressionConfig:
    def test_defaults_to_zstd(self, tmp_path):
        # zstd became the default 2026-08-07: measured ~30% smaller than
        # snappy on real GDELT data at comparable or faster write speed,
        # and it's lossless, so there's no accuracy tradeoff to weigh.
        filt = GDELTFilter(str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"])
        assert filt.compression == "zstd"

    def test_default_codec_is_used_on_write(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        out_path = tmp_path / "out" / "data_filtered.parquet"
        filt.filter_single_file(src, out_path)

        metadata = pq.ParquetFile(out_path).metadata
        codec = metadata.row_group(0).column(0).compression
        assert codec.lower() == "zstd"

    def test_explicit_codec_overrides_the_default(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"], compression="snappy",
        )
        out_path = tmp_path / "out" / "data_filtered.parquet"
        filt.filter_single_file(src, out_path)

        metadata = pq.ParquetFile(out_path).metadata
        codec = metadata.row_group(0).column(0).compression
        assert codec.lower() == "snappy"


class TestFloat32Columns:
    def test_defaults_to_none_and_keeps_float64(self, tmp_path):
        filt = GDELTFilter(str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"])
        assert filt.float32_columns is None

    def test_configured_columns_are_narrowed_to_float32(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        # A value with more significant figures than float32 can hold
        # (~7), so the round-trip actually changes something observable
        # rather than the test passing by coincidence.
        _write_parquet(src, {
            "GlobalEventID": [1, 2],
            "QuadClass": [1, 2],
            "AvgTone": [0.0284010224368077, -1.234567891234],
        })

        filt = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            float32_columns=["AvgTone"],
        )
        out_path = tmp_path / "out" / "data_filtered.parquet"
        filt.filter_single_file(src, out_path)

        schema = pq.ParquetFile(out_path).schema_arrow
        assert schema.field("AvgTone").type == pa.float32()
        assert schema.field("QuadClass").type != pa.float32()

        result = pd.read_parquet(out_path)
        # The value actually changed under float32 rounding, proving the
        # cast ran rather than the column merely being typed float32 on
        # an unchanged 64-bit value. float() is required here, not
        # incidental: comparing a numpy float32 directly against a Python
        # float silently compares at float32 precision (numpy downcasts
        # the Python float rather than upcasting the float32), which
        # would make this assertion pass even without narrowing at all.
        assert float(result["AvgTone"].iloc[0]) != 0.0284010224368077

    def test_a_configured_column_missing_from_the_file_is_skipped_not_fatal(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1], "QuadClass": [1]})

        filt = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            float32_columns=["DoesNotExist"],
        )
        out_path = tmp_path / "out" / "data_filtered.parquet"
        # Must not raise even though the configured column isn't present.
        filt.filter_single_file(src, out_path)
        assert out_path.exists()

    def test_a_configured_non_float_column_is_skipped_not_fatal(self, tmp_path):
        # QuadClass is an int64 column; asking to float32-narrow it is a
        # misconfiguration (stale config pointed at the wrong name, a
        # renamed/retyped column, etc.), not something to force-cast.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1], "QuadClass": [1]})

        filt = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            float32_columns=["QuadClass"],
        )
        out_path = tmp_path / "out" / "data_filtered.parquet"
        filt.filter_single_file(src, out_path)

        schema = pq.ParquetFile(out_path).schema_arrow
        assert schema.field("QuadClass").type == pa.int64()

    def test_interacts_correctly_with_output_columns_projection(self, tmp_path):
        # The float32 cast must survive column projection, whichever order
        # a reader might assume they interact in.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {
            "GlobalEventID": [1],
            "QuadClass": [1],
            "AvgTone": [0.0284010224368077],
        })

        filt = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            output_columns=["GlobalEventID", "AvgTone"],
            float32_columns=["AvgTone"],
        )
        out_path = tmp_path / "out" / "data_filtered.parquet"
        filt.filter_single_file(src, out_path)

        schema = pq.ParquetFile(out_path).schema_arrow
        assert list(schema.names) == ["GlobalEventID", "AvgTone"]
        assert schema.field("AvgTone").type == pa.float32()


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


class TestRunFilterDatasetParameter:
    """run_filter (not GDELTFilter itself, which is already dataset-agnostic)
    is what resolves dataset-specific paths.* and filter.columns_to_check
    keys; this end-to-end coverage is what's new here, constructor-level
    resolution alone can't prove the right directory gets read/written or
    the right check-list gets enforced."""

    @staticmethod
    def _config(tmp_path):
        events_in, events_out = tmp_path / "events_in", tmp_path / "events_out"
        gkg_in, gkg_out = tmp_path / "gkg_v2_in", tmp_path / "gkg_v2_out"
        events_in.mkdir()
        gkg_in.mkdir()
        return {
            "paths": {
                "parquet_data_directory": str(events_in),
                "filtered_data_directory": str(events_out),
                "gkg_v2_parquet_data_directory": str(gkg_in),
                "gkg_v2_filtered_data_directory": str(gkg_out),
            },
            "filter": {
                "columns_to_check": {
                    "gdelt_event": ["Actor1Name"],
                    "gdelt_gkg_v2": ["V2DOCUMENTIDENTIFIER"],
                },
            },
            "converter": {"partitioning": {"enabled": False}},
        }, events_in, gkg_in

    def test_defaults_to_events_for_backward_compatibility(self, tmp_path):
        cfg, events_in, _ = self._config(tmp_path)
        pd.DataFrame({
            "GlobalEventID": [1, 2], "Actor1Name": ["A", None],
        }).to_parquet(events_in / "a.parquet")

        processed, failed = run_filter(cfg)

        assert (processed, failed) == (1, 0)
        out = pd.read_parquet(cfg["paths"]["filtered_data_directory"] + "/a_filtered.parquet")
        assert out["GlobalEventID"].tolist() == [1]

    def test_non_events_dataset_reads_its_own_directory_and_check_list(self, tmp_path):
        # Actor1Name (gdelt_event's own check column) doesn't exist on the
        # GKG side at all; if run_filter ever fell back to gdelt_event's
        # columns_to_check by mistake, GDELTFilter's "missing columns are
        # skipped, not fatal" behavior would silently pass both rows
        # through unfiltered instead of enforcing V2DOCUMENTIDENTIFIER, so
        # a wrong dataset resolution here would show up as len(out) == 2.
        cfg, _, gkg_in = self._config(tmp_path)
        pd.DataFrame({
            "GKGRECORDID": ["r1", "r2"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com", None],
        }).to_parquet(gkg_in / "a.parquet")

        processed, failed = run_filter(cfg, dataset="gdelt_gkg_v2")

        assert (processed, failed) == (1, 0)
        out_dir = cfg["paths"]["gkg_v2_filtered_data_directory"]
        out = pd.read_parquet(out_dir + "/a_filtered.parquet")
        assert out["GKGRECORDID"].tolist() == ["r1"]

    def test_passes_max_workers_through_to_the_filterer(self, tmp_path, monkeypatch):
        cfg, events_in, _ = self._config(tmp_path)
        cfg["filter"]["max_workers"] = 3
        pd.DataFrame(
            {"GlobalEventID": [1], "Actor1Name": ["A"]}
        ).to_parquet(events_in / "a.parquet")

        captured = {}
        real_init = GDELTFilter.__init__

        def spy_init(self, *args, **kwargs):
            captured.update(kwargs)
            real_init(self, *args, **kwargs)

        monkeypatch.setattr(GDELTFilter, "__init__", spy_init)

        run_filter(cfg)

        assert captured["max_workers"] == 3

    def test_missing_max_workers_key_defaults_to_none(self, tmp_path):
        # config["filter"] historically had no max_workers key at all
        # (pre-dating this feature); run_filter must not KeyError on it.
        cfg, events_in, _ = self._config(tmp_path)
        pd.DataFrame(
            {"GlobalEventID": [1], "Actor1Name": ["A"]}
        ).to_parquet(events_in / "a.parquet")

        processed, failed = run_filter(cfg)

        assert (processed, failed) == (1, 0)

    def test_missing_output_columns_and_compression_keys_default_to_full_zstd(self, tmp_path):
        # Same backward-compatibility guarantee as max_workers above: configs
        # that pre-date output_columns/compression/float32_columns must not
        # KeyError, must keep writing every column, and get zstd (the
        # current default) rather than erroring for lack of an explicit
        # per-dataset override.
        cfg, events_in, _ = self._config(tmp_path)
        pd.DataFrame(
            {"GlobalEventID": [1], "Actor1Name": ["A"]}
        ).to_parquet(events_in / "a.parquet")

        processed, failed = run_filter(cfg)

        assert (processed, failed) == (1, 0)
        out_path = cfg["paths"]["filtered_data_directory"] + "/a_filtered.parquet"
        out = pd.read_parquet(out_path)
        assert list(out.columns) == ["GlobalEventID", "Actor1Name"]
        codec = pq.ParquetFile(out_path).metadata.row_group(0).column(0).compression
        assert codec.lower() == "zstd"

    def test_output_columns_and_compression_are_resolved_per_dataset(self, tmp_path):
        cfg, _, gkg_in = self._config(tmp_path)
        cfg["filter"]["output_columns"] = {
            "gdelt_gkg_v2": ["GKGRECORDID", "V2DOCUMENTIDENTIFIER"],
        }
        cfg["filter"]["compression"] = {"gdelt_gkg_v2": "zstd"}
        pd.DataFrame({
            "GKGRECORDID": ["r1", "r2"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com", "http://b.com"],
            "V2GCAM": ["unused", "unused"],
        }).to_parquet(gkg_in / "a.parquet")

        processed, failed = run_filter(cfg, dataset="gdelt_gkg_v2")

        assert (processed, failed) == (1, 0)
        out_path = Path(cfg["paths"]["gkg_v2_filtered_data_directory"]) / "a_filtered.parquet"
        out = pd.read_parquet(out_path)
        assert list(out.columns) == ["GKGRECORDID", "V2DOCUMENTIDENTIFIER"]

        codec = pq.ParquetFile(out_path).metadata.row_group(0).column(0).compression
        assert codec.lower() == "zstd"

    def test_float32_columns_is_resolved_per_dataset(self, tmp_path):
        cfg, events_in, gkg_in = self._config(tmp_path)
        cfg["filter"]["float32_columns"] = {"gdelt_gkg_v2": ["Tone"]}
        pd.DataFrame({
            "GKGRECORDID": ["r1"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com"],
            "Tone": [1.5],
        }).to_parquet(gkg_in / "a.parquet")
        pd.DataFrame({
            "GlobalEventID": [1], "Actor1Name": ["A"], "GoldsteinScale": [2.8],
        }).to_parquet(events_in / "a.parquet")

        # events_in has no float32_columns entry configured for it, so it
        # must be unaffected by gdelt_gkg_v2's setting.
        processed_events, _ = run_filter(cfg)
        processed_gkg, failed_gkg = run_filter(cfg, dataset="gdelt_gkg_v2")

        assert (processed_events, processed_gkg, failed_gkg) == (1, 1, 0)

        events_schema = pq.ParquetFile(
            cfg["paths"]["filtered_data_directory"] + "/a_filtered.parquet"
        ).schema_arrow
        assert events_schema.field("GoldsteinScale").type != pa.float32()

        gkg_schema = pq.ParquetFile(
            Path(cfg["paths"]["gkg_v2_filtered_data_directory"]) / "a_filtered.parquet"
        ).schema_arrow
        assert gkg_schema.field("Tone").type == pa.float32()


class TestCrossrefJoinKeyWarning:
    """output_columns makes it easy to prune a dataset's crossref join
    key by accident (see gdeltforge.crossref.crossref.REQUIRED_JOIN_COLUMNS);
    run_filter should warn about it at filter time rather than let the
    failure surface only when `crossref` is run later, possibly after an
    expensive sample pass in between."""

    def test_warns_when_output_columns_omits_the_join_key(self, tmp_path, caplog):
        cfg, _, gkg_in = TestRunFilterDatasetParameter._config(tmp_path)
        cfg["filter"]["output_columns"] = {
            # Missing V2DOCUMENTIDENTIFIER, gdelt_gkg_v2's join key.
            "gdelt_gkg_v2": ["GKGRECORDID"],
        }
        pd.DataFrame({
            "GKGRECORDID": ["r1"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com"],
        }).to_parquet(gkg_in / "a.parquet")

        with caplog.at_level("WARNING"):
            run_filter(cfg, dataset="gdelt_gkg_v2")

        assert any(
            "V2DOCUMENTIDENTIFIER" in r.message and "crossref" in r.message
            for r in caplog.records
        )

    def test_no_warning_when_the_join_key_is_kept(self, tmp_path, caplog):
        cfg, _, gkg_in = TestRunFilterDatasetParameter._config(tmp_path)
        cfg["filter"]["output_columns"] = {
            "gdelt_gkg_v2": ["GKGRECORDID", "V2DOCUMENTIDENTIFIER"],
        }
        pd.DataFrame({
            "GKGRECORDID": ["r1"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com"],
        }).to_parquet(gkg_in / "a.parquet")

        with caplog.at_level("WARNING"):
            run_filter(cfg, dataset="gdelt_gkg_v2")

        assert not any("crossref" in r.message for r in caplog.records)

    def test_no_warning_when_output_columns_is_unset(self, tmp_path, caplog):
        # output_columns=None means every column survives; nothing to warn
        # about even though gdelt_gkg_v2 does have a required join key.
        cfg, _, gkg_in = TestRunFilterDatasetParameter._config(tmp_path)
        pd.DataFrame({
            "GKGRECORDID": ["r1"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com"],
        }).to_parquet(gkg_in / "a.parquet")

        with caplog.at_level("WARNING"):
            run_filter(cfg, dataset="gdelt_gkg_v2")

        assert not any("crossref" in r.message for r in caplog.records)


class TestRunFilterWarnsAboutDeleteSource:
    """Same shared warning as run_converter (see test_converter.py's own
    version). columns_to_check is the setting most tests here already
    configure non-empty (see TestRunFilterDatasetParameter._config), so
    delete_source=True alone is enough to trigger it without any extra
    setup."""

    def test_warns_when_delete_source_and_columns_to_check_are_both_active(
        self, tmp_path, caplog
    ):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pd.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).to_parquet(
            events_in / "a.parquet"
        )

        with caplog.at_level("WARNING"):
            run_filter(cfg, delete_source=True)

        assert any(
            "delete_source" in r.message and "columns_to_check" in r.message
            for r in caplog.records
        )

    def test_no_warning_when_delete_source_is_false(self, tmp_path, caplog):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pd.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).to_parquet(
            events_in / "a.parquet"
        )

        with caplog.at_level("WARNING"):
            run_filter(cfg, delete_source=False)

        assert not any("delete_source" in r.message for r in caplog.records)

    def test_no_warning_when_nothing_narrows_the_output(self, tmp_path, caplog):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        cfg["filter"]["columns_to_check"]["gdelt_event"] = []
        pd.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).to_parquet(
            events_in / "a.parquet"
        )

        with caplog.at_level("WARNING"):
            run_filter(cfg, delete_source=True)

        assert not any("delete_source" in r.message for r in caplog.records)


class TestVerboseLogging:
    """--verbose raises this module's own logger to DEBUG, revealing the
    per-file "{name}: rows -> rows"/"Skipping already filtered" lines
    that are DEBUG-level (invisible) by default. logger.setLevel is a
    real, process-wide mutation on a singleton (logging.getLogger caches
    by name), so every test here restores INFO afterward regardless of
    outcome, rather than leaking state into whichever test runs next."""

    def test_off_by_default_logger_level_is_unchanged(self, tmp_path):
        filter_module.logger.setLevel(logging.INFO)
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pd.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).to_parquet(
            events_in / "a.parquet"
        )

        run_filter(cfg)

        assert filter_module.logger.level == logging.INFO

    def test_verbose_lowers_the_logger_to_debug(self, tmp_path):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pd.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).to_parquet(
            events_in / "a.parquet"
        )
        try:
            run_filter(cfg, verbose=True)
            assert filter_module.logger.level == logging.DEBUG
        finally:
            filter_module.logger.setLevel(logging.INFO)

    def test_verbose_reveals_the_per_file_row_count_line(self, tmp_path, caplog):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pd.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).to_parquet(
            events_in / "a.parquet"
        )
        try:
            with caplog.at_level("DEBUG", logger="gdeltforge.filtering.filter"):
                run_filter(cfg, verbose=True)
            assert any("a.parquet" in r.message and "rows" in r.message for r in caplog.records)
        finally:
            filter_module.logger.setLevel(logging.INFO)

    def test_warns_for_gkg_v1_counts_too(self, tmp_path, caplog):
        # gdelt_gkg_v1_counts is a real, distinct crossref target (the
        # `crossref --gkg-version v1-counts` path) with its own entry in
        # REQUIRED_JOIN_COLUMNS, not just an alias of gdelt_gkg_v1.
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        cfg["paths"]["gkg_v1_counts_parquet_data_directory"] = str(events_in)
        cfg["paths"]["gkg_v1_counts_filtered_data_directory"] = str(tmp_path / "out")
        cfg["filter"]["columns_to_check"]["gdelt_gkg_v1_counts"] = ["Date"]
        # Missing EventIds, gdelt_gkg_v1_counts' join key.
        cfg["filter"]["output_columns"] = {"gdelt_gkg_v1_counts": ["Date"]}
        pd.DataFrame({"Date": [20130401], "EventIds": ["1,2"]}).to_parquet(
            events_in / "a.parquet"
        )

        with caplog.at_level("WARNING"):
            run_filter(cfg, dataset="gdelt_gkg_v1_counts")

        assert any(
            "EventIds" in r.message and "crossref" in r.message for r in caplog.records
        )


class TestQuietLogging:
    """--quiet raises this module's own logger to WARNING, suppressing
    the setup/summary lines run_filter otherwise always logs at INFO.
    Mutually exclusive with --verbose at the CLI; this module doesn't
    enforce that itself, so it isn't re-tested here."""

    def test_quiet_raises_the_logger_to_warning(self, tmp_path):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pd.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).to_parquet(
            events_in / "a.parquet"
        )
        try:
            run_filter(cfg, quiet=True)
            assert filter_module.logger.level == logging.WARNING
        finally:
            filter_module.logger.setLevel(logging.INFO)

    def test_quiet_suppresses_the_summary_line(self, tmp_path, caplog):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pd.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).to_parquet(
            events_in / "a.parquet"
        )
        try:
            with caplog.at_level("DEBUG", logger="gdeltforge.filtering.filter"):
                run_filter(cfg, quiet=True)
            assert not any("FILTERING SUMMARY" in r.message for r in caplog.records)
        finally:
            filter_module.logger.setLevel(logging.INFO)


class TestFilterSingleFileAtomicity:
    """filter_single_file used to write straight to output_path via a
    streaming ParquetWriter. Now that filter_all_files runs files across a
    worker pool (TestMaxWorkersConfig), a killed worker is a real,
    reachable failure mode, not just a hypothetical: a truncated file
    left at output_path would be silently picked up by anything reading
    the filtered directory afterwards. Now writes through a temp file and
    only renames into place once the stream completes, matching the same
    pattern already used for converter output."""

    def test_leaves_no_file_on_write_failure(self, tmp_path, monkeypatch):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        def boom(self, table, **kwargs):
            raise OSError("simulated crash mid-write")

        monkeypatch.setattr(filter_module.pq.ParquetWriter, "write_table", boom)

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        out_path = tmp_path / "out" / "data_filtered.parquet"

        with pytest.raises(OSError):
            filt.filter_single_file(src, out_path)

        assert not out_path.exists()
        assert not out_path.with_name(out_path.name + ".tmp").exists()

    def test_successful_write_leaves_no_tmp_behind(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTFilter(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        out_path = tmp_path / "out" / "data_filtered.parquet"
        filt.filter_single_file(src, out_path)

        assert out_path.exists()
        assert not out_path.with_name(out_path.name + ".tmp").exists()
        assert pq.read_table(out_path).num_rows == 2
