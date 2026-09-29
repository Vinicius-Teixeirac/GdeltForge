import concurrent.futures
import datetime
import json
import logging
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tqdm import tqdm

import gdeltforge.cleaning.cleaner as cleaner_module
from gdeltforge.cleaning.cleaner import GDELTCleaner, run_cleaner
from gdeltforge.cleaning.steps import Date1920Repair


def _write_parquet(path, data):
    pl.DataFrame(data).write_parquet(path)


class TestMaxWorkersConfig:
    def test_defaults_to_none_so_executor_uses_cpu_count(self, tmp_path):
        filt = GDELTCleaner(str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"])
        assert filt.max_workers is None

    def test_explicit_value_is_respected(self, tmp_path):
        filt = GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"], max_workers=2
        )
        assert filt.max_workers == 2

    def test_zero_raises_immediately_instead_of_a_contradictory_log_sequence(
        self, tmp_path
    ):
        # max_workers: 0 used to reach ProcessPoolExecutor unchecked: 0
        # is falsy, so the pre-flight "Filtering N flat file(s) ... using
        # X worker process(es)..." log line's own `self.max_workers or
        # os.cpu_count()` fallback silently reported the real CPU count
        # instead, one line before ProcessPoolExecutor's own constructor
        # raised "max_workers must be greater than 0" against the
        # original, still-0 value. Checked eagerly here now, before
        # either ever sees it.
        with pytest.raises(ValueError, match="must be greater than 0"):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"], max_workers=0
            )

    def test_negative_value_raises_the_same_way_as_zero(self, tmp_path):
        with pytest.raises(ValueError, match="must be greater than 0"):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"], max_workers=-1
            )


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

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["Actor1Name", "QuadClass"])
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        rows_before, rows_after = filt.clean_single_file(src, out_path)

        assert rows_before == 4
        assert rows_after == 2  # rows 2 and 3 each have one NaN in a checked column

        result = pl.read_parquet(out_path)
        assert sorted(result["GlobalEventID"].to_list()) == [1, 4]

    def test_missing_columns_are_skipped_not_fatal(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, None]})

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass", "DoesNotExist"])
        rows_before, rows_after = filt.clean_single_file(src, tmp_path / "out" / "o.parquet")

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

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), [])
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        rows_before, rows_after = filt.clean_single_file(src, out_path)

        assert (rows_before, rows_after) == (3, 3)
        assert out_path.exists()
        result = pl.read_parquet(out_path)
        assert sorted(result["GlobalEventID"].to_list()) == [1, 2, 3]

    def test_configured_columns_all_missing_raises_instead_of_a_silent_success(
        self, tmp_path
    ):
        # Distinct from the empty-columns_to_check case above: here the
        # caller actually configured filter columns, and none of them
        # exist in this file's schema, a real signal something's
        # misconfigured (e.g. a typo), not a no-op. This used to log an
        # ERROR and return as though the file had been filtered
        # successfully, at 100% retention, with no output written; a
        # caller working only from the returned counts (as filter_all_
        # files does) had no way to tell that apart from a genuine,
        # correctly-checked file. It now raises, so clean_all_files
        # counts this file as failed rather than processed.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2]})

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["DoesNotExist"])
        out_path = tmp_path / "out" / "data_cleaned.parquet"

        with pytest.raises(ValueError, match="none of the configured columns_to_check"):
            filt.clean_single_file(src, out_path)

        assert not out_path.exists()

    def test_empty_file_returns_zero_zero(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "empty.parquet"
        _write_parquet(src, {"GlobalEventID": pl.Series([], dtype=pl.Int64)})

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["GlobalEventID"])
        rows_before, rows_after = filt.clean_single_file(src, tmp_path / "out" / "o.parquet")

        assert (rows_before, rows_after) == (0, 0)


class TestFilterAllFiles:
    def test_aggregates_across_flat_files(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, None]})
        _write_parquet(
            input_dir / "b.parquet", {"GlobalEventID": [3, 4, 5], "QuadClass": [1, 2, 3]}
        )

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        processed, failed = filt.clean_all_files()

        assert processed == 2
        assert failed == 0

    def test_empty_columns_to_check_still_writes_every_file(self, tmp_path):
        # Batch-level version of TestFilterSingleFile's equivalent test:
        # the real regression was the summary claiming every file
        # "processed successfully" while clean_single_file quietly wrote
        # nothing, so this checks actual files on disk, not just the
        # returned counts.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2]})
        _write_parquet(input_dir / "b.parquet", {"GlobalEventID": [3, 4, 5]})

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), [])
        processed, failed = filt.clean_all_files()

        assert (processed, failed) == (2, 0)
        out_dir = tmp_path / "out"
        assert (out_dir / "a_cleaned.parquet").exists()
        assert (out_dir / "b_cleaned.parquet").exists()
        assert len(pl.read_parquet(out_dir / "a_cleaned.parquet")) == 2
        assert len(pl.read_parquet(out_dir / "b_cleaned.parquet")) == 3

    def test_counts_a_corrupt_file_as_failed(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        (input_dir / "bad.parquet").write_bytes(b"not a real parquet file")

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        processed, failed = filt.clean_all_files()

        assert processed == 0
        assert failed == 1

    def test_counts_an_all_invalid_columns_to_check_file_as_failed(self, tmp_path):
        # Batch-level version of TestFilterSingleFile's equivalent test:
        # this used to be indistinguishable, at the clean_all_files
        # level, from a file that was genuinely and correctly filtered,
        # counted under "processed" at 100% retention with a 0 exit code
        # despite the ERROR logged for it.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "data.parquet", {"GlobalEventID": [1, 2]})

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["DoesNotExist"])
        processed, failed = filt.clean_all_files()

        assert (processed, failed) == (0, 1)
        assert not (tmp_path / "out" / "data_cleaned.parquet").exists()

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

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"], max_workers=2)
        processed, failed = filt.clean_all_files()

        assert (processed, failed) == (1, 1)
        out = pl.read_parquet(tmp_path / "out" / "good_cleaned.parquet")
        assert out["GlobalEventID"].to_list() == [1]

    def test_preserves_historical_directory_structure(self, tmp_path):
        flat_in = tmp_path / "flat_in"
        hist_in = tmp_path / "hist_in"
        flat_in.mkdir()
        hist_in.mkdir()

        part_dir = hist_in / "Year=1979"
        part_dir.mkdir()
        _write_parquet(part_dir / "1979.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTCleaner(
            str(flat_in), str(tmp_path / "flat_out"), ["QuadClass"],
            historical_input_folder=str(hist_in),
            historical_output_folder=str(tmp_path / "hist_out"),
        )
        processed, failed = filt.clean_all_files()

        assert (processed, failed) == (1, 0)
        assert (tmp_path / "hist_out" / "Year=1979" / "1979_cleaned.parquet").exists()


class TestFilterAllFilesInterruptHandling:
    """clean_all_files' own version of convert's identical regression
    coverage (test_converter.py's TestProcessAllFilesInterruptHandling):
    every future is submitted up front, so the executor's own default
    __exit__ (shutdown(wait=True)) would drain every one of them,
    including ones that hadn't even started, before actually exiting.
    Simulated at the as_completed() iteration point, the same place a
    real Ctrl+C signal actually lands, rather than trying to raise it
    from inside a worker process."""

    def test_interrupt_cancels_queued_futures_and_still_propagates(
        self, monkeypatch, tmp_path
    ):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        for i in range(3):
            _write_parquet(input_dir / f"{i}.parquet", {"GlobalEventID": [1, 2]})

        real_as_completed = cleaner_module.as_completed

        def interrupting_as_completed(fs, *a, **kw):
            for i, f in enumerate(real_as_completed(fs, *a, **kw)):
                if i == 1:
                    raise KeyboardInterrupt()
                yield f

        monkeypatch.setattr(cleaner_module, "as_completed", interrupting_as_completed)

        shutdown_calls = []
        original_shutdown = cleaner_module.ProcessPoolExecutor.shutdown

        def spying_shutdown(self, *a, **kw):
            shutdown_calls.append(kw)
            return original_shutdown(self, *a, **kw)

        monkeypatch.setattr(cleaner_module.ProcessPoolExecutor, "shutdown", spying_shutdown)

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), [])

        with pytest.raises(KeyboardInterrupt):
            filt.clean_all_files()

        assert any(
            c.get("wait") is False and c.get("cancel_futures") is True
            for c in shutdown_calls
        )


@contextmanager
def _capture_unraisable_exceptions():
    """sys.unraisablehook (Python >=3.8) is what CPython calls instead of
    printing "Exception ignored in: ..." to stderr directly, so replacing
    it here is what lets a test observe the exact leak this guards
    against, rather than only being able to see it as incidental stderr
    noise a real terminal user would notice by eye."""
    events = []
    original_hook = sys.unraisablehook
    sys.unraisablehook = events.append
    try:
        yield events
    finally:
        sys.unraisablehook = original_hook


def _patch_tqdm_close_to_raise_once(monkeypatch):
    """Simulates a second interrupt landing while tqdm's own close() is
    running, matching real tqdm's own close() being idempotent (a second
    call after the first already succeeded is a no-op). Scoped to
    instances created while this patch is active, not global: see
    samplers.py's own test_samplers.py::TestTqdmInterruptDoesNotLeakA
    Traceback._patch_tqdm_close_to_raise_once for why an unscoped patch
    (a bare shared flag) is a real test-isolation hazard, confirmed
    against the full suite under this project's own pinned dependency
    floor: a leftover tqdm instance from an unrelated, already-finished
    test being garbage collected during this test's own window could
    consume its one allowed raise."""
    real_init = tqdm.__init__
    tracked_ids: set[int] = set()

    def tracking_init(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        tracked_ids.add(id(self))

    closed_once_ids: set[int] = set()

    def close_raises_once(self):
        if id(self) not in tracked_ids or id(self) in closed_once_ids:
            return
        closed_once_ids.add(id(self))
        raise KeyboardInterrupt("second interrupt, during close")

    monkeypatch.setattr(tqdm, "__init__", tracking_init)
    monkeypatch.setattr(tqdm, "close", close_raises_once)


class TestFilterAllFilesTqdmInterruptDoesNotLeakATraceback:
    """clean_all_files' executor loop shares the identical bare-"for x
    in tqdm(iterable):" pattern samplers.py's own
    TestTqdmInterruptDoesNotLeakATraceback class documents and guards
    against in full (see that class's own docstring for the real
    mechanism)."""

    def test_interrupt_during_future_result_does_not_leak_a_traceback(
        self, monkeypatch, tmp_path
    ):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "0.parquet", {"GlobalEventID": [1, 2]})

        _patch_tqdm_close_to_raise_once(monkeypatch)

        def result_and_interrupt(self, *a, **kw):
            raise KeyboardInterrupt("first interrupt, mid-loop")

        monkeypatch.setattr(concurrent.futures.Future, "result", result_and_interrupt)

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), [])

        with _capture_unraisable_exceptions() as events:
            with pytest.raises(KeyboardInterrupt):
                filt.clean_all_files()

        assert events == [], f"tqdm leaked an unraisable exception: {events}"


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

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        filt.clean_all_files()

        # get_logger sets an explicit INFO level on this module's own
        # named logger at import time, so a bare caplog.at_level("DEBUG")
        # (root-only) never reaches it; the logger name must be given
        # explicitly to actually lower its effective level.
        with caplog.at_level("DEBUG", logger="gdeltforge.cleaning.cleaner"):
            processed, failed = filt.clean_all_files()

        assert (processed, failed) == (0, 0)
        assert any(
            "Skipping already cleaned" in r.message and "a.parquet" in r.message
            for r in caplog.records
        )

    def test_a_changed_columns_to_check_forces_reprocessing(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(
            input_dir / "a.parquet",
            {"GlobalEventID": [1, 2, 3], "QuadClass": [1, None, 3], "Actor1Name": [None, "B", "C"]},
        )

        GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"]).clean_all_files()
        out_path = tmp_path / "out" / "a_cleaned.parquet"
        # QuadClass alone: only row 2 (index 1) has a NaN there.
        assert sorted(pl.read_parquet(out_path)["GlobalEventID"].to_list()) == [1, 3]

        # Rerun with a different columns_to_check must not be skipped by
        # the marker left above, and must actually re-filter by the new
        # criteria rather than leaving the stale QuadClass-only output.
        processed, failed = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["Actor1Name"]
        ).clean_all_files()

        assert (processed, failed) == (1, 0)
        assert sorted(pl.read_parquet(out_path)["GlobalEventID"].to_list()) == [2, 3]

    def test_a_changed_output_columns_forces_reprocessing(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(
            input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, 2]}
        )

        GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"], output_columns=["GlobalEventID"]
        ).clean_all_files()
        out_path = tmp_path / "out" / "a_cleaned.parquet"
        assert list(pl.read_parquet(out_path).columns) == ["GlobalEventID"]

        processed, failed = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"], output_columns=None
        ).clean_all_files()

        assert (processed, failed) == (1, 0)
        assert list(pl.read_parquet(out_path).columns) == ["GlobalEventID", "QuadClass"]

    def test_an_unchanged_config_across_reordered_columns_still_skips(self, tmp_path, caplog):
        # columns_to_check=["A", "B"] and ["B", "A"] enforce the same set;
        # config_fingerprint sorts list fields, so this must still skip.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(
            input_dir / "a.parquet", {"GlobalEventID": [1], "A": [1], "B": [1]}
        )

        GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["A", "B"]).clean_all_files()

        with caplog.at_level("DEBUG", logger="gdeltforge.cleaning.cleaner"):
            processed, failed = GDELTCleaner(
                str(input_dir), str(tmp_path / "out"), ["B", "A"]
            ).clean_all_files()

        assert (processed, failed) == (0, 0)
        assert any("Skipping already cleaned" in r.message for r in caplog.records)

    def test_a_file_that_still_errors_is_not_marked_done(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        (input_dir / "bad.parquet").write_bytes(b"not a real parquet file")

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        filt.clean_all_files()

        assert not cleaner_module.is_marked_done(
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

        GDELTCleaner(str(input_dir), str(tmp_path / "out"), []).clean_all_files()

        assert src.exists()

    def test_deletes_the_source_after_a_successful_filter(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "a.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2]})

        processed, failed = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), [], delete_source=True
        ).clean_all_files()

        assert (processed, failed) == (1, 0)
        assert not src.exists()
        assert (tmp_path / "out" / "a_cleaned.parquet").exists()

    def test_also_deletes_the_source_s_own_done_marker(self, tmp_path):
        # The marker sits next to the source parquet, not the filtered
        # output; once the source is gone it gates nothing
        # (clean_all_files' own glob can never find a deleted file
        # again), so leaving it behind is just an orphaned file
        # --delete-source's whole point was to avoid accumulating.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "a.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2]})
        marker_path = src.with_name(src.name + ".done")

        GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), [], delete_source=True
        ).clean_all_files()

        assert not marker_path.exists()

    def test_never_deletes_a_file_that_failed_to_filter(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        bad = input_dir / "bad.parquet"
        bad.write_bytes(b"not a real parquet file")

        processed, failed = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), [], delete_source=True
        ).clean_all_files()

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
            processed, failed = GDELTCleaner(
                str(input_dir), str(tmp_path / "out"), [], delete_source=True
            ).clean_all_files()

        assert (processed, failed) == (1, 0)
        assert src.exists()
        assert any(
            "Could not delete source parquet" in r.message and "a.parquet" in r.message
            for r in caplog.records
        )


class TestForce:
    """force (CLI: --force) bypasses the is_marked_done check in
    clean_all_files, so a file already marked done is reprocessed and
    its filtered output overwritten instead of skipped. Off by default."""

    def test_off_by_default_a_done_file_is_skipped(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"]).clean_all_files()
        processed, failed = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"]
        ).clean_all_files()

        assert (processed, failed) == (0, 0)

    def test_force_reprocesses_a_file_already_marked_done(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "a.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"]).clean_all_files()
        processed, failed = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"], force=True
        ).clean_all_files()

        assert (processed, failed) == (1, 0)
        assert src.exists()  # force alone does not imply delete_source


class TestDryRun:
    """dry_run (CLI: --dry-run) reports what would be filtered without
    processing anything: no worker is submitted, no output is written,
    no .done marker is created. Off by default."""

    def test_dry_run_writes_nothing_and_marks_nothing_done(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"], dry_run=True)
        processed, failed = filt.clean_all_files()

        assert (processed, failed) == (0, 0)
        assert not cleaner_module.is_marked_done(
            input_dir / "a.parquet", filt._config_fingerprint
        )
        assert not (tmp_path / "out").exists() or list((tmp_path / "out").glob("*.parquet")) == []

    @pytest.mark.parametrize("report", [False, True])
    def test_dry_run_creates_no_directory(self, tmp_path, report):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})
        (tmp_path / "hist_in").mkdir()

        GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            historical_input_folder=str(tmp_path / "hist_in"),
            historical_output_folder=str(tmp_path / "hist_out"),
            dry_run=True, report=report,
        ).clean_all_files()

        assert sorted(p.name for p in tmp_path.iterdir()) == ["hist_in", "in"]

    def test_dry_run_reports_the_would_be_processed_count_at_info(self, tmp_path, caplog):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        with caplog.at_level("INFO", logger="gdeltforge.cleaning.cleaner"):
            GDELTCleaner(
                str(input_dir), str(tmp_path / "out"), ["QuadClass"], dry_run=True
            ).clean_all_files()

        assert any(
            "[dry run] Would clean 1 flat file(s)" in r.message for r in caplog.records
        )

    def test_dry_run_sees_force_s_effect_on_the_skip_list(self, tmp_path, caplog):
        # A file already marked done is invisible to a plain dry run (it
        # would be skipped for real too), but --force --dry-run together
        # must preview it as something that WOULD be reprocessed.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"]).clean_all_files()

        with caplog.at_level("INFO", logger="gdeltforge.cleaning.cleaner"):
            processed, failed = GDELTCleaner(
                str(input_dir), str(tmp_path / "out"), ["QuadClass"], dry_run=True
            ).clean_all_files()
        assert (processed, failed) == (0, 0)
        assert any("Nothing to clean" in r.message for r in caplog.records)

        caplog.clear()
        with caplog.at_level("INFO", logger="gdeltforge.cleaning.cleaner"):
            GDELTCleaner(
                str(input_dir), str(tmp_path / "out"), ["QuadClass"], force=True, dry_run=True
            ).clean_all_files()

        assert any(
            "[dry run] Would clean 1 flat file(s)" in r.message for r in caplog.records
        )


class TestOrder:
    """order (CLI: --order) controls which file is submitted to the
    worker pool first; verified through dry_run's own per-file preview
    log, the one place the resulting order is directly observable
    without mocking ProcessPoolExecutor's internal submission order too."""

    @staticmethod
    def _write_three_flat_files(input_dir):
        input_dir.mkdir(parents=True, exist_ok=True)
        names = (
            "20200601.export.parquet", "20200101.export.parquet", "20191231.export.parquet",
        )
        for name in names:
            _write_parquet(input_dir / name, {"GlobalEventID": [1], "QuadClass": [1]})

    def test_default_order_is_ascending(self, tmp_path, caplog):
        input_dir = tmp_path / "in"
        self._write_three_flat_files(input_dir)

        with caplog.at_level("DEBUG", logger="gdeltforge.cleaning.cleaner"):
            GDELTCleaner(
                str(input_dir), str(tmp_path / "out"), ["QuadClass"], dry_run=True
            ).clean_all_files()

        would_filter = [
            r.message for r in caplog.records if r.message.startswith("[dry run]   ")
        ]
        assert would_filter == [
            "[dry run]   20191231.export.parquet",
            "[dry run]   20200101.export.parquet",
            "[dry run]   20200601.export.parquet",
        ]

    def test_desc_orders_newest_first(self, tmp_path, caplog):
        input_dir = tmp_path / "in"
        self._write_three_flat_files(input_dir)

        with caplog.at_level("DEBUG", logger="gdeltforge.cleaning.cleaner"):
            GDELTCleaner(
                str(input_dir), str(tmp_path / "out"), ["QuadClass"],
                order="desc", dry_run=True,
            ).clean_all_files()

        would_filter = [
            r.message for r in caplog.records if r.message.startswith("[dry run]   ")
        ]
        assert would_filter == [
            "[dry run]   20200601.export.parquet",
            "[dry run]   20200101.export.parquet",
            "[dry run]   20191231.export.parquet",
        ]

    def test_flat_and_historical_are_ordered_together_not_concatenated(self, tmp_path, caplog):
        # A per-group sort (flat sorted, historical sorted, then
        # concatenated) would put every flat file before every historical
        # one regardless of order. Sorting them together means desc
        # surfaces the single newest file across both groups first.
        flat_in = tmp_path / "flat_in"
        hist_in = tmp_path / "hist_in"
        flat_in.mkdir()
        part_dir = hist_in / "Year=1979"
        part_dir.mkdir(parents=True)
        _write_parquet(part_dir / "1979.parquet", {"GlobalEventID": [1], "QuadClass": [1]})
        _write_parquet(
            flat_in / "20200101.export.parquet", {"GlobalEventID": [1], "QuadClass": [1]}
        )

        with caplog.at_level("DEBUG", logger="gdeltforge.cleaning.cleaner"):
            GDELTCleaner(
                str(flat_in), str(tmp_path / "flat_out"), ["QuadClass"],
                historical_input_folder=str(hist_in),
                historical_output_folder=str(tmp_path / "hist_out"),
                order="desc", dry_run=True,
            ).clean_all_files()

        would_filter = [
            r.message for r in caplog.records if r.message.startswith("[dry run]   ")
        ]
        assert would_filter == ["[dry run]   20200101.export.parquet", "[dry run]   1979.parquet"]


class TestOutputColumns:
    def test_defaults_to_none_and_keeps_every_column(self, tmp_path):
        filt = GDELTCleaner(str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"])
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

        filt = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            output_columns=["GlobalEventID", "QuadClass"],
        )
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        filt.clean_single_file(src, out_path)

        result = pl.read_parquet(out_path)
        assert list(result.columns) == ["GlobalEventID", "QuadClass"]

    def test_a_configured_column_missing_from_the_file_is_skipped_not_fatal(
        self, tmp_path, caplog
    ):
        # Mirrors columns_to_check's existing/missing split: schema drift
        # (a column absent from one file) shouldn't crash the whole run.
        # This used to drop DoesNotExist with no trace at any log level;
        # a warning naming it now fires, matching narrow_to_available_
        # columns' own treatment of the same mistake in sample/crossref,
        # so a real typo here is no longer silently, permanently invisible.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            output_columns=["GlobalEventID", "DoesNotExist"],
        )
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        with caplog.at_level("WARNING"):
            filt.clean_single_file(src, out_path)

        result = pl.read_parquet(out_path)
        assert list(result.columns) == ["GlobalEventID"]
        assert any(
            "output_columns" in r.message and "DoesNotExist" in r.message
            for r in caplog.records
        )

    def test_no_warning_when_every_output_column_exists(self, tmp_path, caplog):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            output_columns=["GlobalEventID", "QuadClass"],
        )
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        with caplog.at_level("WARNING"):
            filt.clean_single_file(src, out_path)

        assert not any("output_columns" in r.message for r in caplog.records)

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

        filt = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["Actor1Name"],
            output_columns=["GlobalEventID", "QuadClass"],
        )
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        rows_before, rows_after = filt.clean_single_file(src, out_path)

        assert (rows_before, rows_after) == (3, 2)
        result = pl.read_parquet(out_path)
        assert sorted(result["GlobalEventID"].to_list()) == [1, 3]
        assert "Actor1Name" not in result.columns


class TestCompressionConfig:
    def test_defaults_to_zstd(self, tmp_path):
        # zstd became the default 2026-08-07: measured ~30% smaller than
        # snappy on real GDELT data at comparable or faster write speed,
        # and it's lossless, so there's no accuracy tradeoff to weigh.
        filt = GDELTCleaner(str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"])
        assert filt.compression == "zstd"

    def test_default_codec_is_used_on_write(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        filt.clean_single_file(src, out_path)

        metadata = pq.ParquetFile(out_path).metadata
        codec = metadata.row_group(0).column(0).compression
        assert codec.lower() == "zstd"

    def test_explicit_codec_overrides_the_default(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"], compression="snappy",
        )
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        filt.clean_single_file(src, out_path)

        metadata = pq.ParquetFile(out_path).metadata
        codec = metadata.row_group(0).column(0).compression
        assert codec.lower() == "snappy"


class TestFloat32Columns:
    def test_defaults_to_none_and_keeps_float64(self, tmp_path):
        filt = GDELTCleaner(str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"])
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

        filt = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            float32_columns=["AvgTone"],
        )
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        filt.clean_single_file(src, out_path)

        schema = pq.ParquetFile(out_path).schema_arrow
        assert schema.field("AvgTone").type == pa.float32()
        assert schema.field("QuadClass").type != pa.float32()

        result = pl.read_parquet(out_path)
        # The value actually changed under float32 rounding, proving the
        # cast ran rather than the column merely being typed float32 on
        # an unchanged 64-bit value. float() is required here, not
        # incidental: comparing a numpy float32 directly against a Python
        # float silently compares at float32 precision (numpy downcasts
        # the Python float rather than upcasting the float32), which
        # would make this assertion pass even without narrowing at all.
        assert float(result["AvgTone"][0]) != 0.0284010224368077

    def test_a_configured_column_missing_from_the_file_is_skipped_not_fatal(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1], "QuadClass": [1]})

        filt = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            float32_columns=["DoesNotExist"],
        )
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        # Must not raise even though the configured column isn't present.
        filt.clean_single_file(src, out_path)
        assert out_path.exists()

    def test_a_configured_non_float_column_is_skipped_not_fatal(self, tmp_path):
        # QuadClass is an int64 column; asking to float32-narrow it is a
        # misconfiguration (stale config pointed at the wrong name, a
        # renamed/retyped column, etc.), not something to force-cast.
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1], "QuadClass": [1]})

        filt = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            float32_columns=["QuadClass"],
        )
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        filt.clean_single_file(src, out_path)

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

        filt = GDELTCleaner(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"],
            output_columns=["GlobalEventID", "AvgTone"],
            float32_columns=["AvgTone"],
        )
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        filt.clean_single_file(src, out_path)

        schema = pq.ParquetFile(out_path).schema_arrow
        assert list(schema.names) == ["GlobalEventID", "AvgTone"]
        assert schema.field("AvgTone").type == pa.float32()


class TestValidateColumns:
    def test_reports_existing_and_missing_columns(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        _write_parquet(input_dir / "a.parquet", {"GlobalEventID": [1], "QuadClass": [1]})

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass", "Nope"])
        result = filt.validate_columns()

        assert result["existing_columns"] == ["QuadClass"]
        assert result["missing_columns"] == ["Nope"]

    def test_no_files_returns_error(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        result = filt.validate_columns()

        assert "error" in result


class TestRunFilterDatasetParameter:
    """run_cleaner (not GDELTCleaner itself, which is already dataset-agnostic)
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
                "cleaned_data_directory": str(events_out),
                "gkg_v2_parquet_data_directory": str(gkg_in),
                "gkg_v2_cleaned_data_directory": str(gkg_out),
            },
            "clean": {
                "columns_to_check": {
                    "gdelt_event": ["Actor1Name"],
                    "gdelt_gkg_v2": ["V2DOCUMENTIDENTIFIER"],
                },
            },
            "converter": {"partitioning": {"enabled": False}},
        }, events_in, gkg_in

    def test_defaults_to_events_for_backward_compatibility(self, tmp_path):
        cfg, events_in, _ = self._config(tmp_path)
        pl.DataFrame({
            "GlobalEventID": [1, 2], "Actor1Name": ["A", None],
        }).write_parquet(events_in / "a.parquet")

        processed, failed = run_cleaner(cfg)

        assert (processed, failed) == (1, 0)
        out = pl.read_parquet(cfg["paths"]["cleaned_data_directory"] + "/a_cleaned.parquet")
        assert out["GlobalEventID"].to_list() == [1]

    def test_non_events_dataset_reads_its_own_directory_and_check_list(self, tmp_path):
        # Actor1Name (gdelt_event's own check column) doesn't exist on the
        # GKG side at all; if run_cleaner ever fell back to gdelt_event's
        # columns_to_check by mistake, GDELTCleaner's "missing columns are
        # skipped, not fatal" behavior would silently pass both rows
        # through unfiltered instead of enforcing V2DOCUMENTIDENTIFIER, so
        # a wrong dataset resolution here would show up as len(out) == 2.
        cfg, _, gkg_in = self._config(tmp_path)
        pl.DataFrame({
            "GKGRECORDID": ["r1", "r2"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com", None],
        }).write_parquet(gkg_in / "a.parquet")

        processed, failed = run_cleaner(cfg, dataset="gdelt_gkg_v2")

        assert (processed, failed) == (1, 0)
        out_dir = cfg["paths"]["gkg_v2_cleaned_data_directory"]
        out = pl.read_parquet(out_dir + "/a_cleaned.parquet")
        assert out["GKGRECORDID"].to_list() == ["r1"]

    def test_passes_max_workers_through_to_the_filterer(self, tmp_path, monkeypatch):
        cfg, events_in, _ = self._config(tmp_path)
        cfg["clean"]["max_workers"] = 3
        pl.DataFrame(
            {"GlobalEventID": [1], "Actor1Name": ["A"]}
        ).write_parquet(events_in / "a.parquet")

        captured = {}
        real_init = GDELTCleaner.__init__

        def spy_init(self, *args, **kwargs):
            captured.update(kwargs)
            real_init(self, *args, **kwargs)

        monkeypatch.setattr(GDELTCleaner, "__init__", spy_init)

        run_cleaner(cfg)

        assert captured["max_workers"] == 3

    def test_passes_io_max_concurrent_reads_through_to_the_cleaner(self, tmp_path, monkeypatch):
        cfg, events_in, _ = self._config(tmp_path)
        cfg["io"] = {"max_concurrent_reads": 2}
        pl.DataFrame(
            {"GlobalEventID": [1], "Actor1Name": ["A"]}
        ).write_parquet(events_in / "a.parquet")

        captured = {}
        real_init = GDELTCleaner.__init__

        def spy_init(self, *args, **kwargs):
            captured.update(kwargs)
            real_init(self, *args, **kwargs)

        monkeypatch.setattr(GDELTCleaner, "__init__", spy_init)

        run_cleaner(cfg)

        assert captured["max_concurrent_reads"] == 2

    def test_max_concurrent_reads_caps_the_worker_count(self, tmp_path, monkeypatch):
        in_dir = tmp_path / "in"
        in_dir.mkdir()
        for name in ("a", "b", "c"):
            pl.DataFrame({"GlobalEventID": [1]}).write_parquet(in_dir / f"{name}.parquet")

        plans = []
        real_plan_workers = cleaner_module.plan_workers

        def recording_plan_workers(*args, **kwargs):
            plan = real_plan_workers(*args, **kwargs)
            plans.append(plan)
            return plan

        monkeypatch.setattr(cleaner_module, "plan_workers", recording_plan_workers)
        cleaner = GDELTCleaner(
            str(in_dir), str(tmp_path / "out"), columns_to_check=[],
            max_workers=4, max_concurrent_reads=2,
        )
        cleaner.clean_all_files()

        assert plans[0].workers == 2

    def test_events_reduced_resolves_historical_folders_regardless_of_partitioning(
        self, tmp_path, monkeypatch
    ):
        # gdelt_event_reduced has no flat output mode at all (see
        # converter.py's dataset_is_always_historical), so its historical
        # directories must resolve here even with converter.partitioning
        # disabled, unlike every other dataset (confirmed below:
        # gdelt_event's own historical directories stay unresolved under
        # the identical config).
        reduced_in = tmp_path / "reduced_in"
        reduced_out = tmp_path / "reduced_out"
        # Distinct from reduced_in/reduced_out (never mkdir'd here) so a
        # wrong-key resolution can't accidentally pass by reusing the
        # flat paths; still under tmp_path, not a bare absolute string,
        # since __init__ genuinely mkdir's the output one below and a
        # real filesystem-root path (this test's own original shape)
        # either silently creates a stray directory outside tmp_path on
        # a permissive machine or raises PermissionError outright on one
        # that isn't, confirmed for real once CI ran this far at all.
        reduced_hist_in = tmp_path / "reduced_hist"
        reduced_hist_out = tmp_path / "reduced_hist_out"
        reduced_in.mkdir()
        cfg = {
            "paths": {
                "event_reduced_parquet_data_directory": str(reduced_in),
                "event_reduced_cleaned_data_directory": str(reduced_out),
                "event_reduced_parquet_historical_directory": str(reduced_hist_in),
                "event_reduced_cleaned_historical_directory": str(reduced_hist_out),
                "parquet_data_directory": str(reduced_in),
                "cleaned_data_directory": str(reduced_out),
            },
            "clean": {"columns_to_check": {"gdelt_event_reduced": [], "gdelt_event": []}},
            "converter": {"partitioning": {"enabled": False}},
        }

        captured = {}
        real_init = GDELTCleaner.__init__

        def spy_init(self, *args, **kwargs):
            captured.update(kwargs)
            real_init(self, *args, **kwargs)

        monkeypatch.setattr(GDELTCleaner, "__init__", spy_init)

        run_cleaner(cfg, dataset="gdelt_event_reduced")
        assert captured["historical_input_folder"] == str(reduced_hist_in)
        assert captured["historical_output_folder"] == str(reduced_hist_out)

        run_cleaner(cfg, dataset="gdelt_event")
        assert captured["historical_input_folder"] is None
        assert captured["historical_output_folder"] is None

    def test_missing_max_workers_key_defaults_to_none(self, tmp_path):
        # config["clean"] historically had no max_workers key at all
        # (pre-dating this feature); run_cleaner must not KeyError on it.
        cfg, events_in, _ = self._config(tmp_path)
        pl.DataFrame(
            {"GlobalEventID": [1], "Actor1Name": ["A"]}
        ).write_parquet(events_in / "a.parquet")

        processed, failed = run_cleaner(cfg)

        assert (processed, failed) == (1, 0)

    def test_missing_output_columns_and_compression_keys_default_to_full_zstd(self, tmp_path):
        # Same backward-compatibility guarantee as max_workers above: configs
        # that pre-date output_columns/compression/float32_columns must not
        # KeyError, must keep writing every column, and get zstd (the
        # current default) rather than erroring for lack of an explicit
        # per-dataset override.
        cfg, events_in, _ = self._config(tmp_path)
        pl.DataFrame(
            {"GlobalEventID": [1], "Actor1Name": ["A"]}
        ).write_parquet(events_in / "a.parquet")

        processed, failed = run_cleaner(cfg)

        assert (processed, failed) == (1, 0)
        out_path = cfg["paths"]["cleaned_data_directory"] + "/a_cleaned.parquet"
        out = pl.read_parquet(out_path)
        assert list(out.columns) == ["GlobalEventID", "Actor1Name"]
        codec = pq.ParquetFile(out_path).metadata.row_group(0).column(0).compression
        assert codec.lower() == "zstd"

    def test_explicit_null_output_columns_compression_float32_columns_default_same_as_missing(
        self, tmp_path
    ):
        # filter.output_columns: (nothing typed under it yet) parses to
        # None, not {} -- a real key present with an explicit null, not
        # the "missing entirely" case above. Every one of these used to
        # crash with "'NoneType' object has no attribute 'get'" instead
        # of falling through to the same defaults as a missing key.
        cfg, events_in, _ = self._config(tmp_path)
        cfg["clean"]["output_columns"] = None
        cfg["clean"]["compression"] = None
        cfg["clean"]["float32_columns"] = None
        pl.DataFrame(
            {"GlobalEventID": [1], "Actor1Name": ["A"]}
        ).write_parquet(events_in / "a.parquet")

        processed, failed = run_cleaner(cfg)

        assert (processed, failed) == (1, 0)
        out_path = cfg["paths"]["cleaned_data_directory"] + "/a_cleaned.parquet"
        out = pl.read_parquet(out_path)
        assert list(out.columns) == ["GlobalEventID", "Actor1Name"]
        codec = pq.ParquetFile(out_path).metadata.row_group(0).column(0).compression
        assert codec.lower() == "zstd"

    def test_explicit_null_columns_to_check_is_a_no_op_same_as_empty_list(self, tmp_path):
        # filter.columns_to_check.<dataset>: (nothing typed under it yet)
        # parses to None, not []; used to crash with "'NoneType' object is
        # not iterable" on the very first file instead of behaving like
        # the documented, deliberate [] no-op.
        cfg, events_in, _ = self._config(tmp_path)
        cfg["clean"]["columns_to_check"]["gdelt_event"] = None
        pl.DataFrame(
            {"GlobalEventID": [1, 2], "Actor1Name": ["A", None]}
        ).write_parquet(events_in / "a.parquet")

        processed, failed = run_cleaner(cfg)

        assert (processed, failed) == (1, 0)
        out_path = cfg["paths"]["cleaned_data_directory"] + "/a_cleaned.parquet"
        out = pl.read_parquet(out_path)
        assert len(out) == 2

    def test_explicit_null_converter_partitioning_is_treated_as_disabled(self, tmp_path):
        # converter.partitioning: (nothing typed under it yet) parses to
        # None, not {}; used to crash with "'NoneType' object has no
        # attribute 'get'" before ever reaching a real file.
        cfg, events_in, _ = self._config(tmp_path)
        cfg["converter"]["partitioning"] = None
        pl.DataFrame(
            {"GlobalEventID": [1], "Actor1Name": ["A"]}
        ).write_parquet(events_in / "a.parquet")

        processed, failed = run_cleaner(cfg)

        assert (processed, failed) == (1, 0)

    def test_output_columns_and_compression_are_resolved_per_dataset(self, tmp_path):
        cfg, _, gkg_in = self._config(tmp_path)
        cfg["clean"]["output_columns"] = {
            "gdelt_gkg_v2": ["GKGRECORDID", "V2DOCUMENTIDENTIFIER"],
        }
        cfg["clean"]["compression"] = {"gdelt_gkg_v2": "zstd"}
        pl.DataFrame({
            "GKGRECORDID": ["r1", "r2"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com", "http://b.com"],
            "V2GCAM": ["unused", "unused"],
        }).write_parquet(gkg_in / "a.parquet")

        processed, failed = run_cleaner(cfg, dataset="gdelt_gkg_v2")

        assert (processed, failed) == (1, 0)
        out_path = Path(cfg["paths"]["gkg_v2_cleaned_data_directory"]) / "a_cleaned.parquet"
        out = pl.read_parquet(out_path)
        assert list(out.columns) == ["GKGRECORDID", "V2DOCUMENTIDENTIFIER"]

        codec = pq.ParquetFile(out_path).metadata.row_group(0).column(0).compression
        assert codec.lower() == "zstd"

    def test_float32_columns_is_resolved_per_dataset(self, tmp_path):
        cfg, events_in, gkg_in = self._config(tmp_path)
        cfg["clean"]["float32_columns"] = {"gdelt_gkg_v2": ["Tone"]}
        pl.DataFrame({
            "GKGRECORDID": ["r1"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com"],
            "Tone": [1.5],
        }).write_parquet(gkg_in / "a.parquet")
        pl.DataFrame({
            "GlobalEventID": [1], "Actor1Name": ["A"], "GoldsteinScale": [2.8],
        }).write_parquet(events_in / "a.parquet")

        # events_in has no float32_columns entry configured for it, so it
        # must be unaffected by gdelt_gkg_v2's setting.
        processed_events, _ = run_cleaner(cfg)
        processed_gkg, failed_gkg = run_cleaner(cfg, dataset="gdelt_gkg_v2")

        assert (processed_events, processed_gkg, failed_gkg) == (1, 1, 0)

        events_schema = pq.ParquetFile(
            cfg["paths"]["cleaned_data_directory"] + "/a_cleaned.parquet"
        ).schema_arrow
        assert events_schema.field("GoldsteinScale").type != pa.float32()

        gkg_schema = pq.ParquetFile(
            Path(cfg["paths"]["gkg_v2_cleaned_data_directory"]) / "a_cleaned.parquet"
        ).schema_arrow
        assert gkg_schema.field("Tone").type == pa.float32()


class TestCrossrefJoinKeyWarning:
    """output_columns makes it easy to prune a dataset's crossref join
    key by accident (see gdeltforge.crossref.crossref.REQUIRED_JOIN_COLUMNS);
    run_cleaner should warn about it at filter time rather than let the
    failure surface only when `crossref` is run later, possibly after an
    expensive sample pass in between."""

    def test_warns_when_output_columns_omits_the_join_key(self, tmp_path, caplog):
        cfg, _, gkg_in = TestRunFilterDatasetParameter._config(tmp_path)
        cfg["clean"]["output_columns"] = {
            # Missing V2DOCUMENTIDENTIFIER, gdelt_gkg_v2's join key.
            "gdelt_gkg_v2": ["GKGRECORDID"],
        }
        pl.DataFrame({
            "GKGRECORDID": ["r1"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com"],
        }).write_parquet(gkg_in / "a.parquet")

        with caplog.at_level("WARNING"):
            run_cleaner(cfg, dataset="gdelt_gkg_v2")

        assert any(
            "V2DOCUMENTIDENTIFIER" in r.message and "crossref" in r.message
            for r in caplog.records
        )

    def test_no_warning_when_the_join_key_is_kept(self, tmp_path, caplog):
        cfg, _, gkg_in = TestRunFilterDatasetParameter._config(tmp_path)
        cfg["clean"]["output_columns"] = {
            "gdelt_gkg_v2": ["GKGRECORDID", "V2DOCUMENTIDENTIFIER"],
        }
        pl.DataFrame({
            "GKGRECORDID": ["r1"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com"],
        }).write_parquet(gkg_in / "a.parquet")

        with caplog.at_level("WARNING"):
            run_cleaner(cfg, dataset="gdelt_gkg_v2")

        assert not any("crossref" in r.message for r in caplog.records)

    def test_no_warning_when_output_columns_is_unset(self, tmp_path, caplog):
        # output_columns=None means every column survives; nothing to warn
        # about even though gdelt_gkg_v2 does have a required join key.
        cfg, _, gkg_in = TestRunFilterDatasetParameter._config(tmp_path)
        pl.DataFrame({
            "GKGRECORDID": ["r1"],
            "V2DOCUMENTIDENTIFIER": ["http://a.com"],
        }).write_parquet(gkg_in / "a.parquet")

        with caplog.at_level("WARNING"):
            run_cleaner(cfg, dataset="gdelt_gkg_v2")

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
        pl.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).write_parquet(
            events_in / "a.parquet"
        )

        with caplog.at_level("WARNING"):
            run_cleaner(cfg, delete_source=True)

        assert any(
            "delete_source" in r.message and "columns_to_check" in r.message
            for r in caplog.records
        )

    def test_no_warning_when_delete_source_is_false(self, tmp_path, caplog):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pl.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).write_parquet(
            events_in / "a.parquet"
        )

        with caplog.at_level("WARNING"):
            run_cleaner(cfg, delete_source=False)

        assert not any("delete_source" in r.message for r in caplog.records)

    def test_no_warning_when_nothing_narrows_the_output(self, tmp_path, caplog):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        cfg["clean"]["columns_to_check"]["gdelt_event"] = []
        pl.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).write_parquet(
            events_in / "a.parquet"
        )

        with caplog.at_level("WARNING"):
            run_cleaner(cfg, delete_source=True)

        assert not any("delete_source" in r.message for r in caplog.records)


class TestVerboseLogging:
    """--verbose raises this module's own logger to DEBUG, revealing the
    per-file "{name}: rows -> rows"/"Skipping already cleaned" lines
    that are DEBUG-level (invisible) by default. logger.setLevel is a
    real, process-wide mutation on a singleton (logging.getLogger caches
    by name), so every test here restores INFO afterward regardless of
    outcome, rather than leaking state into whichever test runs next."""

    def test_off_by_default_logger_level_is_unchanged(self, tmp_path):
        cleaner_module.logger.setLevel(logging.INFO)
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pl.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).write_parquet(
            events_in / "a.parquet"
        )

        run_cleaner(cfg)

        assert cleaner_module.logger.level == logging.INFO

    def test_verbose_lowers_the_logger_to_debug(self, tmp_path):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pl.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).write_parquet(
            events_in / "a.parquet"
        )
        try:
            run_cleaner(cfg, verbose=True)
            assert cleaner_module.logger.level == logging.DEBUG
        finally:
            cleaner_module.logger.setLevel(logging.INFO)

    def test_verbose_reveals_the_per_file_row_count_line(self, tmp_path, caplog):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pl.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).write_parquet(
            events_in / "a.parquet"
        )
        try:
            with caplog.at_level("DEBUG", logger="gdeltforge.cleaning.cleaner"):
                run_cleaner(cfg, verbose=True)
            assert any("a.parquet" in r.message and "rows" in r.message for r in caplog.records)
        finally:
            cleaner_module.logger.setLevel(logging.INFO)

    def test_warns_for_gkg_v1_counts_too(self, tmp_path, caplog):
        # gdelt_gkg_v1_counts is a real, distinct crossref target (the
        # `crossref --gkg-version v1-counts` path) with its own entry in
        # REQUIRED_JOIN_COLUMNS, not just an alias of gdelt_gkg_v1.
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        cfg["paths"]["gkg_v1_counts_parquet_data_directory"] = str(events_in)
        cfg["paths"]["gkg_v1_counts_cleaned_data_directory"] = str(tmp_path / "out")
        cfg["clean"]["columns_to_check"]["gdelt_gkg_v1_counts"] = ["Date"]
        # Missing EventIds, gdelt_gkg_v1_counts' join key.
        cfg["clean"]["output_columns"] = {"gdelt_gkg_v1_counts": ["Date"]}
        pl.DataFrame({"Date": [20130401], "EventIds": ["1,2"]}).write_parquet(
            events_in / "a.parquet"
        )

        with caplog.at_level("WARNING"):
            run_cleaner(cfg, dataset="gdelt_gkg_v1_counts")

        assert any(
            "EventIds" in r.message and "crossref" in r.message for r in caplog.records
        )


class TestQuietLogging:
    """--quiet raises this module's own logger to WARNING, suppressing
    the setup/summary lines run_cleaner otherwise always logs at INFO.
    Mutually exclusive with --verbose at the CLI; this module doesn't
    enforce that itself, so it isn't re-tested here."""

    def test_quiet_raises_the_logger_to_warning(self, tmp_path):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pl.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).write_parquet(
            events_in / "a.parquet"
        )
        try:
            run_cleaner(cfg, quiet=True)
            assert cleaner_module.logger.level == logging.WARNING
        finally:
            cleaner_module.logger.setLevel(logging.INFO)

    def test_quiet_suppresses_the_summary_line(self, tmp_path, caplog):
        cfg, events_in, _ = TestRunFilterDatasetParameter._config(tmp_path)
        pl.DataFrame({"GlobalEventID": [1], "Actor1Name": ["A"]}).write_parquet(
            events_in / "a.parquet"
        )
        try:
            with caplog.at_level("DEBUG", logger="gdeltforge.cleaning.cleaner"):
                run_cleaner(cfg, quiet=True)
            assert not any("CLEANING SUMMARY" in r.message for r in caplog.records)
        finally:
            cleaner_module.logger.setLevel(logging.INFO)


class TestFilterSingleFileAtomicity:
    """clean_single_file used to write straight to output_path via a
    streaming ParquetWriter. Now that clean_all_files runs files across a
    worker pool (TestMaxWorkersConfig), a killed worker is a real,
    reachable failure mode, not just a hypothetical: a truncated file
    left at output_path would be silently picked up by anything reading
    the filtered directory afterwards. Now writes through a temp file and
    only renames into place once the stream completes, matching the same
    pattern already used for converter output."""

    def test_leaves_no_file_on_write_failure(self, tmp_path, monkeypatch):
        # PID-pinned: clean_single_file's own staging name is now PID-
        # suffixed (the fix for two concurrent filter runs sharing one
        # output path), so the leftover-tmp check below has to know the
        # exact name to look for.
        monkeypatch.setattr(os, "getpid", lambda: 12345)
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        def boom(self, *args, **kwargs):
            raise OSError("simulated crash mid-write")

        monkeypatch.setattr(pl.LazyFrame, "sink_parquet", boom)

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        out_path = tmp_path / "out" / "data_cleaned.parquet"

        with pytest.raises(OSError):
            filt.clean_single_file(src, out_path)

        assert not out_path.exists()
        assert not out_path.with_name(f"{out_path.name}.12345.tmp").exists()

    def test_successful_write_leaves_no_tmp_behind(self, tmp_path, monkeypatch):
        monkeypatch.setattr(os, "getpid", lambda: 12345)
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTCleaner(str(input_dir), str(tmp_path / "out"), ["QuadClass"])
        out_path = tmp_path / "out" / "data_cleaned.parquet"
        filt.clean_single_file(src, out_path)

        assert out_path.exists()
        assert not out_path.with_name(f"{out_path.name}.12345.tmp").exists()
        assert pq.read_table(out_path).num_rows == 2


class TestFilterSingleFileConcurrentInvocations:
    """
    clean_single_file used to build its own fixed ".tmp" suffix inline
    (tmp_path = output_path.with_name(output_path.name + ".tmp")),
    unlike utils.io.write_parquet_atomic's already-fixed equivalent. Two
    concurrent GDELTCleaner instances filtering the same source file into
    the same output path raced on that shared name: whichever process's
    os.replace() ran second found its own tmp file already renamed away
    by the other, raising a raw FileNotFoundError and counting an
    otherwise-successful file as "failed" (e.g. two overlapping
    `gdeltforge filter --dataset events --force` runs, one launched
    before realizing the first was still going).

    clean_single_file streams through lf.sink_parquet rather than
    materializing a DataFrame first, specifically to keep peak memory
    bounded for a large file, so it can't route through
    write_parquet_atomic (which writes an already-collected DataFrame)
    the way _write_partition_file now does; it needs its own PID-suffixed
    name instead.

    A genuine two-process race turned out to be a poor fit for a fast,
    reliable unit test: two real OS processes started at the same
    instant via a multiprocessing.Barrier reproduced the bug 0/8 times on
    this platform, since a rename of a small/medium file is fast enough
    relative to process-wake scheduling jitter that one side routinely
    finishes its entire write-then-rename before the other even starts,
    even though the exact same code, against real live data, reproduced
    it in a live QA pass. Instead, "process B" is run to completion from
    inside a patched sink_parquet, right after "process A"'s own write
    lands on disk but before A's rename runs, deterministically forcing
    the interleaving that a real race only produces by chance: A's
    os.replace() now always runs against whatever B's own run already
    did to the filesystem, regardless of platform timing. Distinct
    os.getpid() values (mocked) are what the fix keys its own tmp-name
    uniqueness on, the same as two genuinely separate OS processes.
    """

    def test_two_concurrent_filters_of_the_same_file_do_not_race(
        self, tmp_path, monkeypatch
    ):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2, 3, 4], "QuadClass": [1, 2, 3, 4]})
        out_dir = tmp_path / "out"
        out_path = out_dir / "data_cleaned.parquet"

        filt_a = GDELTCleaner(str(input_dir), str(out_dir), ["QuadClass"])
        filt_b = GDELTCleaner(str(input_dir), str(out_dir), ["QuadClass"])

        pids = iter([111, 222, 333, 444])
        monkeypatch.setattr(os, "getpid", lambda: next(pids))

        real_sink_parquet = pl.LazyFrame.sink_parquet
        state = {"ran_b": False}

        def sink_parquet_then_run_b(self, path, *args, **kwargs):
            result = real_sink_parquet(self, path, *args, **kwargs)
            if not state["ran_b"]:
                state["ran_b"] = True
                filt_b.clean_single_file(src, out_path)
            return result

        monkeypatch.setattr(pl.LazyFrame, "sink_parquet", sink_parquet_then_run_b)

        # Must not raise: under the bug, B's completed rename (using the
        # same shared tmp name A just wrote to) leaves nothing at A's own
        # tmp path by the time A's own os.replace() runs.
        filt_a.clean_single_file(src, out_path)

        assert out_path.exists()
        assert len(pl.read_parquet(out_path)) == 4


class TestLegacyOutputRemoval:
    def test_pre_012_output_of_the_same_source_is_replaced(self, tmp_path):
        # Before 0.12 the stage wrote <stem>_filtered.parquet; leaving it next
        # to the new <stem>_cleaned.parquet would double that day's rows for
        # every reader that globs *.parquet.
        in_dir, out_dir = tmp_path / "in", tmp_path / "out"
        in_dir.mkdir()
        out_dir.mkdir()
        pl.DataFrame({"GlobalEventID": [1, 2]}).write_parquet(in_dir / "20200101.parquet")
        pl.DataFrame({"GlobalEventID": [1, 2]}).write_parquet(out_dir / "20200101_filtered.parquet")
        pl.DataFrame({"GlobalEventID": [9]}).write_parquet(out_dir / "20200102_filtered.parquet")

        GDELTCleaner(str(in_dir), str(out_dir), columns_to_check=[]).clean_all_files()

        assert (out_dir / "20200101_cleaned.parquet").exists()
        assert not (out_dir / "20200101_filtered.parquet").exists()
        # A legacy file whose source wasn't cleaned in this run is left alone.
        assert (out_dir / "20200102_filtered.parquet").exists()


class TestDeprecatedModule:
    def test_old_import_path_warns_and_still_works(self, tmp_path):
        import importlib
        import sys as _sys

        _sys.modules.pop("gdeltforge.filtering.filter", None)
        with pytest.warns(DeprecationWarning, match="gdeltforge.filtering.filter is deprecated"):
            old = importlib.import_module("gdeltforge.filtering.filter")
        assert issubclass(old.GDELTFilter, GDELTCleaner)
        assert old.run_filter is run_cleaner


class TestRefusesToWriteIntoItsInput:
    """Safeguard: cleaned output must never land beside the converted files
    it came from."""

    def test_same_directory_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="would write into its own input"):
            GDELTCleaner(str(tmp_path), str(tmp_path), columns_to_check=[])

    def test_directory_inside_the_input_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="would write into its own input"):
            GDELTCleaner(str(tmp_path), str(tmp_path / "cleaned"), columns_to_check=[])

    def test_historical_output_inside_flat_input_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="would write into its own input"):
            GDELTCleaner(
                str(tmp_path / "parquet"), str(tmp_path / "cleaned"), columns_to_check=[],
                historical_input_folder=str(tmp_path / "hist"),
                historical_output_folder=str(tmp_path / "parquet" / "hist_cleaned"),
            )

    def test_nothing_is_created_before_refusing(self, tmp_path):
        with pytest.raises(ValueError):
            GDELTCleaner(str(tmp_path), str(tmp_path / "cleaned"), columns_to_check=[])
        assert not (tmp_path / "cleaned").exists()

    def test_sibling_directories_are_fine(self, tmp_path):
        GDELTCleaner(str(tmp_path / "parquet"), str(tmp_path / "cleaned"), columns_to_check=[])


class TestCleanedFileMarker:
    def test_every_cleaned_file_says_it_is_cleaned(self, tmp_path):
        from gdeltforge.utils.io import cleaned_marker

        in_dir = tmp_path / "in"
        in_dir.mkdir()
        pl.DataFrame({"GlobalEventID": [1], "Actor1Code": ["USA"]}).write_parquet(
            in_dir / "20200101.parquet"
        )
        GDELTCleaner(
            str(in_dir), str(tmp_path / "out"), columns_to_check=["Actor1Code"]
        ).clean_all_files()

        marker = cleaned_marker(tmp_path / "out" / "20200101_cleaned.parquet")
        assert marker is not None
        assert marker["source"] == "20200101.parquet"
        assert marker["steps"] == [
            {"step": "require", "lossy": True, "columns": ["Actor1Code"]}
        ]
        # The converted input carries no marker.
        assert cleaned_marker(in_dir / "20200101.parquet") is None

    def test_input_that_is_already_cleaned_is_warned_about(self, tmp_path, caplog):
        in_dir = tmp_path / "in"
        in_dir.mkdir()
        pl.DataFrame({"GlobalEventID": [1]}).write_parquet(in_dir / "20200101.parquet")
        GDELTCleaner(str(in_dir), str(tmp_path / "out"), columns_to_check=[]).clean_all_files()

        with caplog.at_level(logging.WARNING):
            GDELTCleaner(
                str(tmp_path / "out"), str(tmp_path / "again"), columns_to_check=[]
            ).clean_all_files()
        assert any("carries the clean stage's marker" in r.message for r in caplog.records)


class TestRunAudit:
    def _run(self, tmp_path, **kwargs):
        in_dir = tmp_path / "in"
        in_dir.mkdir()
        pl.DataFrame({
            "GlobalEventID": [1, 2, 3],
            "EventRootCode": ["01", "99", None],
            "Actor1Code": ["USA", "USA", None],
        }).write_parquet(in_dir / "20200101.parquet")
        out = tmp_path / "out"
        GDELTCleaner(
            str(in_dir), str(out), columns_to_check=["Actor1Code"], **kwargs
        ).clean_all_files()
        return out

    @staticmethod
    def _audit(out):
        return next((out.parent / f"{out.name}_runs").glob("*.parquet"))

    def test_one_audit_file_per_run_with_one_row_per_file(self, tmp_path):
        self._run(tmp_path)
        audits = list((tmp_path / "out_runs").glob("*.parquet"))
        assert len(audits) == 1
        audit = pl.read_parquet(audits[0])
        assert audit.select("source", "output", "rows_in", "rows_out").rows() == [
            ("20200101.parquet", "20200101_cleaned.parquet", 3, 2)
        ]

    def test_counts_unrecognized_codes_per_coded_column(self, tmp_path):
        # "99" isn't a CAMEO event root; the null is not counted.
        out = self._run(tmp_path)
        audit = pl.read_parquet(self._audit(out))
        assert audit["unrecognized.EventRootCode"].to_list() == [1]

    def test_run_settings_are_in_the_audit_metadata(self, tmp_path):
        out = self._run(tmp_path)
        meta = pl.read_parquet_metadata(self._audit(out))
        run = json.loads(meta["gdeltforge:clean-run"])
        assert run["failed"] == []
        assert run["steps"][0]["step"] == "require"

    def test_an_empty_null_check_is_not_listed(self, tmp_path):
        from gdeltforge.utils.io import cleaned_marker

        in_dir = tmp_path / "in"
        in_dir.mkdir()
        pl.DataFrame({"GlobalEventID": [1]}).write_parquet(in_dir / "a.parquet")
        cleaner = GDELTCleaner(str(in_dir), str(tmp_path / "out"), columns_to_check=[])
        cleaner.clean_all_files()
        marker = cleaned_marker(tmp_path / "out" / "a_cleaned.parquet")
        assert marker is not None
        assert marker["steps"] == []

    def test_dry_run_writes_no_audit(self, tmp_path):
        self._run(tmp_path, dry_run=True)
        assert not (tmp_path / "out_runs").exists()

    def test_audit_is_never_read_as_data(self, tmp_path):
        from gdeltforge.utils.io import read_parquet_path

        out = self._run(tmp_path)
        assert read_parquet_path(out).height == 2
        # polars reads every subdirectory of a directory, `_`-prefixed ones
        # included, so the audit must sit outside the cleaned directory.
        assert pl.read_parquet(out).height == 2
        assert pl.scan_parquet(out).select(pl.len()).collect().item() == 2

    def test_configured_runs_folder_is_used(self, tmp_path):
        runs = tmp_path / "audits" / "events"
        self._run(tmp_path, runs_folder=str(runs))
        assert len(list(runs.glob("*.parquet"))) == 1
        assert not (tmp_path / "out_runs").exists()

    @pytest.mark.parametrize("where", ["out", "out/sub", "in", "in/sub"])
    def test_runs_folder_inside_a_data_directory_is_refused(self, tmp_path, where):
        (tmp_path / "in").mkdir()
        with pytest.raises(ValueError, match="run audit directory"):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                runs_folder=str(tmp_path / where),
            )

    def test_runs_folder_inside_another_datasets_data_is_refused(self, tmp_path):
        from gdeltforge.utils.config import _bundled_default_dict

        cfg = _bundled_default_dict()
        (tmp_path / "mentions").mkdir()
        cfg["paths"]["mentions_parquet_data_directory"] = str(tmp_path / "mentions")
        cfg["paths"]["mentions_cleaned_data_directory"] = str(tmp_path / "mentions_clean")
        cfg["paths"]["parquet_data_directory"] = str(tmp_path / "events")
        cfg["paths"]["mentions_clean_runs_directory"] = str(tmp_path / "events" / "audits")
        with pytest.raises(ValueError, match="run audit directory"):
            run_cleaner(cfg, dataset="gdelt_mentions")
        assert not (tmp_path / "events").exists()

    def test_run_cleaner_reads_the_dataset_runs_key(self, tmp_path, monkeypatch):
        captured = {}
        real_init = GDELTCleaner.__init__

        def spy_init(self, *args, **kwargs):
            captured.update(kwargs)
            real_init(self, *args, **kwargs)

        monkeypatch.setattr(GDELTCleaner, "__init__", spy_init)
        (tmp_path / "gkg").mkdir()
        cfg = {
            "paths": {
                "gkg_v2_parquet_data_directory": str(tmp_path / "gkg"),
                "gkg_v2_cleaned_data_directory": str(tmp_path / "gkg_cleaned"),
                "gkg_v2_clean_runs_directory": str(tmp_path / "gkg_audits"),
            },
            "clean": {"columns_to_check": {"gdelt_gkg_v2": []}},
        }
        run_cleaner(cfg, dataset="gdelt_gkg_v2")
        assert captured["runs_folder"] == str(tmp_path / "gkg_audits")


DEFAULT_ERRATA = {"date_1920": True, "keep_original": True, "event_markers": "keep"}


def _write_new_year_2020(in_dir):
    # Three rows as GDELT published them on 2020-01-02: two dated 1920 by
    # its year bug, one genuine 2019 date; one row is a CAMEO null code.
    in_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "GlobalEventID": [1, 2, 3],
        "Day": [19200101, 19200102, 20191226],
        "MonthYear": [192001, 192001, 201912],
        "Year": [1920, 1920, 2019],
        "FractionDate": [1920.0027, 1920.0055, 2019.9808],
        "DATEADDED": [20200102, 20200102, 20200102],
        "EventCode": ["010", "---", "190"],
        "EventBaseCode": ["010", "---", "190"],
        "EventRootCode": ["01", "--", "19"],
    }).write_parquet(in_dir / "20200102.export.parquet")


class TestErrata:
    def test_output_columns_leaving_out_the_originals_makes_the_repair_lossy(
        self, tmp_path, caplog
    ):
        from gdeltforge.utils.io import cleaned_marker

        _write_new_year_2020(tmp_path / "in")
        with caplog.at_level(logging.WARNING):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                errata=DEFAULT_ERRATA, output_columns=["GlobalEventID", "Day"],
            ).clean_all_files()
        assert any("leaves out ['Day_original'" in r.message for r in caplog.records)
        marker = cleaned_marker(tmp_path / "out" / "20200102.export_cleaned.parquet")
        assert marker is not None
        step = next(s for s in marker["steps"] if s.get("rule") == "date_1920")
        assert step["lossy"] is True and step["originals_left_out"] is True
        with pytest.raises(ValueError, match="left out of output_columns"):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out2"), columns_to_check=[],
                errata=DEFAULT_ERRATA, output_columns=["GlobalEventID", "Day"],
                delete_source=True,
            )

    def test_output_columns_listing_the_originals_keeps_the_repair_lossless(
        self, tmp_path, caplog
    ):
        cols = ["GlobalEventID", "Day", "Day_original", "MonthYear_original",
                "Year_original", "FractionDate_original"]
        _write_new_year_2020(tmp_path / "in")
        with caplog.at_level(logging.WARNING):
            cleaner = GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                errata=DEFAULT_ERRATA, output_columns=cols,
            )
        assert not any("leaves out" in r.message for r in caplog.records)
        assert not next(s for s in cleaner.steps if isinstance(s, Date1920Repair)).lossy

    @pytest.mark.parametrize("dropped", ["DATEADDED", "Day"])
    @pytest.mark.parametrize("dry_run", [False, True])
    def test_a_file_the_repair_cant_read_is_warned_about(
        self, tmp_path, caplog, dropped, dry_run
    ):
        _write_new_year_2020(tmp_path / "in")
        path = tmp_path / "in" / "20200102.export.parquet"
        pl.read_parquet(path).drop(dropped).write_parquet(path)
        with caplog.at_level(logging.WARNING):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                errata=DEFAULT_ERRATA, dry_run=dry_run, report=dry_run,
            ).clean_all_files()
        assert any(
            "errata.date_1920 didn't run on 1 file(s)" in r.message for r in caplog.records
        )

    def test_no_warning_when_every_file_has_the_columns(self, tmp_path, caplog):
        _write_new_year_2020(tmp_path / "in")
        with caplog.at_level(logging.WARNING):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                errata=DEFAULT_ERRATA,
            ).clean_all_files()
        assert not any("didn't run" in r.message for r in caplog.records)

    def test_a_cleaned_directory_reads_as_one_dataset(self, tmp_path):
        # A window file and a file from another year: both carry the four
        # *_original columns, so polars reads the directory in one go and
        # pandas/pyarrow readers don't lose them.
        _write_new_year_2020(tmp_path / "in")
        pl.DataFrame({
            "GlobalEventID": [4], "Day": [20210101], "MonthYear": [202101],
            "Year": [2021], "FractionDate": [2021.0027], "DATEADDED": [20210101],
            "EventCode": ["010"], "EventBaseCode": ["010"], "EventRootCode": ["01"],
        }).write_parquet(tmp_path / "in" / "20210101.export.parquet")
        out_dir = tmp_path / "out"
        GDELTCleaner(
            str(tmp_path / "in"), str(out_dir), columns_to_check=[], errata=DEFAULT_ERRATA
        ).clean_all_files()
        out = pl.read_parquet(out_dir).sort("GlobalEventID")
        assert out["Day_original"].to_list() == [19200101, 19200102, None, None]
        assert out["Day"].to_list() == [20200101, 20200102, 20191226, 20210101]

    def test_default_settings_repair_the_dates_and_keep_every_value(self, tmp_path):
        _write_new_year_2020(tmp_path / "in")
        out_dir = tmp_path / "out"
        GDELTCleaner(
            str(tmp_path / "in"), str(out_dir), columns_to_check=[], errata=DEFAULT_ERRATA
        ).clean_all_files()

        out = pl.read_parquet(out_dir / "20200102.export_cleaned.parquet").sort("GlobalEventID")
        assert out["Day"].to_list() == [20200101, 20200102, 20191226]
        assert out["Day_original"].to_list() == [19200101, 19200102, None]
        # Markers are kept by default: nothing is lost.
        assert out.height == 3

        audit = pl.read_parquet(next((out_dir.parent / f"{out_dir.name}_runs").glob("*.parquet")))
        assert audit["errata.date_1920"].to_list() == [2]
        assert audit["errata.event_markers_keep"].to_list() == [1]

    def test_no_errata_setting_leaves_gdelts_values(self, tmp_path):
        _write_new_year_2020(tmp_path / "in")
        GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[]
        ).clean_all_files()
        out = pl.read_parquet(tmp_path / "out" / "20200102.export_cleaned.parquet")
        assert sorted(out["Day"].to_list()) == [19200101, 19200102, 20191226]

    def test_dropping_markers_removes_their_rows(self, tmp_path):
        _write_new_year_2020(tmp_path / "in")
        GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
            errata={"event_markers": "drop"},
        ).clean_all_files()
        out = pl.read_parquet(tmp_path / "out" / "20200102.export_cleaned.parquet")
        assert "---" not in out["EventCode"].to_list()

    @pytest.mark.parametrize("errata, message", [
        ({"date_1921": True}, "unknown setting"),
        ({"event_markers": "flag"}, "must be one of"),
        ({"date_1920": "yes"}, "must be true or false"),
    ])
    def test_invalid_settings_fail_before_anything_runs(self, tmp_path, errata, message):
        with pytest.raises(ValueError, match=message):
            GDELTCleaner(str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                         errata=errata)
        assert not (tmp_path / "out").exists()

    def test_changing_errata_settings_cleans_every_file_again(self, tmp_path):
        _write_new_year_2020(tmp_path / "in")
        GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[]
        ).clean_all_files()
        processed, _ = GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
            errata=DEFAULT_ERRATA,
        ).clean_all_files()
        assert processed == 1


class TestMisappliedCleanSettings:
    """Settings that used to be ignored without a word, or to fail only
    once files were being read, are caught before any file is touched."""

    @staticmethod
    def _config(tmp_path, **clean):
        from gdeltforge.utils.config import _bundled_default_dict

        cfg = _bundled_default_dict()
        (tmp_path / "in").mkdir(exist_ok=True)
        for dataset in ("gdelt_event", "gdelt_mentions"):
            prefix = "" if dataset == "gdelt_event" else "mentions_"
            cfg["paths"][f"{prefix}parquet_data_directory"] = str(tmp_path / "in")
            cfg["paths"][f"{prefix}cleaned_data_directory"] = str(tmp_path / "out")
        cfg["clean"].update(clean)
        return cfg

    @pytest.mark.parametrize("clean, dataset, message", [
        ({"derive": {"gdelt_evnt": {"event_date": True}}}, "gdelt_event",
         "clean.derive.gdelt_evnt: not a dataset"),
        ({"derive": {"gdelt_event": True}}, "gdelt_event",
         "clean.derive.gdelt_event must be a mapping"),
        ({"errata": {"gdelt_mentions": {"event_markers": "drop"}}}, "gdelt_mentions",
         "clean.errata.gdelt_mentions: the known GDELT errors"),
        ({"derive": {"gdelt_mentions": {"event_date": True}}}, "gdelt_mentions",
         "gdelt_mentions has no Day column"),
        ({"derive": {"gdelt_mentions": {"labels": ["EventRootCode"]}}}, "gdelt_mentions",
         "aren't columns of gdelt_mentions"),
        ({"derive": {"gdelt_event": {"labels": ["EventRootCode", "EventRootCode"]}}},
         "gdelt_event", r"clean\.derive\.gdelt_event\.labels lists \['EventRootCode'\]"),
        ({"derive": {"gdelt_event": {"labels": "EventRootCode"}}}, "gdelt_event",
         "labels must be a list of column names"),
        ({"errata": {"gdelt_event": None}}, "gdelt_event",
         r"clean\.errata\.gdelt_event is empty \(null\)"),
        ({"errata": None}, "gdelt_event", r"clean\.errata is empty \(null\)"),
        ({"normalize": {"gdelt_event": None}}, "gdelt_event", "is empty"),
    ])
    def test_fails_before_reading_any_file(self, tmp_path, clean, dataset, message):
        with pytest.raises(ValueError, match=message):
            run_cleaner(self._config(tmp_path, **clean), dataset=dataset)
        assert not (tmp_path / "out").exists()

    def test_an_unknown_key_under_clean_warns(self, tmp_path, caplog):
        cfg = self._config(tmp_path, allow_lossy_delete_sources=True)
        with caplog.at_level(logging.WARNING):
            run_cleaner(cfg, dataset="gdelt_event")
        assert any(
            "unknown setting(s) ['allow_lossy_delete_sources']" in r.message
            for r in caplog.records
        )


class TestDeleteSourceRefusesLossySteps:
    def test_the_refusal_suggests_what_applies(self, tmp_path):
        with pytest.raises(ValueError) as err:
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                normalize={"trim_strings": True}, delete_source=True,
            )
        assert "turn normalize off" in str(err.value)
        assert "event_markers: keep" not in str(err.value)

    @pytest.mark.parametrize("value", ["no", "false", "true", 1, 0])
    def test_the_opt_in_accepts_only_true_or_false(self, tmp_path, value):
        # A quoted "no" used to count as true and let --delete-source
        # remove the converted copy under a lossy step.
        with pytest.raises(ValueError, match="allow_lossy_delete_source must be true or false"):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                errata={"event_markers": "drop"}, delete_source=True,
                allow_lossy_delete_source=value,
            )

    def test_a_refused_run_logs_no_data_loss_warning_first(self, tmp_path, caplog):
        (tmp_path / "in").mkdir()
        cfg = {
            "paths": {
                "parquet_data_directory": str(tmp_path / "in"),
                "cleaned_data_directory": str(tmp_path / "out"),
            },
            "clean": {
                "columns_to_check": {"gdelt_event": ["Actor1Code"]},
                "errata": {"gdelt_event": {"event_markers": "drop"}},
            },
        }
        with caplog.at_level(logging.WARNING), pytest.raises(ValueError, match="lossy"):
            run_cleaner(cfg, delete_source=True)
        assert not any("delete_source is set" in r.message for r in caplog.records)

    def test_run_cleaner_rejects_a_string_opt_in(self, tmp_path):
        (tmp_path / "in").mkdir()
        cfg = {
            "paths": {
                "parquet_data_directory": str(tmp_path / "in"),
                "cleaned_data_directory": str(tmp_path / "out"),
            },
            "clean": {
                "columns_to_check": {"gdelt_event": []},
                "errata": {"gdelt_event": {"event_markers": "drop"}},
                "allow_lossy_delete_source": "no",
            },
        }
        with pytest.raises(ValueError, match="allow_lossy_delete_source must be true or false"):
            run_cleaner(cfg, delete_source=True)
        assert (tmp_path / "in").exists()

    def test_refused_while_a_new_lossy_step_is_on(self, tmp_path):
        with pytest.raises(ValueError, match="errata.event_markers: drop"):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                errata={"event_markers": "drop"}, delete_source=True,
            )

    def test_a_repair_without_originals_is_lossy_too(self, tmp_path):
        with pytest.raises(ValueError, match="date_1920 without keep_original"):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                errata={"date_1920": True, "keep_original": False}, delete_source=True,
            )

    def test_explicit_opt_in_allows_it(self, tmp_path):
        GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
            errata={"event_markers": "drop"}, delete_source=True,
            allow_lossy_delete_source=True,
        )

    def test_lossless_defaults_never_refuse(self, tmp_path):
        GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
            errata=DEFAULT_ERRATA, delete_source=True,
        )

    def test_the_original_steps_keep_their_warning_only(self, tmp_path):
        # columns_to_check/output_columns/float32_columns predate the guard.
        GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=["A"],
            output_columns=["A"], float32_columns=["B"], delete_source=True,
        )


class TestDryRunReport:
    def test_reports_each_steps_cost_and_writes_nothing(self, tmp_path, caplog):
        _write_new_year_2020(tmp_path / "in")
        out_dir = tmp_path / "out"
        with caplog.at_level(logging.INFO):
            GDELTCleaner(
                str(tmp_path / "in"), str(out_dir), columns_to_check=[],
                errata={**DEFAULT_ERRATA, "event_markers": "drop"},
                dry_run=True, report=True,
            ).clean_all_files()
        messages = [r.message for r in caplog.records]
        assert "[dry run] errata.date_1920: 2" in messages
        assert "[dry run] errata.event_markers_drop: 1" in messages
        assert any(m.startswith("[dry run] 1 file(s) read: 3 rows in, 2 out") for m in messages)
        assert any("lossy steps: errata.event_markers: drop" in m for m in messages)
        assert list(out_dir.glob("*.parquet")) == []
        assert not (out_dir.parent / f"{out_dir.name}_runs").exists()

    def test_plain_dry_run_does_not_read_the_data(self, tmp_path, caplog):
        _write_new_year_2020(tmp_path / "in")
        with caplog.at_level(logging.INFO):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                errata=DEFAULT_ERRATA, dry_run=True,
            ).clean_all_files()
        assert not any("errata.date_1920" in r.message for r in caplog.records)

    def test_reads_with_the_same_worker_plan_as_a_real_run(self, tmp_path, monkeypatch):
        in_dir = tmp_path / "in"
        in_dir.mkdir()
        for name in ("a", "b", "c"):
            pl.DataFrame({"GlobalEventID": [1]}).write_parquet(in_dir / f"{name}.parquet")

        plans = []
        real_plan_workers = cleaner_module.plan_workers

        def recording_plan_workers(*args, **kwargs):
            plan = real_plan_workers(*args, **kwargs)
            plans.append(plan)
            return plan

        monkeypatch.setattr(cleaner_module, "plan_workers", recording_plan_workers)
        GDELTCleaner(
            str(in_dir), str(tmp_path / "out"), columns_to_check=[],
            max_workers=4, max_concurrent_reads=2, dry_run=True, report=True,
        ).clean_all_files()

        assert [plan.workers for plan in plans] == [2]


class TestRunCleanerErrataConfig:
    def test_errata_and_opt_in_are_read_from_the_config(self, tmp_path, monkeypatch):
        captured = {}
        real_init = GDELTCleaner.__init__

        def spy_init(self, *args, **kwargs):
            captured.update(kwargs)
            real_init(self, *args, **kwargs)

        monkeypatch.setattr(GDELTCleaner, "__init__", spy_init)
        (tmp_path / "in").mkdir()
        cfg = {
            "paths": {"parquet_data_directory": str(tmp_path / "in"),
                      "cleaned_data_directory": str(tmp_path / "out")},
            "clean": {"columns_to_check": {"gdelt_event": []},
                      "errata": {"gdelt_event": DEFAULT_ERRATA},
                      "allow_lossy_delete_source": True},
            "converter": {"partitioning": {"enabled": False}},
        }
        run_cleaner(cfg, dataset="gdelt_event", dry_run=True, report=True)
        assert captured["errata"] == DEFAULT_ERRATA
        assert captured["allow_lossy_delete_source"] is True
        assert captured["report"] is True


class TestFingerprintWithoutErrata:
    def test_no_errata_keeps_the_pre_012_fingerprint(self, tmp_path):
        # Datasets no errata rule applies to must not be cleaned again just
        # because the stage learned about errata.
        from gdeltforge.utils.io import config_fingerprint

        cleaner = GDELTCleaner(str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=["A"])
        assert cleaner._config_fingerprint == config_fingerprint(
            columns_to_check=["A"], output_columns=None, float32_columns=None,
            compression="zstd",
        )


class TestNormalizeSettings:
    def test_trims_and_nulls_through_the_stage(self, tmp_path):
        in_dir = tmp_path / "in"
        in_dir.mkdir()
        pl.DataFrame({"GlobalEventID": [1, 2], "Actor2Code": [" USA", " "]}).write_parquet(
            in_dir / "20130501.export.parquet"
        )
        out_dir = tmp_path / "out"
        GDELTCleaner(
            str(in_dir), str(out_dir), columns_to_check=[],
            normalize={"trim_strings": True, "blank_to_null": True},
        ).clean_all_files()
        out = pl.read_parquet(out_dir / "20130501.export_cleaned.parquet")
        assert out["Actor2Code"].to_list() == ["USA", None]
        audit = pl.read_parquet(next((out_dir.parent / f"{out_dir.name}_runs").glob("*.parquet")))
        assert audit["normalize.trimmed"].to_list() == [1]
        assert audit["normalize.blank_to_null"].to_list() == [1]

    def test_off_by_default_and_fingerprint_unchanged(self, tmp_path):
        from gdeltforge.utils.io import config_fingerprint

        cleaner = GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
            normalize={"trim_strings": False},
        )
        assert cleaner.steps == []
        assert cleaner._config_fingerprint == config_fingerprint(
            columns_to_check=[], output_columns=None, float32_columns=None, compression="zstd",
        )

    @pytest.mark.parametrize("normalize, message", [
        ({"trim": True}, "unknown setting"),
        ({"trim_strings": "yes"}, "must be true or false"),
    ])
    def test_invalid_settings_fail_up_front(self, tmp_path, normalize, message):
        with pytest.raises(ValueError, match=message):
            GDELTCleaner(str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                         normalize=normalize)

    def test_delete_source_refuses_it(self, tmp_path):
        with pytest.raises(ValueError, match="normalize.trim_strings"):
            GDELTCleaner(
                str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                normalize={"trim_strings": True}, delete_source=True,
            )


class TestDeriveSettings:
    def test_adds_the_columns_through_the_stage(self, tmp_path):
        from gdeltforge.utils.io import cleaned_marker

        _write_new_year_2020(tmp_path / "in")
        out_dir = tmp_path / "out"
        GDELTCleaner(
            str(tmp_path / "in"), str(out_dir), columns_to_check=[], errata=DEFAULT_ERRATA,
            derive={"event_date": True, "labels": ["EventRootCode"]},
        ).clean_all_files()
        path = out_dir / "20200102.export_cleaned.parquet"
        out = pl.read_parquet(path).sort("GlobalEventID")
        # After errata: the repaired 1920 dates become real 2020 dates.
        assert out["EventDate"].to_list()[0] == datetime.date(2020, 1, 1)
        assert out["EventRootCode_Label"].to_list()[0] == "MAKE PUBLIC STATEMENT"
        marker = cleaned_marker(path)
        assert marker is not None
        derive_step = next(s for s in marker["steps"] if s["step"] == "derive")
        assert derive_step == {"step": "derive", "lossy": False, "event_date": True,
                               "labels": ["EventRootCode"]}

    def test_projection_keeps_derived_columns_only_when_listed(self, tmp_path):
        _write_new_year_2020(tmp_path / "in")
        GDELTCleaner(
            str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
            derive={"event_date": True}, output_columns=["GlobalEventID", "EventDate"],
        ).clean_all_files()
        out = pl.read_parquet(tmp_path / "out" / "20200102.export_cleaned.parquet")
        assert out.columns == ["GlobalEventID", "EventDate"]

    @pytest.mark.parametrize("derive, message", [
        ({"event_dates": True}, "unknown setting"),
        ({"event_date": 1}, "must be true or false"),
        ({"labels": "EventCode"}, "must be a list"),
        ({"labels": ["GlobalEventID"]}, "aren't CAMEO-coded columns"),
    ])
    def test_invalid_settings_fail_up_front(self, tmp_path, derive, message):
        with pytest.raises(ValueError, match=message):
            GDELTCleaner(str(tmp_path / "in"), str(tmp_path / "out"), columns_to_check=[],
                         derive=derive)
