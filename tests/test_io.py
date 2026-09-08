import gc
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import polars as pl
import pytest

from gdeltforge.utils.io import (
    _pid_exists,
    _schema_from_json,
    _schema_to_json,
    clearer_dataset_errors,
    config_fingerprint,
    delete_done_marker,
    is_marked_done,
    mark_done,
    narrow_to_available_columns,
    read_csv_export,
    read_parquet_path,
    reconcile_parquet_schema,
    scan_dataset_reconciled,
    scan_file_against_schema,
    warn_if_delete_source_drops_recoverable_data,
    write_dataframe_atomic,
    write_parquet_atomic,
)


class TestWriteParquetAtomic:
    def test_writes_file_and_leaves_no_tmp_behind(self, tmp_path):
        out = tmp_path / "sample.parquet"
        df = pl.DataFrame({"GlobalEventID": [1, 2, 3]})

        write_parquet_atomic(df, out)

        assert out.exists()
        assert pl.read_parquet(out)["GlobalEventID"].to_list() == [1, 2, 3]
        assert not (tmp_path / "sample.parquet.tmp").exists()

    def test_warns_and_overwrites_leftover_tmp_from_interrupted_run(
        self, tmp_path, caplog, monkeypatch
    ):
        # The tmp name is PID-suffixed (see the concurrent-write fix this
        # guards against below), so a "leftover" this run can actually
        # recognize is scoped to its own PID: pinning os.getpid() is what
        # makes this deterministic to set up, the same class of leftover
        # a hard-killed process's own next run under OS PID reuse would
        # otherwise reproduce.
        monkeypatch.setattr(os, "getpid", lambda: 12345)
        out = tmp_path / "sample.parquet"
        tmp_path_leftover = tmp_path / "sample.parquet.12345.tmp"
        tmp_path_leftover.write_bytes(b"partial garbage from a killed run")

        df = pl.DataFrame({"GlobalEventID": [1, 2, 3]})
        with caplog.at_level(logging.WARNING):
            write_parquet_atomic(df, out)

        assert "leftover incomplete file" in caplog.text
        assert pl.read_parquet(out)["GlobalEventID"].to_list() == [1, 2, 3]
        assert not tmp_path_leftover.exists()

    def test_extra_kwargs_are_passed_through_to_write_parquet(self, tmp_path, monkeypatch):
        out = tmp_path / "sample.parquet"
        captured = {}

        real_write_parquet = pl.DataFrame.write_parquet

        def spy(self, path, **kwargs):
            captured.update(kwargs)
            return real_write_parquet(self, path, **kwargs)

        monkeypatch.setattr(pl.DataFrame, "write_parquet", spy)

        write_parquet_atomic(pl.DataFrame({"a": [1]}), out, compression="snappy")

        assert captured == {"compression": "snappy"}

    def test_cleans_up_tmp_and_reraises_on_write_failure(self, tmp_path, monkeypatch):
        out = tmp_path / "sample.parquet"

        def boom(self, path, *args, **kwargs):
            Path(path).write_bytes(b"partial write before failure")
            raise OSError("disk full")

        monkeypatch.setattr(pl.DataFrame, "write_parquet", boom)

        with pytest.raises(OSError):
            write_parquet_atomic(pl.DataFrame({"a": [1]}), out)

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
        df = pl.DataFrame({"GlobalEventID": [1, 2, 3]})

        write_dataframe_atomic(df, out, export_format="parquet")

        assert out.exists()
        assert pl.read_parquet(out)["GlobalEventID"].to_list() == [1, 2, 3]
        assert not (tmp_path / "sample.parquet.tmp").exists()

    def test_csv_writes_a_real_readable_file(self, tmp_path):
        out = tmp_path / "sample.csv"
        df = pl.DataFrame({"GlobalEventID": [1, 2, 3], "QuadClass": [1, 2, 3]})

        write_dataframe_atomic(df, out, export_format="csv")

        assert out.exists()
        result = pl.read_csv(out)
        assert result["GlobalEventID"].to_list() == [1, 2, 3]
        assert result["QuadClass"].to_list() == [1, 2, 3]
        assert not (tmp_path / "sample.csv.tmp").exists()

    def test_csv_writes_without_an_index_column(self, tmp_path):
        # Regression guard carried over from the pandas implementation,
        # where this required an explicit index=False: polars frames have
        # no index concept at all, so there's nothing to suppress here,
        # but the guarantee (no synthetic extra column in the output)
        # still deserves its own test rather than being assumed.
        out = tmp_path / "sample.csv"
        df = pl.DataFrame({"GlobalEventID": [1, 2, 3]})

        write_dataframe_atomic(df, out, export_format="csv")

        assert pl.read_csv(out).columns == ["GlobalEventID"]

    def test_csv_warns_about_zero_padded_string_codes_with_the_read_back_fix(
        self, tmp_path, caplog
    ):
        # Regression coverage for a real gap found via a live comprehensive
        # QA pass: EventCode/EventBaseCode/EventRootCode are zero-padded
        # strings ("020", "07") in the parquet source, correctly typed
        # String. A standard CSV read with default type inference,
        # confirmed directly for polars' own read_csv (the tool this
        # pipeline's own output is most likely to be re-read with), reads
        # an unquoted-looking numeric field back as an integer and drops
        # the leading zero, EVEN when the written field was quoted:
        # quoting does not change a reader's own default inference. There
        # is no write-side fix for that, so this checks the warning names
        # the real limitation and a working read-back fix instead.
        out = tmp_path / "sample.csv"
        df = pl.DataFrame({
            "GlobalEventID": [1, 2],
            "EventCode": ["020", "173"],
            "EventBaseCode": ["02", "17"],
        })

        with caplog.at_level(logging.WARNING):
            write_dataframe_atomic(df, out, export_format="csv")

        assert any(
            "EventCode" in r.message and "EventBaseCode" in r.message for r in caplog.records
        )
        assert any("schema_overrides" in r.message for r in caplog.records)
        assert any("read_csv_export" in r.message for r in caplog.records)

        # The suggested manual fix from the warning must actually work,
        # for a caller who reads the file back some other way than
        # read_csv_export (its own dedicated tests below cover that path).
        result = pl.read_csv(out, schema_overrides={"EventCode": pl.Utf8, "EventBaseCode": pl.Utf8})
        assert result["EventCode"].to_list() == ["020", "173"]
        assert result["EventBaseCode"].to_list() == ["02", "17"]

    def test_csv_no_warning_when_no_zero_padded_columns_are_present(self, tmp_path, caplog):
        out = tmp_path / "sample.csv"
        df = pl.DataFrame({"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        with caplog.at_level(logging.WARNING):
            write_dataframe_atomic(df, out, export_format="csv")

        assert not any("schema_overrides" in r.message for r in caplog.records)

    def test_csv_still_quotes_the_zero_padded_field_in_the_written_file(self, tmp_path):
        # quote_style="non_numeric" is still applied: it protects a value
        # containing a comma/newline/quote regardless, and costs nothing,
        # even though it does not by itself fix the read-back inference
        # issue the warning above describes.
        out = tmp_path / "sample.csv"
        df = pl.DataFrame({"EventCode": ["020"]})

        write_dataframe_atomic(df, out, export_format="csv")

        assert '"020"' in out.read_text()

    def test_csv_genuine_numeric_columns_stay_unquoted(self, tmp_path):
        # quote_style="non_numeric" must not force-quote a real numeric
        # column just for being adjacent to string ones in the same file.
        out = tmp_path / "sample.csv"
        df = pl.DataFrame({"GlobalEventID": [1, 2], "GoldsteinScale": [-5.0, 3.0]})

        write_dataframe_atomic(df, out, export_format="csv")

        raw = out.read_text()
        assert '"1"' not in raw
        assert '"-5.0"' not in raw

    def test_csv_caller_can_still_override_quote_style(self, tmp_path):
        # kwargs.setdefault, not an unconditional override: an explicit
        # caller preference still wins.
        out = tmp_path / "sample.csv"
        df = pl.DataFrame({"EventCode": ["020"]})

        write_dataframe_atomic(df, out, export_format="csv", quote_style="never")

        assert out.read_text().strip() == "EventCode\n020"

    def test_csv_warns_and_overwrites_leftover_tmp_from_interrupted_run(
        self, tmp_path, caplog, monkeypatch
    ):
        # Same PID-pinning reasoning as write_parquet_atomic's identical
        # test above.
        monkeypatch.setattr(os, "getpid", lambda: 12345)
        out = tmp_path / "sample.csv"
        tmp_path_leftover = tmp_path / "sample.csv.12345.tmp"
        tmp_path_leftover.write_bytes(b"partial garbage from a killed run")

        df = pl.DataFrame({"GlobalEventID": [1, 2, 3]})
        with caplog.at_level(logging.WARNING):
            write_dataframe_atomic(df, out, export_format="csv")

        assert "leftover incomplete file" in caplog.text
        assert pl.read_csv(out)["GlobalEventID"].to_list() == [1, 2, 3]
        assert not tmp_path_leftover.exists()

    def test_csv_cleans_up_tmp_and_reraises_on_write_failure(self, tmp_path, monkeypatch):
        out = tmp_path / "sample.csv"

        def boom(self, path, *args, **kwargs):
            Path(path).write_bytes(b"partial write before failure")
            raise OSError("disk full")

        monkeypatch.setattr(pl.DataFrame, "write_csv", boom)

        with pytest.raises(OSError):
            write_dataframe_atomic(pl.DataFrame({"a": [1]}), out, export_format="csv")

        assert not out.exists()
        assert not (tmp_path / "sample.csv.tmp").exists()

    def test_unsupported_format_raises_clearly(self, tmp_path):
        out = tmp_path / "sample.json"
        with pytest.raises(ValueError, match="Unsupported export format: 'json'"):
            write_dataframe_atomic(pl.DataFrame({"a": [1]}), out, export_format="json")

        assert not out.exists()

    def test_csv_export_writes_a_schema_sidecar(self, tmp_path):
        out = tmp_path / "sample.csv"
        df = pl.DataFrame({
            "GlobalEventID": [1, 2], "GoldsteinScale": [-5.0, 3.0], "EventCode": ["020", "173"],
        })

        write_dataframe_atomic(df, out, export_format="csv")

        sidecar = tmp_path / "sample.csv.schema.json"
        assert sidecar.exists()
        assert not sidecar.with_name(sidecar.name + ".tmp").exists()
        assert json.loads(sidecar.read_text()) == {
            "GlobalEventID": "Int64", "GoldsteinScale": "Float64", "EventCode": "String",
        }

    def test_parquet_export_writes_no_sidecar(self, tmp_path):
        # The sidecar exists to work around CSV's own lack of a type
        # system; Parquet already carries its schema natively, so there's
        # nothing for a sidecar to add here.
        out = tmp_path / "sample.parquet"
        write_dataframe_atomic(pl.DataFrame({"EventCode": ["020"]}), out, export_format="parquet")

        assert not (tmp_path / "sample.parquet.schema.json").exists()

    def test_a_sidecar_write_failure_degrades_without_losing_the_csv(
        self, tmp_path, monkeypatch, caplog
    ):
        # Best-effort: the CSV export the caller actually asked for must
        # survive even if the schema sidecar can't be written (a
        # read-only destination, a full disk), degrading to
        # read_csv_export's own no-sidecar fallback rather than losing
        # output that already succeeded.
        out = tmp_path / "sample.csv"

        real_write_text = Path.write_text

        def boom(self, *args, **kwargs):
            if self.name.endswith(".schema.json.tmp"):
                raise OSError("disk full")
            return real_write_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", boom)

        with caplog.at_level(logging.WARNING):
            write_dataframe_atomic(
                pl.DataFrame({"GlobalEventID": [1, 2]}), out, export_format="csv"
            )

        assert out.exists()
        assert not (tmp_path / "sample.csv.schema.json").exists()
        assert not (tmp_path / "sample.csv.schema.json.tmp").exists()
        assert any("schema sidecar" in r.message for r in caplog.records)


class TestReadCsvExport:
    """read_csv_export is the actual fix for the CSV round-trip gap named
    in write_dataframe_atomic's own warning (EventCode/EventBaseCode/
    EventRootCode losing their leading zero on a standard re-read, and
    any other column silently losing its real dtype the same way): it
    restores every column's real dtype from the schema sidecar written
    alongside a gdeltforge CSV export, rather than leaving pl.read_csv to
    infer types from content the way a bare pl.read_csv/pd.read_csv call
    would. No write-side CSV setting can close this gap on its own (CSV
    itself carries no type information at all), so this is a read-side
    fix: a caller who reads back through this function instead of a bare
    pl.read_csv gets a genuinely lossless round trip; one who doesn't
    still gets the documented warning and manual workaround."""

    def test_full_round_trip_is_byte_for_byte_lossless(self, tmp_path):
        out = tmp_path / "sample.csv"
        df = pl.DataFrame({
            "GlobalEventID": [1, 2, None],
            "GoldsteinScale": [-5.0, None, 3.0],
            "EventCode": ["020", None, "057"],
            "EventBaseCode": ["02", "01", ""],
            "Actor1Name": ["A", None, "C"],
        })

        write_dataframe_atomic(df, out, export_format="csv")
        result = read_csv_export(out)

        assert result.schema == df.schema
        assert result.equals(df)

    def test_zero_padded_codes_keep_their_leading_zero(self, tmp_path):
        out = tmp_path / "sample.csv"
        write_dataframe_atomic(
            pl.DataFrame({"EventCode": ["020", "173"]}), out, export_format="csv"
        )

        result = read_csv_export(out)

        assert result["EventCode"].to_list() == ["020", "173"]

    def test_no_sidecar_falls_back_to_default_inference_with_a_warning(
        self, tmp_path, caplog
    ):
        # A CSV that never came from gdeltforge (or a pre-existing export
        # from before this existed): no sidecar to consult, so this can
        # only degrade to the same documented limitation write time
        # already warns about, not silently claim a fix that isn't there.
        out = tmp_path / "foreign.csv"
        pl.DataFrame({"EventCode": ["020", "173"]}).write_csv(out, quote_style="non_numeric")

        with caplog.at_level(logging.WARNING):
            result = read_csv_export(out)

        assert result["EventCode"].to_list() == [20, 173]
        assert any("schema sidecar" in r.message for r in caplog.records)
        assert any("schema_overrides" in r.message for r in caplog.records)

    def test_no_sidecar_no_warning_when_no_zero_padded_columns_present(
        self, tmp_path, caplog
    ):
        out = tmp_path / "foreign.csv"
        pl.DataFrame({"GlobalEventID": [1, 2]}).write_csv(out)

        with caplog.at_level(logging.WARNING):
            read_csv_export(out)

        assert not caplog.records

    def test_explicit_schema_overrides_win_over_the_sidecar(self, tmp_path):
        out = tmp_path / "sample.csv"
        write_dataframe_atomic(
            pl.DataFrame({"GlobalEventID": [1, 2], "EventCode": ["020", "173"]}),
            out, export_format="csv",
        )

        result = read_csv_export(out, schema_overrides={"GlobalEventID": pl.Float64})

        assert result.schema["GlobalEventID"] == pl.Float64
        assert result["GlobalEventID"].to_list() == [1.0, 2.0]
        # The column the caller didn't override still comes from the
        # sidecar, not default inference.
        assert result["EventCode"].to_list() == ["020", "173"]

    def test_other_read_csv_kwargs_still_pass_through(self, tmp_path):
        out = tmp_path / "sample.csv"
        write_dataframe_atomic(
            pl.DataFrame({"GlobalEventID": [1, 2, 3]}), out, export_format="csv"
        )

        result = read_csv_export(out, n_rows=2)

        assert len(result) == 2


class TestSchemaJson:
    """_schema_to_json/_schema_from_json: the plain-JSON representation
    the schema sidecar is written as, kept polars-independent on purpose
    (a human, or a caller who never imports polars, can still read it)."""

    def test_round_trips_every_dtype_this_project_s_data_actually_uses(self):
        schema = {"GlobalEventID": pl.Int64, "GoldsteinScale": pl.Float64, "EventCode": pl.String}

        assert _schema_from_json(_schema_to_json(schema)) == schema

    def test_an_unrecognized_dtype_name_is_skipped_not_raised(self):
        # A sidecar from a newer/older gdeltforge naming a dtype this
        # polars version doesn't have, or a hand-edited one with a typo,
        # degrades to default inference for just that column rather than
        # failing the whole read.
        result = _schema_from_json({"A": "Int64", "B": "NotARealDtype"})

        assert result == {"A": pl.Int64}

    def test_a_non_dtype_polars_attribute_name_is_also_skipped(self):
        # "concat" is a real name on the polars module, just not a dtype;
        # getattr(pl, "concat") must not be mistaken for one.
        result = _schema_from_json({"A": "concat"})

        assert result == {}


class TestPidExists:
    """
    Used only to decide whether a leftover PID-suffixed temp file (see
    TestOrphanedTempFileCleanup below) is safe to remove. Checked
    directly here since getting this wrong in either direction is real:
    a false "alive" leaves a genuine orphan on disk forever; a false
    "dead" could delete a live, concurrent process's own in-progress
    write.
    """

    def test_own_pid_is_alive(self):
        assert _pid_exists(os.getpid())

    def test_an_almost_certainly_nonexistent_pid_is_not_alive(self):
        # PIDs are a bounded, kernel-assigned namespace on every real
        # platform (Windows: typically < ~2^32 but practically always
        # small; POSIX: PID_MAX_LIMIT is 2^22); this value is chosen far
        # outside any range a real running process would ever hold.
        assert not _pid_exists(2**31 - 1)

    def test_a_real_child_process_is_alive_until_it_exits(self):
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(5)"])
        pid = proc.pid
        try:
            assert _pid_exists(pid)
        finally:
            proc.kill()
            proc.wait(timeout=10)
        # On Windows, a process stays queryable as long as any handle to
        # it is still open, including the parent's own Popen object, a
        # real OS semantic, not a bug in _pid_exists: the actual
        # production caller (an unrelated, later gdeltforge invocation)
        # never holds such a handle to a dead writer it didn't spawn.
        # Dropping this process's own handle reproduces that condition.
        del proc
        gc.collect()
        assert not _pid_exists(pid)


class TestOrphanedTempFileCleanup:
    """
    write_parquet_atomic/write_dataframe_atomic's temp path is PID-
    suffixed so two genuinely concurrent processes never collide on one
    shared name (TestWriteParquetAtomic/TestWriteDataframeAtomic's own
    leftover tests above cover that). That fix has a side effect: the
    leftover-detection warning used to check only the exact current-PID
    path, which a genuinely different, now-dead process's own leftover
    essentially never is, so it accumulated on disk indefinitely with no
    warning at any level. Found via a live comprehensive QA pass,
    reproduced with a real SIGKILL mid-write.

    The naive fix (glob any PID and delete unconditionally) would
    introduce a worse bug than the one it closes: two processes writing
    the same destination concurrently is exactly the scenario the PID
    suffix exists to allow safely, and deleting a live sibling's own
    in-progress temp file out from under it would silently corrupt that
    write. _clean_orphaned_tmp_files only removes a match whose owning
    PID is confirmed dead; a live PID (this process's own, or another
    genuinely running one) is left untouched.
    """

    def test_a_different_dead_pids_leftover_is_detected_and_removed(
        self, tmp_path, caplog, monkeypatch
    ):
        monkeypatch.setattr("gdeltforge.utils.io._pid_exists", lambda pid: False)
        out = tmp_path / "sample.parquet"
        orphan = out.with_name(f"{out.name}.999999.tmp")
        orphan.write_bytes(b"partial parquet bytes from a killed run, different pid")

        with caplog.at_level(logging.WARNING):
            write_parquet_atomic(pl.DataFrame({"GlobalEventID": [1, 2, 3]}), out)

        assert "leftover incomplete file" in caplog.text
        assert "999999" in caplog.text
        assert not orphan.exists()
        assert pl.read_parquet(out)["GlobalEventID"].to_list() == [1, 2, 3]

    def test_a_different_but_still_alive_pids_leftover_is_left_alone(
        self, tmp_path, caplog, monkeypatch
    ):
        # The critical safety property: a live PID must never be treated
        # as an orphan, since it could be a genuinely concurrent, healthy
        # writer mid-write to this exact destination right now.
        monkeypatch.setattr("gdeltforge.utils.io._pid_exists", lambda pid: True)
        out = tmp_path / "sample.parquet"
        active = out.with_name(f"{out.name}.999999.tmp")
        active.write_bytes(b"a live sibling process's own in-progress write")

        with caplog.at_level(logging.WARNING):
            write_parquet_atomic(pl.DataFrame({"GlobalEventID": [1, 2, 3]}), out)

        assert "999999" not in caplog.text
        assert active.exists()
        assert active.read_bytes() == b"a live sibling process's own in-progress write"
        assert pl.read_parquet(out)["GlobalEventID"].to_list() == [1, 2, 3]

    def test_csv_export_gets_the_same_cleanup(self, tmp_path, monkeypatch):
        monkeypatch.setattr("gdeltforge.utils.io._pid_exists", lambda pid: False)
        out = tmp_path / "sample.csv"
        orphan = out.with_name(f"{out.name}.999999.tmp")
        orphan.write_bytes(b"partial csv bytes from a killed run")

        write_dataframe_atomic(pl.DataFrame({"GlobalEventID": [1, 2]}), out, export_format="csv")

        assert not orphan.exists()
        assert pl.read_csv(out)["GlobalEventID"].to_list() == [1, 2]

    def test_a_non_matching_file_is_left_alone(self, tmp_path):
        # Anything at the destination that doesn't match the <name>.<pid>.tmp
        # shape at all (a stray unrelated file, or a name a caller happens
        # to control) is never touched by this cleanup.
        out = tmp_path / "sample.parquet"
        unrelated = out.with_name(f"{out.name}.backup.tmp")
        unrelated.write_bytes(b"not an orphaned write, just a similarly-named file")

        write_parquet_atomic(pl.DataFrame({"GlobalEventID": [1]}), out)

        assert unrelated.exists()

    def test_real_sigkill_mid_write_leaves_an_orphan_that_a_later_run_cleans_up(
        self, tmp_path
    ):
        # The un-mocked, real-process version of the two tests above:
        # a genuine SIGKILL mid-write leaves a real orphaned temp file,
        # and a later, unrelated, fully successful run at the same
        # destination detects and removes it.
        script = tmp_path / "slow_writer.py"
        script.write_text(
            "import time\n"
            "import polars as pl\n"
            "from pathlib import Path\n"
            "from gdeltforge.utils.io import write_parquet_atomic\n"
            "_orig = pl.DataFrame.write_parquet\n"
            "def slow(self, path, *a, **kw):\n"
            "    Path(path).write_bytes(b'partial')\n"
            "    time.sleep(10)\n"
            "    return _orig(self, path, *a, **kw)\n"
            "pl.DataFrame.write_parquet = slow\n"
            "write_parquet_atomic(pl.DataFrame({'GlobalEventID': [1]}), 'out.parquet')\n"
        )
        proc = subprocess.Popen([sys.executable, str(script)], cwd=tmp_path)
        try:
            time.sleep(1.5)
            proc.kill()  # SIGKILL on POSIX, TerminateProcess on Windows
            proc.wait(timeout=10)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)
        # See TestPidExists' identical note: a process stays queryable on
        # Windows as long as any handle to it (including this test's own
        # Popen object) is still open. A real, later, unrelated
        # gdeltforge invocation never holds such a handle to begin with.
        del proc
        gc.collect()

        orphans_after_kill = list(tmp_path.glob("out.parquet.*.tmp"))
        assert len(orphans_after_kill) == 1, (
            "expected one orphaned tmp file from the killed process"
        )

        write_parquet_atomic(pl.DataFrame({"GlobalEventID": [1, 2, 3]}), tmp_path / "out.parquet")

        assert not orphans_after_kill[0].exists(), (
            "the orphan should be cleaned up by the later run"
        )
        assert pl.read_parquet(tmp_path / "out.parquet")["GlobalEventID"].to_list() == [1, 2, 3]


class TestReadParquetPath:
    def test_reads_a_single_file_directly(self, tmp_path):
        f = tmp_path / "sample.parquet"
        pl.DataFrame({"GlobalEventID": [1, 2, 3]}).write_parquet(f)

        result = read_parquet_path(f)

        assert result["GlobalEventID"].to_list() == [1, 2, 3]

    def test_reads_every_parquet_file_in_a_directory(self, tmp_path):
        pl.DataFrame({"GlobalEventID": [1, 2]}).write_parquet(tmp_path / "a.parquet")
        pl.DataFrame({"GlobalEventID": [3, 4, 5]}).write_parquet(tmp_path / "b.parquet")

        result = read_parquet_path(tmp_path)

        assert sorted(result["GlobalEventID"].to_list()) == [1, 2, 3, 4, 5]

    def test_ignores_done_resumability_markers_in_a_directory(self, tmp_path):
        # The real bug: convert/filter's own .done markers (mark_done above
        # writes them as a dot-prefixed sibling of the data) sit in exactly
        # these directories by design. This explicit *.parquet glob is what
        # keeps them out, not an assumption that the underlying engine
        # skips dot-prefixed files on its own (polars' own bare-directory
        # read does not, confirmed directly; see read_parquet_path's own
        # docstring).
        f = tmp_path / "20260811.export.parquet"
        pl.DataFrame({"GlobalEventID": [1, 2]}).write_parquet(f)
        mark_done(f, "some-fingerprint")
        assert (tmp_path / ".20260811.export.parquet.done").exists()

        result = read_parquet_path(tmp_path)

        assert result["GlobalEventID"].to_list() == [1, 2]

    def test_nonexistent_path_raises_a_crafted_error_not_a_raw_os_one(self, tmp_path):
        # A path naming neither a file nor a directory used to reach
        # pl.read_parquet unchecked, surfacing its own raw "No such file
        # or directory (os error 2): ..." straight from polars' Rust
        # reader, unlike every other missing-path case in this project.
        missing = tmp_path / "nonexistent.parquet"

        with pytest.raises(FileNotFoundError, match="does not exist"):
            read_parquet_path(missing)

    def test_reads_files_in_a_hive_partitioned_subdirectory_too(self, tmp_path):
        # Regression coverage for a real gap found via a live comprehensive
        # QA pass: crossref --events <a real, valid, non-empty Hive-
        # partitioned historical directory> reported "No parquet files
        # found", since the directory branch only ever globbed its own
        # top level, never Year=YYYY/MonthYear=YYYYMM/*.parquet -- the
        # exact shape converter.partitioning writes for events/events-
        # reduced, and IndexedSampler's own FileIndex already walks.
        hist_dir = tmp_path / "Year=2008" / "MonthYear=200801"
        hist_dir.mkdir(parents=True)
        pl.DataFrame({"GlobalEventID": [1, 2, 3]}).write_parquet(
            hist_dir / "200801_filtered.parquet"
        )

        result = read_parquet_path(tmp_path)

        assert sorted(result["GlobalEventID"].to_list()) == [1, 2, 3]

    def test_reads_a_mix_of_flat_and_hive_partitioned_files_together(self, tmp_path):
        pl.DataFrame({"GlobalEventID": [1, 2]}).write_parquet(tmp_path / "a.parquet")
        hist_dir = tmp_path / "Year=2008" / "MonthYear=200801"
        hist_dir.mkdir(parents=True)
        pl.DataFrame({"GlobalEventID": [3, 4, 5]}).write_parquet(hist_dir / "hist.parquet")

        result = read_parquet_path(tmp_path)

        assert sorted(result["GlobalEventID"].to_list()) == [1, 2, 3, 4, 5]

    def test_reconciles_a_narrower_historical_file_with_wider_flat_files(self, tmp_path):
        # Same root cause as IndexedSampler's own identical fix: a real
        # accumulated directory can genuinely mix files whose own
        # physical schema differs (a Hive-partitioned file converted
        # before a schema fix landed, an output_columns setting narrowed
        # at one point and widened again later). A plain per-file
        # pl.read_parquet + pl.concat has no way to tolerate that; this
        # is the same class of "unable to append to a DataFrame of width
        # X with a DataFrame of width Y" crash #13 found for --mode
        # indexed, reachable here too the moment --events points crossref
        # at a directory mixing both shapes.
        pl.DataFrame({"GlobalEventID": [1, 2], "QuadClass": [1, 2]}).write_parquet(
            tmp_path / "a.parquet"
        )
        hist_dir = tmp_path / "Year=2008"
        hist_dir.mkdir()
        pl.DataFrame({"GlobalEventID": [3, 4]}).write_parquet(hist_dir / "hist.parquet")

        result = read_parquet_path(tmp_path)

        by_id = {row["GlobalEventID"]: row["QuadClass"] for row in result.to_dicts()}
        assert by_id[1] == 1 and by_id[2] == 2
        assert by_id[3] is None and by_id[4] is None

    def test_ignores_done_resumability_markers_in_a_nested_historical_directory(
        self, tmp_path
    ):
        hist_dir = tmp_path / "Year=2008"
        hist_dir.mkdir()
        f = hist_dir / "hist.parquet"
        pl.DataFrame({"GlobalEventID": [1, 2]}).write_parquet(f)
        mark_done(f, "some-fingerprint")
        assert (hist_dir / ".hist.parquet.done").exists()

        result = read_parquet_path(tmp_path)

        assert result["GlobalEventID"].to_list() == [1, 2]

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
    """clearer_dataset_errors wraps a dataset read so a bare, low-level
    ArrowInvalid/ComputeError/OSError, e.g. "Could not open Parquet input
    source '<path>': ..." or "File out of specification: ...", gets an
    actionable message on top, naming what was being read and the likely
    causes, instead of surfacing as a mystery low-level engine error.
    Two engines' own exception types are both live call sites today:
    indexer.py still reads via pyarrow.dataset directly (ArrowException),
    while read_parquet_path and everything ported to polars raises
    ComputeError instead. Confirmed the real shape of both against a
    genuinely corrupt file, not assumed."""

    def test_an_arrow_error_is_wrapped_with_context(self):
        import pyarrow as pa

        with pytest.raises(RuntimeError, match=r"reading 3 parquet file\(s\)") as exc_info:
            with clearer_dataset_errors("3 parquet file(s)"):
                raise pa.ArrowInvalid("Could not open Parquet input source 'x': bad magic bytes")
        assert "Common causes" in str(exc_info.value)

    def test_a_polars_compute_error_is_wrapped_with_context(self):
        with pytest.raises(RuntimeError, match=r"reading 3 parquet file\(s\)") as exc_info:
            with clearer_dataset_errors("3 parquet file(s)"):
                raise pl.exceptions.ComputeError("File out of specification: bad magic bytes")
        assert "Common causes" in str(exc_info.value)

    def test_a_real_corrupt_file_raises_a_wrapped_error_through_read_parquet_path(
        self, tmp_path
    ):
        # Not a synthetic raise: a real, genuinely non-parquet file run
        # through the actual read_parquet_path/polars call chain this
        # wrapper protects, confirming the exception type polars really
        # raises for this case is one the except clause actually catches.
        # A single-file path isn't itself wrapped (read_parquet_path only
        # wraps its multi-file directory branch), so the corrupt file is
        # placed inside a directory to exercise that branch for real.
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()
        (parquet_dir / "corrupt.parquet").write_bytes(
            b"not a real parquet file, just garbage bytes"
        )

        with pytest.raises(RuntimeError, match="Common causes"):
            read_parquet_path(parquet_dir)

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
        # Only the exception types pyarrow/polars/OS-level read failures
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


class TestNarrowToAvailableColumns:
    """
    Shared by samplers.py's FilteredSampler and crossref.py's v1/v2 join
    paths: both build a column projection that defaults to a dataset's
    full declared schema when the caller doesn't pass --columns, which
    isn't the same thing as what a real, possibly output_columns-pruned
    file on disk actually has. required distinguishes a column a caller
    has no usable path forward without (raise clearly) from everything
    else, which is just an output-only request (drop with a warning)."""

    def test_missing_required_column_raises_a_clear_error(self):
        with pytest.raises(ValueError, match="required column.*EventIds"):
            narrow_to_available_columns(
                logging.getLogger("test"), "GKG 1.0 dataset in /data",
                requested={"EventIds", "Date"}, required={"EventIds"},
                available={"Date"},
            )

    def test_missing_optional_columns_warn_and_are_dropped(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = narrow_to_available_columns(
                logging.getLogger("test"), "GKG 1.0 dataset in /data",
                requested={"EventIds", "Tone", "Themes"}, required={"EventIds"},
                available={"EventIds", "Date"},
            )
        assert result == ["EventIds"]
        message = caplog.records[0].message
        assert "Tone" in message and "Themes" in message
        assert "EventIds" not in message.split(":")[1]  # not reported as dropped

    def test_nothing_missing_warns_nothing_and_keeps_everything_requested(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = narrow_to_available_columns(
                logging.getLogger("test"), "GKG 1.0 dataset in /data",
                requested={"EventIds", "Date"}, required={"EventIds"},
                available={"EventIds", "Date", "Tone"},
            )
        assert result == ["Date", "EventIds"]
        assert caplog.records == []

    def test_a_required_column_absent_from_requested_is_still_returned(self):
        # A join key is always included in the read regardless of
        # whether the caller's own --columns happened to name it.
        result = narrow_to_available_columns(
            logging.getLogger("test"), "GKG 1.0 dataset in /data",
            requested={"Date"}, required={"EventIds"}, available={"EventIds", "Date"},
        )
        assert result == ["Date", "EventIds"]


class TestReconcileParquetSchema:
    """
    Shared by samplers.py's _scan_dataset/IndexedSampler.get_random_sample
    and crossref.py's _dataset: a real accumulated GDELT archive can
    declare the same column under genuinely different dtypes in different
    files (events' own Actor2Geo_Type is Float64 in every archive through
    2007-10, Int64 from 2007-11 onward; GKG 2.1's V2.1DATE is Float64 in
    441 files scattered across six years, Int64 everywhere else), and a
    plain schema.setdefault(name, dtype) union silently keeps whichever
    file was read first rather than detecting the conflict at all.
    """

    def test_a_single_shared_dtype_across_files_is_kept_as_is(self, tmp_path):
        pl.DataFrame({"GlobalEventID": [1], "QuadClass": [1]}).write_parquet(
            tmp_path / "a.parquet"
        )
        pl.DataFrame({"GlobalEventID": [2], "QuadClass": [2]}).write_parquet(
            tmp_path / "b.parquet"
        )

        schema = reconcile_parquet_schema([tmp_path / "a.parquet", tmp_path / "b.parquet"])

        assert schema == {"GlobalEventID": pl.Int64(), "QuadClass": pl.Int64()}

    def test_int_and_float_across_files_widens_to_float(self, tmp_path):
        pl.DataFrame({"Actor2Geo_Type": [1.0, 2.0]}).write_parquet(tmp_path / "old.parquet")
        pl.DataFrame({"Actor2Geo_Type": [1, 2]}).write_parquet(tmp_path / "new.parquet")

        schema = reconcile_parquet_schema([tmp_path / "old.parquet", tmp_path / "new.parquet"])

        assert schema["Actor2Geo_Type"] == pl.Float64()

    def test_mixed_integer_widths_across_files_widen_to_int64(self, tmp_path):
        pl.DataFrame({"n": pl.Series([1, 2], dtype=pl.Int32)}).write_parquet(
            tmp_path / "a.parquet"
        )
        pl.DataFrame({"n": pl.Series([3, 4], dtype=pl.Int64)}).write_parquet(
            tmp_path / "b.parquet"
        )

        schema = reconcile_parquet_schema([tmp_path / "a.parquet", tmp_path / "b.parquet"])

        assert schema["n"] == pl.Int64()

    def test_a_non_numeric_conflict_raises_a_clear_error_naming_both_dtypes(self, tmp_path):
        # A string column in one file and a numeric column of the same
        # name in another is a real data problem, not a width difference
        # pl.concat(..., how="vertical_relaxed") could paper over safely;
        # this must fail loudly rather than silently coercing one side.
        pl.DataFrame({"code": ["US", "BR"]}).write_parquet(tmp_path / "a.parquet")
        pl.DataFrame({"code": [1, 2]}).write_parquet(tmp_path / "b.parquet")

        with pytest.raises(
            pl.exceptions.SchemaError, match="code.*Int64.*String|code.*String.*Int64"
        ):
            reconcile_parquet_schema([tmp_path / "a.parquet", tmp_path / "b.parquet"])


class TestScanDatasetReconciled:
    def test_no_conflict_reads_correctly_across_files(self, tmp_path):
        pl.DataFrame({"GlobalEventID": [1, 2]}).write_parquet(tmp_path / "a.parquet")
        pl.DataFrame({"GlobalEventID": [3, 4]}).write_parquet(tmp_path / "b.parquet")

        df = scan_dataset_reconciled([tmp_path / "a.parquet", tmp_path / "b.parquet"]).collect()

        assert sorted(df["GlobalEventID"].to_list()) == [1, 2, 3, 4]

    def test_a_column_missing_from_one_file_comes_back_null_for_its_rows(self, tmp_path):
        pl.DataFrame({"GlobalEventID": [1, 2], "QuadClass": [1, 2]}).write_parquet(
            tmp_path / "a.parquet"
        )
        pl.DataFrame({"GlobalEventID": [3, 4]}).write_parquet(tmp_path / "b.parquet")

        df = scan_dataset_reconciled([tmp_path / "a.parquet", tmp_path / "b.parquet"]).collect()

        by_id = {row["GlobalEventID"]: row["QuadClass"] for row in df.to_dicts()}
        assert by_id[1] == 1 and by_id[2] == 2
        assert by_id[3] is None and by_id[4] is None

    def test_a_dtype_conflict_across_files_reconciles_instead_of_crashing(self, tmp_path):
        # Mirrors the real Actor2Geo_Type split directly: scan_parquet's
        # own schema= parameter is an assertion, so a plain union scan
        # crashes ("data type mismatch ... incoming: Int64 != target:
        # Float64") the moment it reaches the file whose real dtype
        # disagrees with whichever file the schema was inferred from.
        pl.DataFrame({
            "GlobalEventID": [1, 2], "Actor2Geo_Type": [1.0, 2.0],
        }).write_parquet(tmp_path / "before_2007_11.parquet")
        pl.DataFrame({
            "GlobalEventID": [3, 4], "Actor2Geo_Type": [3, 4],
        }).write_parquet(tmp_path / "after_2007_11.parquet")

        df = scan_dataset_reconciled(
            [tmp_path / "before_2007_11.parquet", tmp_path / "after_2007_11.parquet"]
        ).collect()

        assert df["Actor2Geo_Type"].dtype == pl.Float64
        by_id = {row["GlobalEventID"]: row["Actor2Geo_Type"] for row in df.to_dicts()}
        assert by_id == {1: 1.0, 2: 2.0, 3: 3.0, 4: 4.0}

    def test_a_dtype_conflict_alongside_a_missing_column_reconciles_both_at_once(
        self, tmp_path
    ):
        # A real archive can hit both reconciliation shapes in the same
        # multi-file read: one file both disagrees on a shared column's
        # dtype AND lacks a column entirely (an output_columns change and
        # a later schema fix landing at different times).
        pl.DataFrame({
            "GlobalEventID": [1, 2], "Actor2Geo_Type": [1.0, 2.0], "SOURCEURL": ["a", "b"],
        }).write_parquet(tmp_path / "a.parquet")
        pl.DataFrame({
            "GlobalEventID": [3], "Actor2Geo_Type": [3],
        }).write_parquet(tmp_path / "b.parquet")

        df = scan_dataset_reconciled([tmp_path / "a.parquet", tmp_path / "b.parquet"]).collect()

        assert df["Actor2Geo_Type"].dtype == pl.Float64
        by_id = {row["GlobalEventID"]: row["SOURCEURL"] for row in df.to_dicts()}
        assert by_id[1] == "a" and by_id[2] == "b" and by_id[3] is None

    def test_three_way_dtype_conflict_reconciles_across_every_file(self, tmp_path):
        pl.DataFrame({"n": pl.Series([1.0], dtype=pl.Float64)}).write_parquet(
            tmp_path / "a.parquet"
        )
        pl.DataFrame({"n": pl.Series([2], dtype=pl.Int32)}).write_parquet(
            tmp_path / "b.parquet"
        )
        pl.DataFrame({"n": pl.Series([3], dtype=pl.Int64)}).write_parquet(
            tmp_path / "c.parquet"
        )

        df = scan_dataset_reconciled(
            [tmp_path / "a.parquet", tmp_path / "b.parquet", tmp_path / "c.parquet"]
        ).collect()

        assert df["n"].dtype == pl.Float64
        assert sorted(df["n"].to_list()) == [1.0, 2.0, 3.0]


class TestScanFileAgainstSchema:
    def test_a_file_matching_the_target_schema_reads_unmodified(self, tmp_path):
        pl.DataFrame({"GlobalEventID": [1], "n": [1]}).write_parquet(tmp_path / "a.parquet")

        df = scan_file_against_schema(
            tmp_path / "a.parquet", {"GlobalEventID": pl.Int64(), "n": pl.Int64()}
        ).collect()

        assert df["n"].dtype == pl.Int64

    def test_a_file_whose_own_dtype_disagrees_is_cast_to_the_target(self, tmp_path):
        pl.DataFrame({"GlobalEventID": [1], "n": [1]}).write_parquet(tmp_path / "a.parquet")

        df = scan_file_against_schema(
            tmp_path / "a.parquet", {"GlobalEventID": pl.Int64(), "n": pl.Float64()}
        ).collect()

        assert df["n"].dtype == pl.Float64
        assert df["n"].to_list() == [1.0]

    def test_a_column_the_file_lacks_comes_back_null_at_the_target_dtype(self, tmp_path):
        pl.DataFrame({"GlobalEventID": [1]}).write_parquet(tmp_path / "a.parquet")

        df = scan_file_against_schema(
            tmp_path / "a.parquet", {"GlobalEventID": pl.Int64(), "n": pl.Float64()}
        ).collect()

        assert df["n"].dtype == pl.Float64
        assert df["n"].to_list() == [None]
