import json
import logging
import os
from pathlib import Path

import polars as pl
import pytest

from gdeltforge.utils.io import (
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
