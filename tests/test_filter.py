from pathlib import Path

import pandas as pd
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
    def test_defaults_to_snappy(self, tmp_path):
        filt = GDELTFilter(str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"])
        assert filt.compression == "snappy"

    def test_explicit_codec_is_used_on_write(self, tmp_path):
        input_dir = tmp_path / "in"
        input_dir.mkdir()
        src = input_dir / "data.parquet"
        _write_parquet(src, {"GlobalEventID": [1, 2], "QuadClass": [1, 2]})

        filt = GDELTFilter(
            str(input_dir), str(tmp_path / "out"), ["QuadClass"], compression="zstd",
        )
        out_path = tmp_path / "out" / "data_filtered.parquet"
        filt.filter_single_file(src, out_path)

        metadata = pq.ParquetFile(out_path).metadata
        codec = metadata.row_group(0).column(0).compression
        assert codec.lower() == "zstd"


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

    def test_missing_output_columns_and_compression_keys_default_to_full_snappy(self, tmp_path):
        # Same backward-compatibility guarantee as max_workers above: configs
        # that pre-date output_columns/compression must not KeyError, and
        # must keep writing every column at snappy as before.
        cfg, events_in, _ = self._config(tmp_path)
        pd.DataFrame(
            {"GlobalEventID": [1], "Actor1Name": ["A"]}
        ).to_parquet(events_in / "a.parquet")

        processed, failed = run_filter(cfg)

        assert (processed, failed) == (1, 0)
        out = pd.read_parquet(cfg["paths"]["filtered_data_directory"] + "/a_filtered.parquet")
        assert list(out.columns) == ["GlobalEventID", "Actor1Name"]

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


class TestFilterSingleFileAtomicity:
    """filter_single_file used to write straight to output_path via a
    streaming ParquetWriter. Now that filter_all_files runs files across a
    worker pool (TestMaxWorkersConfig), a killed worker is a real,
    reachable failure mode, not just a hypothetical -- a truncated file
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
