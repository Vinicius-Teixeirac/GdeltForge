import logging
from datetime import date
from pathlib import Path

import polars as pl
import polars.testing as pl_testing
import pytest

import gdeltforge.crossref.crossref as crossref_module
from gdeltforge.crossref.crossref import (
    GKG_V1_COVERAGE_START,
    GKG_V2_COVERAGE_START,
    OPTIONAL_MENTIONS_PAYLOAD_COLUMNS,
    REQUIRED_JOIN_COLUMNS,
    crossref_events_gkg_auto,
    crossref_events_gkg_v1,
    crossref_events_gkg_v2,
    warn_if_directory_is_large,
    warn_if_events_df_is_large,
    warn_if_events_predate_gkg_coverage,
    warn_if_output_columns_drops_join_key,
)
from gdeltforge.scraping.scraper import parse_gdeltv2_file_date

GKG_V1_COLUMNS = ["Date", "EventIds", "NumArticles", "Themes"]
GKG_V2_COLUMNS = ["V2DOCUMENTIDENTIFIER", "GKGRECORDID", "V1THEMES"]


class TestRequiredJoinColumns:
    """REQUIRED_JOIN_COLUMNS is imported by both converter.py's
    run_converter and filter.py's run_filter to power a proactive
    output_columns warning (see test_converter.py / test_filter.py), so
    its keys and values are a real cross-module contract, not just
    internal detail."""

    def test_covers_every_dataset_a_crossref_path_touches(self):
        assert set(REQUIRED_JOIN_COLUMNS) == {
            "gdelt_event", "gdelt_gkg_v1", "gdelt_gkg_v1_counts",
            "gdelt_gkg_v2", "gdelt_mentions",
        }

    def test_matches_the_columns_actually_enforced_by_require_column(self):
        assert REQUIRED_JOIN_COLUMNS["gdelt_event"] == ("GlobalEventID",)
        assert REQUIRED_JOIN_COLUMNS["gdelt_gkg_v1"] == ("EventIds",)
        assert REQUIRED_JOIN_COLUMNS["gdelt_gkg_v1_counts"] == ("EventIds",)
        assert REQUIRED_JOIN_COLUMNS["gdelt_gkg_v2"] == ("V2DOCUMENTIDENTIFIER",)
        assert REQUIRED_JOIN_COLUMNS["gdelt_mentions"] == ("GLOBALEVENTID", "MentionIdentifier")

    def test_optional_mentions_payload_columns_are_disjoint_from_required(self):
        # MentionTimeDate/Confidence must never end up in both sets: that
        # would make a column simultaneously join-breaking-if-missing
        # (REQUIRED_JOIN_COLUMNS) and gracefully-omittable-if-missing
        # (OPTIONAL_MENTIONS_PAYLOAD_COLUMNS), a contradiction.
        assert set(REQUIRED_JOIN_COLUMNS["gdelt_mentions"]).isdisjoint(
            OPTIONAL_MENTIONS_PAYLOAD_COLUMNS
        )

    def test_gdelt_event_reduced_has_no_entry(self):
        # gdelt_event_reduced is deliberately absent: GDELT.MASTERREDUCEDV2
        # .1979-2013.zip carries no GlobalEventID, SOURCEURL, or DATEADDED
        # at all (it's a pre-aggregated DATE+ACTOR1+ACTOR2+EVENTCODE roll-
        # up), so no crossref path can ever join against it. See
        # TestEventsReducedCannotJoinThroughCrossref below for the actual
        # failure this produces.
        assert "gdelt_event_reduced" not in REQUIRED_JOIN_COLUMNS


class TestEventsReducedCannotJoinThroughCrossref:
    """gdelt_event_reduced's real 17 columns (Date, Source, Target,
    CAMEOCode, NumEvents, NumArts, QuadClass, Goldstein, and the
    Source/Target/Action geo fields) include no GlobalEventID, SOURCEURL,
    or DATEADDED: it's a pre-aggregated historical dump, not per-event
    data, so no bridge to Mentions/GKG exists for it to join through.
    Confirms this fails clearly at crossref time (the same
    _require_column check every other missing-join-key case already
    goes through, see TestCrossrefEventsGkgV1.
    test_missing_global_event_id_column_raises), rather than silently
    returning zero rows or a confusing failure further downstream."""

    @staticmethod
    def _events_reduced_df():
        return pl.DataFrame({
            "Date": [19790101, 19790615],
            "Source": ["USA", "USA"],
            "Target": ["GBR", "GBR"],
            "CAMEOCode": ["010", "020"],
            "NumEvents": [5, 2],
            "NumArts": [3, 1],
            "QuadClass": [1, 2],
            "Goldstein": [1.5, -1.5],
            "SourceGeoType": [1, 1],
            "SourceGeoLat": [10.0, 10.0],
            "SourceGeoLong": [-20.0, -20.0],
            "TargetGeoType": [1, 1],
            "TargetGeoLat": [11.0, 11.0],
            "TargetGeoLong": [-21.0, -21.0],
            "ActionGeoType": [1, 1],
            "ActionGeoLat": [12.0, 12.0],
            "ActionGeoLong": [-22.0, -22.0],
        })

    def test_crossref_events_gkg_v1_rejects_it(self):
        with pytest.raises(ValueError, match="GlobalEventID"):
            crossref_events_gkg_v1(
                self._events_reduced_df(), "unused_gkg_folder", GKG_V1_COLUMNS
            )

    def test_crossref_events_gkg_v2_rejects_it(self):
        with pytest.raises(ValueError, match="GlobalEventID"):
            crossref_events_gkg_v2(
                self._events_reduced_df(), "unused_mentions_folder",
                "unused_gkg_folder", GKG_V2_COLUMNS,
            )

    def test_crossref_events_gkg_auto_rejects_it(self):
        with pytest.raises(ValueError, match="GlobalEventID"):
            crossref_events_gkg_auto(
                self._events_reduced_df(),
                "unused_gkg_v1_folder", GKG_V1_COLUMNS,
                "unused_mentions_folder",
                "unused_gkg_v2_folder", GKG_V2_COLUMNS,
            )


class TestWarnIfOutputColumnsDropsJoinKey:
    """Core logic shared by run_converter and run_filter; each module's
    own tests only need to prove they call this with the right
    arguments, not re-verify the logic itself."""

    def test_warns_when_the_join_key_is_missing(self, caplog):
        with caplog.at_level(logging.WARNING):
            warn_if_output_columns_drops_join_key(
                logging.getLogger("test"), "convert", "gdelt_event", ["Actor1Name"]
            )
        assert any(
            "GlobalEventID" in r.message and "crossref" in r.message for r in caplog.records
        )

    def test_no_warning_when_the_join_key_is_present(self, caplog):
        with caplog.at_level(logging.WARNING):
            warn_if_output_columns_drops_join_key(
                logging.getLogger("test"), "convert", "gdelt_event",
                ["GlobalEventID", "Actor1Name"],
            )
        assert caplog.records == []

    def test_no_warning_when_output_columns_is_none(self, caplog):
        # None means every column survives; nothing to warn about.
        with caplog.at_level(logging.WARNING):
            warn_if_output_columns_drops_join_key(
                logging.getLogger("test"), "convert", "gdelt_event", None
            )
        assert caplog.records == []

    def test_unrecognized_dataset_name_neither_warns_nor_errors(self, caplog):
        # REQUIRED_JOIN_COLUMNS only covers the five real pipeline
        # datasets; .get() returning None for anything else must be a
        # silent no-op, not a KeyError, so this stays forward-compatible
        # with any future dataset that isn't crossref-relevant.
        with caplog.at_level(logging.WARNING):
            warn_if_output_columns_drops_join_key(
                logging.getLogger("test"), "convert", "gdelt_some_future_dataset",
                ["SomeColumn"],
            )
        assert caplog.records == []

    def test_stage_name_is_attributed_correctly_in_the_message(self, caplog):
        with caplog.at_level(logging.WARNING):
            warn_if_output_columns_drops_join_key(
                logging.getLogger("test"), "convert", "gdelt_event", []
            )
        assert any("convert.output_columns.gdelt_event" in r.message for r in caplog.records)


class TestWarnIfEventsPredateGkgCoverage:
    """Core logic shared by crossref_events_gkg_v1 and _v2; each join
    function's own tests only need to prove they call this with the
    right arguments, not re-verify the logic itself."""

    def test_coverage_start_constants_match_gdelt_s_real_file_listings(self):
        # Verified 2026-08-07 against real GDELT file listings, not the
        # codebook alone: GKG 1.0's earliest published file is
        # 20130401.gkg.csv.zip; the earliest file in
        # gdeltv2/masterfilelist.txt for GKG 2.1, the 15-minute Events
        # feed, and Mentions alike is 20150218230000.
        assert GKG_V1_COVERAGE_START == 20130401
        assert GKG_V2_COVERAGE_START == 20150218

    def test_no_warning_when_all_events_are_within_coverage(self, caplog):
        events_df = pl.DataFrame({"DATEADDED": [20200101, 20200102]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        assert caplog.records == []

    def test_warns_when_all_events_predate_coverage(self, caplog):
        events_df = pl.DataFrame({"DATEADDED": [20100101, 20120101]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        assert any(
            "All 2" in r.message and "GKG 1.0" in r.message and "20130401" in r.message
            for r in caplog.records
        )

    def test_warns_with_a_partial_count_when_only_some_events_predate_coverage(self, caplog):
        events_df = pl.DataFrame({"DATEADDED": [20100101, 20200101, 20200102]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        assert any("1 of 3" in r.message for r in caplog.records)

    def test_no_warning_when_dateadded_column_is_absent(self, caplog):
        # A sample built with --columns that excluded DATEADDED: this is
        # a diagnostic on top of the join, not something the join itself
        # depends on, so it must degrade silently, not error.
        events_df = pl.DataFrame({"GlobalEventID": [1, 2]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        assert caplog.records == []

    def test_no_warning_on_empty_dateadded(self, caplog):
        events_df = pl.DataFrame({"DATEADDED": pl.Series([], dtype=pl.Float64)})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        assert caplog.records == []

    def test_null_dateadded_values_are_excluded_from_the_count(self, caplog):
        events_df = pl.DataFrame({"DATEADDED": [20100101, None, 20200101]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        # 1 real pre-coverage row out of 2 non-null rows, not 3.
        assert any("1 of 2" in r.message for r in caplog.records)


class TestWarnIfEventsDfIsLarge:
    """Core logic shared by crossref_events_gkg_v1 and _v2; each join
    function's own tests only need to prove they call this, not
    re-verify the logic itself. Never a hard error: an archive-scale
    join is a legitimate thing to ask for, just an expensive one."""

    def test_no_warning_at_or_below_threshold(self, caplog):
        n = crossref_module._LARGE_EVENTS_JOIN_WARNING_THRESHOLD
        events_df = pl.DataFrame({"GlobalEventID": range(n)})
        with caplog.at_level(logging.WARNING):
            warn_if_events_df_is_large(events_df)
        assert caplog.records == []

    def test_warns_above_threshold(self, caplog):
        n = crossref_module._LARGE_EVENTS_JOIN_WARNING_THRESHOLD + 1
        events_df = pl.DataFrame({"GlobalEventID": range(n)})
        with caplog.at_level(logging.WARNING):
            warn_if_events_df_is_large(events_df)
        assert any(
            f"{n:,}" in r.message and "gdeltforge sample" in r.message for r in caplog.records
        )

    def test_no_warning_for_a_typical_bounded_sample(self, caplog):
        events_df = pl.DataFrame({"GlobalEventID": [1, 2, 3]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_df_is_large(events_df)
        assert caplog.records == []


class TestWarnIfDirectoryIsLarge:
    """Core logic shared by crossref_events_gkg_v1 (its gkg_folder) and
    _v2 (its mentions_folder and gkg_v2_folder, checked independently);
    each join function's own tests only need to prove they call this for
    the right folder(s), not re-verify the logic itself. Never a hard
    error: a genuinely large local archive is a real directory someone
    might legitimately point crossref at, just a slow one to list.

    Path.glob is faked here rather than creating tens of thousands of
    real files on disk, matching the same trick this file's dedup tests
    already use (test_reprocessed_article_dedup_is_correct_regardless_
    of_glob_order): fine for these isolated tests, which call
    warn_if_directory_is_large directly and never reach _dataset()'s own
    real file I/O the way the integration tests below do.
    """

    @staticmethod
    def _fake_glob(n):
        def glob(_self, _pattern):
            return [Path(f"{i}.parquet") for i in range(n)]
        return glob

    def test_no_warning_at_or_below_threshold(self, tmp_path, caplog, monkeypatch):
        n = crossref_module._LARGE_GKG_DIRECTORY_WARNING_THRESHOLD
        monkeypatch.setattr(Path, "glob", self._fake_glob(n))
        with caplog.at_level(logging.WARNING):
            warn_if_directory_is_large(str(tmp_path), "Mentions", parse_gdeltv2_file_date)
        assert caplog.records == []

    def test_warns_above_threshold(self, tmp_path, caplog, monkeypatch):
        n = crossref_module._LARGE_GKG_DIRECTORY_WARNING_THRESHOLD + 1
        monkeypatch.setattr(Path, "glob", self._fake_glob(n))
        with caplog.at_level(logging.WARNING):
            warn_if_directory_is_large(str(tmp_path), "Mentions", parse_gdeltv2_file_date)
        assert any(
            f"{n:,}" in r.message and "Mentions" in r.message and repr(str(tmp_path)) in r.message
            for r in caplog.records
        )

    def test_no_warning_for_a_typical_local_directory(self, tmp_path, caplog):
        (tmp_path / "a.parquet").touch()
        (tmp_path / "b.parquet").touch()
        with caplog.at_level(logging.WARNING):
            warn_if_directory_is_large(str(tmp_path), "Mentions", parse_gdeltv2_file_date)
        assert caplog.records == []

    def test_counts_the_post_date_filter_list_not_the_raw_directory(
        self, tmp_path, caplog, monkeypatch
    ):
        # Three real, date-parseable files; narrowing to the last two
        # must be what gets counted, not the raw directory's three,
        # proving the file count this warning reports is the same list
        # crossref_events_gkg_v1/_v2 would actually open with the same
        # bounds, not a stale, pre-narrowing total.
        for name in ("20200101000000", "20200102000000", "20200103000000"):
            (tmp_path / f"{name}.gkg.parquet").touch()
        monkeypatch.setattr(crossref_module, "_LARGE_GKG_DIRECTORY_WARNING_THRESHOLD", 1)

        with caplog.at_level(logging.WARNING):
            warn_if_directory_is_large(
                str(tmp_path), "GKG 2.1", parse_gdeltv2_file_date,
                start_date=date(2020, 1, 2), end_date=None,
            )

        assert any("2 files" in r.message and "GKG 2.1" in r.message for r in caplog.records)
        assert not any("3 files" in r.message for r in caplog.records)


# ------------------------------------------------------------
# crossref_events_gkg_v1: direct join on EventIds
# ------------------------------------------------------------
class TestCrossrefEventsGkgV1:
    @staticmethod
    def _events_df():
        return pl.DataFrame({
            "GlobalEventID": [1001, 1002, 1003],
            "Actor1Name": ["Alice", "Bob", "Carol"],
            "NumArticles": [5, 3, 7],
        })

    @staticmethod
    def _write_gkg_v1(tmp_path):
        folder = tmp_path / "gkg_v1"
        folder.mkdir()
        pl.DataFrame({
            "Date": [20130401, 20130401],
            "EventIds": ["1001,1002", "9999"],
            "NumArticles": [10, 2],
            "Themes": ["TAX_FNCACT", "UNRELATED"],
        }).write_parquet(folder / "20130401.gkg.parquet")
        pl.DataFrame({
            "Date": [20130402, 20130402],
            "EventIds": ["1001", None],
            "NumArticles": [4, 1],
            "Themes": ["ECON_STOCKMARKET", "EMPTY_TEST"],
        }).write_parquet(folder / "20130402.gkg.parquet")
        return str(folder)

    def test_basic_join_and_hand_computed_row_count(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        result = crossref_events_gkg_v1(self._events_df(), folder, GKG_V1_COLUMNS)

        # Event 1001 matches "1001,1002" (batch 1) AND "1001" (batch 2) -> 2 rows.
        # Event 1002 matches "1001,1002" only -> 1 row.
        # Event 1003 matches nothing -> 0 rows.
        assert len(result) == 3
        assert sorted(result["GlobalEventID"]) == [1001, 1001, 1002]

    def test_event_with_no_gkg_match_is_absent_not_a_null_row(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        result = crossref_events_gkg_v1(self._events_df(), folder, GKG_V1_COLUMNS)

        assert 1003 not in set(result["GlobalEventID"])

    def test_gkg_row_naming_multiple_events_is_not_collapsed(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        result = crossref_events_gkg_v1(self._events_df(), folder, GKG_V1_COLUMNS)

        # The "1001,1002" row must produce two separate output rows (one
        # per event), sharing the same GKG-side data, not one merged row.
        shared = result.filter(pl.col("GKG_EventIds") == "1001,1002")
        assert sorted(shared["GlobalEventID"]) == [1001, 1002]
        assert len(set(shared["GKG_Themes"])) == 1

    def test_unrelated_and_empty_eventids_never_leak_into_output(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        result = crossref_events_gkg_v1(self._events_df(), folder, GKG_V1_COLUMNS)

        assert "UNRELATED" not in set(result["GKG_Themes"])
        assert "EMPTY_TEST" not in set(result["GKG_Themes"])

    def test_colliding_column_name_is_disambiguated_by_prefix(self, tmp_path):
        # NumArticles exists on both Events and GKG 1.0; both must survive
        # the join distinctly, neither silently overwriting the other.
        folder = self._write_gkg_v1(tmp_path)
        result = crossref_events_gkg_v1(self._events_df(), folder, GKG_V1_COLUMNS)

        row = result.filter(
            (pl.col("GlobalEventID") == 1001) & (pl.col("GKG_EventIds") == "1001,1002")
        ).row(0, named=True)
        assert row["NumArticles"] == 5       # Events' own NumArticles
        assert row["GKG_NumArticles"] == 10  # GKG's NumArticles, untouched

    def test_columns_restricts_gkg_side_output(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        result = crossref_events_gkg_v1(
            self._events_df(), folder, GKG_V1_COLUMNS, columns={"Date"}
        )

        assert "GKG_Date" in result.columns
        assert "GKG_Themes" not in result.columns
        assert "GKG_NumArticles" not in result.columns
        # EventIds is always read (it's the join key) even when not requested.
        assert "GKG_EventIds" in result.columns

    def test_invalid_columns_raises(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        with pytest.raises(ValueError, match="Invalid columns"):
            crossref_events_gkg_v1(
                self._events_df(), folder, GKG_V1_COLUMNS, columns={"NotARealColumn"}
            )

    def test_missing_global_event_id_column_raises(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        bad_events = self._events_df().drop(["GlobalEventID"])
        with pytest.raises(ValueError, match="GlobalEventID"):
            crossref_events_gkg_v1(bad_events, folder, GKG_V1_COLUMNS)

    def test_missing_eventids_in_schema_raises(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        with pytest.raises(ValueError, match="EventIds"):
            crossref_events_gkg_v1(self._events_df(), folder, ["Date", "NumArticles"])

    def test_no_matches_returns_empty_dataframe(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        events_df = pl.DataFrame({"GlobalEventID": [424242], "NumArticles": [1]})
        result = crossref_events_gkg_v1(events_df, folder, GKG_V1_COLUMNS)
        assert result.is_empty()

    def test_warns_when_some_events_predate_gkg_v1_coverage(self, tmp_path, caplog):
        # Event 1001 (real match, DATEADDED within coverage) must still
        # join normally alongside event 1002 (pre-coverage, gets warned
        # about): the warning is a diagnostic, not a filter.
        folder = self._write_gkg_v1(tmp_path)
        events_df = pl.DataFrame({
            "GlobalEventID": [1001, 1002],
            "DATEADDED": [20130401, 20120101],
        })

        with caplog.at_level("WARNING"):
            result = crossref_events_gkg_v1(events_df, folder, GKG_V1_COLUMNS)

        assert any(
            "1 of 2" in r.message and "GKG 1.0" in r.message for r in caplog.records
        )
        assert 1001 in set(result["GlobalEventID"])

    def test_warns_when_events_df_is_large(self, tmp_path, caplog, monkeypatch):
        # Threshold lowered to keep this fast: the real join over a
        # genuinely 1M+-row events_df is exactly the cost this warning
        # exists to flag, not something a unit test should pay to prove
        # the wiring is in place. TestWarnIfEventsDfIsLarge covers the
        # real default threshold and message content in isolation.
        monkeypatch.setattr(crossref_module, "_LARGE_EVENTS_JOIN_WARNING_THRESHOLD", 1)
        folder = self._write_gkg_v1(tmp_path)

        with caplog.at_level("WARNING"):
            crossref_events_gkg_v1(self._events_df(), folder, GKG_V1_COLUMNS)

        assert any("gdeltforge sample" in r.message for r in caplog.records)

    def test_warns_when_gkg_directory_is_large(self, tmp_path, caplog, monkeypatch):
        # Real fixture directory (2 files), threshold lowered to 0 rather
        # than faking thousands of files: TestWarnIfDirectoryIsLarge
        # already covers the real default threshold and message content
        # in isolation, this only proves v1 checks its own gkg_folder.
        monkeypatch.setattr(crossref_module, "_LARGE_GKG_DIRECTORY_WARNING_THRESHOLD", 0)
        folder = self._write_gkg_v1(tmp_path)

        with caplog.at_level("WARNING"):
            crossref_events_gkg_v1(self._events_df(), folder, GKG_V1_COLUMNS)

        assert any("GKG 1.0 directory" in r.message for r in caplog.records)

    def test_start_date_excludes_the_earlier_file(self, tmp_path):
        # _write_gkg_v1 writes 20130401.gkg.parquet ("1001,1002"/"9999")
        # and 20130402.gkg.parquet ("1001"/None). Excluding the earlier
        # file removes event 1002's only match, leaving just 1001's
        # match through the later file.
        folder = self._write_gkg_v1(tmp_path)
        result = crossref_events_gkg_v1(
            self._events_df(), folder, GKG_V1_COLUMNS, start_date=date(2013, 4, 2),
        )

        assert sorted(result["GlobalEventID"]) == [1001]
        assert list(result["GKG_EventIds"]) == ["1001"]

    def test_end_date_excludes_the_later_file(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        result = crossref_events_gkg_v1(
            self._events_df(), folder, GKG_V1_COLUMNS, end_date=date(2013, 4, 1),
        )

        assert sorted(result["GlobalEventID"]) == [1001, 1002]
        assert set(result["GKG_EventIds"]) == {"1001,1002"}

    def test_no_date_bounds_matches_the_no_argument_default(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        explicit = crossref_events_gkg_v1(
            self._events_df(), folder, GKG_V1_COLUMNS, start_date=None, end_date=None,
        )
        default = crossref_events_gkg_v1(self._events_df(), folder, GKG_V1_COLUMNS)

        pl_testing.assert_frame_equal(explicit, default)

    def test_date_range_excluding_every_file_raises(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        with pytest.raises(FileNotFoundError, match="No parquet files"):
            crossref_events_gkg_v1(
                self._events_df(), folder, GKG_V1_COLUMNS, start_date=date(2013, 5, 1),
            )


class TestIterRowSlicesBoundedByExplosion:
    """_iter_row_slices_bounded_by_explosion caps how many rows a single
    explode step downstream can produce, found necessary via a live
    comprehensive QA pass: a real GKG 1.0 Counts file had one row with
    13,051 comma-separated EventIds against a same-file mean of ~37,
    which let that single row dominate an entire batch's peak memory
    once exploded, surfacing as a Rust-level allocation failure under
    real memory pressure."""

    @staticmethod
    def _slice_lengths(df, list_col, max_exploded):
        return [
            s.height
            for s in crossref_module._iter_row_slices_bounded_by_explosion(
                df, list_col, max_exploded
            )
        ]

    def test_ordinary_rows_stay_in_one_slice_when_under_the_cap(self):
        df = pl.DataFrame({"ids": [["a", "b"], ["c"], ["d", "e", "f"]]})
        assert self._slice_lengths(df, "ids", max_exploded=100) == [3]

    def test_a_slice_boundary_is_drawn_once_the_running_total_would_exceed_the_cap(self):
        df = pl.DataFrame({"ids": [["a"], ["b"], ["c"], ["d"]]})
        # Running totals: 1, 2, 3, 4. Capped at 2, a slice never accumulates
        # past it, so the boundary falls after every second row.
        assert self._slice_lengths(df, "ids", max_exploded=2) == [2, 2]

    def test_a_single_pathological_row_gets_its_own_slice_rather_than_looping_forever(self):
        df = pl.DataFrame({"ids": [["a"], [str(i) for i in range(50)], ["b"]]})
        # The middle row's own length (50) already exceeds the cap (10); it
        # must still be yielded, alone, not silently dropped or split.
        assert self._slice_lengths(df, "ids", max_exploded=10) == [1, 1, 1]

    def test_every_row_is_covered_exactly_once(self):
        df = pl.DataFrame({"ids": [["a", "b"], ["c"], ["d", "e"], ["f"], ["g", "h", "i"]]})
        total = sum(self._slice_lengths(df, "ids", max_exploded=3))
        assert total == df.height

    def test_join_result_is_identical_whether_or_not_bounding_forces_multiple_steps(
        self, tmp_path, monkeypatch
    ):
        # A real end-to-end join, forced through several internal bounded
        # steps by lowering the cap far below what a real run would ever
        # use, must produce exactly the same rows as the unbounded case:
        # the cap only changes how much memory one step touches at a time,
        # never which matches are found.
        folder = tmp_path / "gkg_v1"
        folder.mkdir()
        pl.DataFrame({
            "Date": [20130401] * 3,
            "EventIds": [
                ",".join(str(1000 + i) for i in range(20)),  # one large row
                "1001,1002",
                "9999",
            ],
            "NumArticles": [1, 2, 3],
        }).write_parquet(folder / "20130401.gkg.parquet")
        events_df = pl.DataFrame({"GlobalEventID": list(range(1000, 1020))})
        gkg_columns = ["Date", "EventIds", "NumArticles"]

        unbounded = crossref_events_gkg_v1(events_df, str(folder), gkg_columns)

        monkeypatch.setattr(crossref_module, "_MAX_EXPLODED_ROWS_PER_STEP", 3)
        bounded = crossref_events_gkg_v1(events_df, str(folder), gkg_columns)

        pl_testing.assert_frame_equal(
            unbounded.sort("GlobalEventID", "GKG_EventIds"),
            bounded.sort("GlobalEventID", "GKG_EventIds"),
        )


class TestCrossrefEventsGkgV1ColumnNarrowing:
    """
    Regression coverage for a real gap found via a live comprehensive
    QA pass: without --columns, the GKG-side read defaults to this
    dataset's full declared schema (GKG_V1_COLUMNS here), which isn't
    the same thing as what a real, possibly output_columns-pruned file
    on disk actually has. That mismatch used to crash with a raw,
    unhelpful polars error ("unable to find column ...") instead of
    working around it. See utils/io.py's own TestNarrowToAvailableColumns
    for the narrowing/warning logic itself; this only confirms
    crossref_events_gkg_v1 wires it in correctly.
    """

    @staticmethod
    def _events_df():
        return pl.DataFrame({"GlobalEventID": [1001, 1002], "NumArticles": [5, 3]})

    @staticmethod
    def _write_pruned_gkg_v1(tmp_path):
        # Themes and NumArticles are both in GKG_V1_COLUMNS but absent
        # here, the shape filter.output_columns pruning a dataset down
        # to just the join key (plus whatever else was kept) produces.
        folder = tmp_path / "gkg_v1_pruned"
        folder.mkdir()
        pl.DataFrame({
            "Date": [20130401, 20130401],
            "EventIds": ["1001,1002", "9999"],
        }).write_parquet(folder / "20130401.gkg.parquet")
        return str(folder)

    def test_default_full_schema_projection_narrows_with_a_warning(self, tmp_path, caplog):
        folder = self._write_pruned_gkg_v1(tmp_path)

        with caplog.at_level("WARNING"):
            result = crossref_events_gkg_v1(self._events_df(), folder, GKG_V1_COLUMNS)

        assert sorted(result["GlobalEventID"]) == [1001, 1002]
        assert "GKG_Date" in result.columns
        assert "GKG_Themes" not in result.columns
        assert "GKG_NumArticles" not in result.columns
        assert any(
            "Themes" in r.message and "NumArticles" in r.message for r in caplog.records
        )

    def test_join_key_genuinely_missing_raises_clearly(self, tmp_path):
        # EventIds is declared in GKG_V1_COLUMNS (passes the earlier,
        # declared-schema-only _require_column check) but this file
        # doesn't actually have it: the join can't run at all without
        # it, so this must raise rather than silently narrow it away.
        folder = tmp_path / "gkg_v1_no_join_key"
        folder.mkdir()
        pl.DataFrame({"Date": [20130401]}).write_parquet(folder / "20130401.gkg.parquet")

        with pytest.raises(ValueError, match="required column.*EventIds"):
            crossref_events_gkg_v1(self._events_df(), folder, GKG_V1_COLUMNS)


# ------------------------------------------------------------
# crossref_events_gkg_v2: two-hop join through Mentions
# ------------------------------------------------------------
class TestCrossrefEventsGkgV2:
    @staticmethod
    def _events_df():
        return pl.DataFrame({
            "GlobalEventID": [2001, 2002, 2003],
            "Actor1Name": ["Dave", "Erin", "Frank"],
        })

    @staticmethod
    def _write_mentions(tmp_path):
        folder = tmp_path / "mentions"
        folder.mkdir()
        pl.DataFrame({
            "GLOBALEVENTID": [2001, 2001, 2002, 9999],
            "MentionIdentifier": [
                "http://a.com/article1",
                "http://a.com/article2",
                "http://a.com/article1",   # same article also covers event 2001
                "http://a.com/unrelated",
            ],
            "MentionTimeDate": [20200101120000, 20200102120000, 20200101130000, 20200101000000],
            "Confidence": [80, 90, 70, 50],
        }).write_parquet(folder / "20200101120000.mentions.parquet")
        return str(folder)

    @staticmethod
    def _write_gkg_v2(tmp_path):
        folder = tmp_path / "gkg_v2"
        folder.mkdir()
        pl.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://a.com/article1", "http://c.com/unrelated"],
            "GKGRECORDID": ["REC1-early", "REC-unrelated"],
            "V1THEMES": ["THEME_OLD", "THEME_X"],
        }).write_parquet(folder / "20200101000000.gkg.parquet")
        pl.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://a.com/article1", "http://a.com/article2"],
            "GKGRECORDID": ["REC1-late", "REC2"],
            "V1THEMES": ["THEME_NEW", "THEME_Y"],
        }).write_parquet(folder / "20200102000000.gkg.parquet")
        return str(folder)

    def test_basic_join_and_hand_computed_row_count(self, tmp_path):
        # on_duplicate_document="latest" pins article1 to its single most
        # recent GKG record, isolating this test's own concern (basic
        # row count) from the reprocessed-article behavior covered by
        # its own dedicated tests below.
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="latest",
        )

        # 2001 x article1, 2001 x article2, 2002 x article1 -> 3 rows.
        assert len(result) == 3
        assert sorted(result["GlobalEventID"]) == [2001, 2001, 2002]

    def test_event_with_no_mentions_is_absent(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS
        )
        assert 2003 not in set(result["GlobalEventID"])

    def test_article_covering_multiple_events_is_not_collapsed(self, tmp_path):
        # on_duplicate_document="latest" isolates this test's own concern
        # (event-side non-collapsing) from the GKG-side reprocessed-
        # article behavior covered separately below.
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="latest",
        )
        article1_rows = result.filter(pl.col("GKG_V2DOCUMENTIDENTIFIER") == "http://a.com/article1")
        assert sorted(article1_rows["GlobalEventID"]) == [2001, 2002]

    def test_reprocessed_article_deduped_keeping_the_latest_batch(self, tmp_path):
        # "all" is the default now (every GKG record for a shared URL is
        # kept); this test is specifically about on_duplicate_document=
        # "latest", so it passes that explicitly rather than relying on
        # whatever the default happens to be.
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="latest",
        )
        article1_rows = result.filter(pl.col("GKG_V2DOCUMENTIDENTIFIER") == "http://a.com/article1")
        # Both the 2001 and 2002 rows for article1 must carry the SAME,
        # latest GKG record (REC1-late), not the earlier REC1-early, and
        # article1 must never contribute two GKG-side variants at once.
        assert set(article1_rows["GKG_GKGRECORDID"]) == {"REC1-late"}

    def test_reprocessed_article_dedup_is_correct_regardless_of_glob_order(
        self, tmp_path, monkeypatch
    ):
        # Path.glob's return order is filesystem-dependent, not guaranteed
        # sorted. Confirmed for real: the previous test passed locally on
        # Windows/NTFS by coincidence (alphabetical happens to match
        # chronological order for GDELT's YYYYMMDDHHMMSS filenames there),
        # but failed on Linux/ext4 in CI, where glob came back in a
        # different order and the dedup silently kept the stale record
        # instead. Forces the adversarial case directly: glob returns the
        # files in reverse (most-recent-first) order, and the dedup must
        # still keep the chronologically later record, proving
        # _dataset()'s explicit sort is what's relied on, not whatever
        # order the filesystem happens to hand back.
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        real_glob = Path.glob

        def reversed_glob(self, pattern):
            return list(reversed(list(real_glob(self, pattern))))

        monkeypatch.setattr(Path, "glob", reversed_glob)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="latest",
        )
        article1_rows = result.filter(pl.col("GKG_V2DOCUMENTIDENTIFIER") == "http://a.com/article1")
        assert set(article1_rows["GKG_GKGRECORDID"]) == {"REC1-late"}

    def test_unrelated_rows_on_either_side_never_leak_in(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS
        )
        assert 9999 not in set(result["GlobalEventID"])
        assert "http://c.com/unrelated" not in set(result["GKG_V2DOCUMENTIDENTIFIER"])
        assert "REC-unrelated" not in set(result["GKG_GKGRECORDID"])

    def test_mention_bridge_fields_are_prefixed_and_carried_through(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS
        )
        assert "Mention_MentionTimeDate" in result.columns
        assert "Mention_Confidence" in result.columns

    def test_missing_optional_payload_column_is_omitted_not_fatal(self, tmp_path):
        # MentionTimeDate and Confidence are payload, not join keys
        # (only GLOBALEVENTID and MentionIdentifier are); a Mentions
        # dataset missing one must still join successfully, just without
        # that Mention_<name> column in the output.
        folder = tmp_path / "mentions_no_confidence"
        folder.mkdir()
        pl.DataFrame({
            "GLOBALEVENTID": [2001, 2002],
            "MentionIdentifier": ["http://a.com/article1", "http://a.com/article1"],
            "MentionTimeDate": [20200101120000, 20200101130000],
        }).write_parquet(folder / "20200101120000.mentions.parquet")
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), str(folder), gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="latest",
        )

        assert len(result) == 2
        assert "Mention_MentionTimeDate" in result.columns
        assert "Mention_Confidence" not in result.columns

    def test_missing_both_optional_payload_columns_still_joins(self, tmp_path):
        folder = tmp_path / "mentions_bare"
        folder.mkdir()
        pl.DataFrame({
            "GLOBALEVENTID": [2001],
            "MentionIdentifier": ["http://a.com/article1"],
        }).write_parquet(folder / "20200101120000.mentions.parquet")
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), str(folder), gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="latest",
        )

        assert len(result) == 1
        assert result["GlobalEventID"][0] == 2001
        assert "Mention_MentionTimeDate" not in result.columns
        assert "Mention_Confidence" not in result.columns

    def test_columns_restricts_gkg_side_output(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            columns={"GKGRECORDID"},
        )
        assert "GKG_GKGRECORDID" in result.columns
        assert "GKG_V1THEMES" not in result.columns
        # V2DOCUMENTIDENTIFIER is always read (it's the join key).
        assert "GKG_V2DOCUMENTIDENTIFIER" in result.columns

    def test_invalid_columns_raises(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)
        with pytest.raises(ValueError, match="Invalid columns"):
            crossref_events_gkg_v2(
                self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
                columns={"NotARealColumn"},
            )

    def test_missing_global_event_id_column_raises(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)
        bad_events = self._events_df().drop(["GlobalEventID"])
        with pytest.raises(ValueError, match="GlobalEventID"):
            crossref_events_gkg_v2(bad_events, mentions_folder, gkg_folder, GKG_V2_COLUMNS)

    def test_missing_document_identifier_in_schema_raises(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)
        with pytest.raises(ValueError, match="V2DOCUMENTIDENTIFIER"):
            crossref_events_gkg_v2(
                self._events_df(), mentions_folder, gkg_folder, ["GKGRECORDID"]
            )

    def test_missing_global_event_id_in_mentions_schema_raises_cleanly(self, tmp_path):
        # Previously this reached pyarrow's own to_table(columns=[...])
        # call unchecked, surfacing a raw pyarrow error instead of the
        # same clean, consistent ValueError every other required column
        # gets.
        folder = tmp_path / "mentions_missing_event_id"
        folder.mkdir()
        pl.DataFrame({
            "MentionIdentifier": ["http://a.com/article1"],
            "MentionTimeDate": [20200101120000],
            "Confidence": [80],
        }).write_parquet(folder / "20200101120000.mentions.parquet")
        gkg_folder = self._write_gkg_v2(tmp_path)

        with pytest.raises(ValueError, match="GLOBALEVENTID"):
            crossref_events_gkg_v2(
                self._events_df(), str(folder), gkg_folder, GKG_V2_COLUMNS
            )

    def test_missing_mention_identifier_in_mentions_schema_raises_cleanly(self, tmp_path):
        folder = tmp_path / "mentions_missing_identifier"
        folder.mkdir()
        pl.DataFrame({
            "GLOBALEVENTID": [2001],
            "MentionTimeDate": [20200101120000],
            "Confidence": [80],
        }).write_parquet(folder / "20200101120000.mentions.parquet")
        gkg_folder = self._write_gkg_v2(tmp_path)

        with pytest.raises(ValueError, match="MentionIdentifier"):
            crossref_events_gkg_v2(
                self._events_df(), str(folder), gkg_folder, GKG_V2_COLUMNS
            )

    def test_no_matching_mentions_returns_empty_dataframe(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)
        events_df = pl.DataFrame({"GlobalEventID": [424242]})
        result = crossref_events_gkg_v2(events_df, mentions_folder, gkg_folder, GKG_V2_COLUMNS)
        assert result.is_empty()

    def test_warns_when_some_events_predate_gdelt_2_coverage(self, tmp_path, caplog):
        # Event 2001 (real match, DATEADDED within coverage) must still
        # join normally alongside event 2002 (pre-coverage, gets warned
        # about): the warning is a diagnostic, not a filter.
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)
        events_df = pl.DataFrame({
            "GlobalEventID": [2001, 2002],
            "DATEADDED": [20200101, 20140101],
        })

        with caplog.at_level("WARNING"):
            result = crossref_events_gkg_v2(
                events_df, mentions_folder, gkg_folder, GKG_V2_COLUMNS
            )

        assert any(
            "1 of 2" in r.message and "GDELT 2.0" in r.message for r in caplog.records
        )
        assert 2001 in set(result["GlobalEventID"])

    def test_warns_when_events_df_is_large(self, tmp_path, caplog, monkeypatch):
        # Same rationale as v1's equivalent test: threshold lowered so
        # this stays fast, real threshold/message covered in isolation
        # by TestWarnIfEventsDfIsLarge.
        monkeypatch.setattr(crossref_module, "_LARGE_EVENTS_JOIN_WARNING_THRESHOLD", 1)
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        with caplog.at_level("WARNING"):
            crossref_events_gkg_v2(self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS)

        assert any("gdeltforge sample" in r.message for r in caplog.records)

    def test_warns_when_either_gkg_directory_is_large(self, tmp_path, caplog, monkeypatch):
        # v2 touches two directories independently (mentions_folder and
        # gkg_v2_folder); both must be checked, not just one, since
        # either can be the one that's actually enormous.
        monkeypatch.setattr(crossref_module, "_LARGE_GKG_DIRECTORY_WARNING_THRESHOLD", 0)
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        with caplog.at_level("WARNING"):
            crossref_events_gkg_v2(self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS)

        labels_warned = {
            "Mentions" for r in caplog.records if "Mentions directory" in r.message
        } | {
            "GKG 2.1" for r in caplog.records if "GKG 2.1 directory" in r.message
        }
        assert labels_warned == {"Mentions", "GKG 2.1"}

    def test_end_date_excludes_the_later_gkg_file(self, tmp_path):
        # Mentions' single file is dated 2020-01-01; _write_gkg_v2 writes
        # an early (2020-01-01) and a late (2020-01-02) file. Bounding
        # both dates to 2020-01-01 keeps Mentions and the early GKG file,
        # excluding the late one: article2's only GKG record lived in
        # the excluded file, so its match disappears entirely, while
        # article1's early record (also its only remaining one) still
        # matches both events that mention it.
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)
        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            start_date=date(2020, 1, 1), end_date=date(2020, 1, 1),
        )

        assert set(result["GKG_GKGRECORDID"]) == {"REC1-early"}
        assert sorted(result["GlobalEventID"]) == [2001, 2002]

    def test_no_date_bounds_matches_the_no_argument_default(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        explicit = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            start_date=None, end_date=None,
        )
        default = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
        )

        pl_testing.assert_frame_equal(explicit, default)

    def test_date_range_excluding_the_mentions_file_raises(self, tmp_path):
        # The bound applies to both folders independently; narrowing past
        # Mentions' single 2020-01-01 file empties hop 1 before GKG is
        # ever reached.
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)
        with pytest.raises(FileNotFoundError, match="No parquet files"):
            crossref_events_gkg_v2(
                self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
                start_date=date(2020, 1, 2),
            )


class TestCrossrefEventsGkgV2ColumnNarrowing:
    """
    Same real gap as TestCrossrefEventsGkgV1ColumnNarrowing, for the
    GKG 2.1 side of the two-hop join: without --columns, the read
    defaults to GKG_V2_COLUMNS in full, which a real output_columns-
    pruned file on disk isn't guaranteed to still have.
    """

    @staticmethod
    def _events_df():
        return pl.DataFrame({"GlobalEventID": [2001]})

    @staticmethod
    def _write_mentions(tmp_path):
        folder = tmp_path / "mentions"
        folder.mkdir()
        pl.DataFrame({
            "GLOBALEVENTID": [2001],
            "MentionIdentifier": ["http://a.com/article1"],
        }).write_parquet(folder / "20200101120000.mentions.parquet")
        return str(folder)

    @staticmethod
    def _write_pruned_gkg_v2(tmp_path):
        # V1THEMES is in GKG_V2_COLUMNS but absent here, the shape
        # filter.output_columns pruning down to just the join key (plus
        # whatever else was kept) produces.
        folder = tmp_path / "gkg_v2_pruned"
        folder.mkdir()
        pl.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://a.com/article1"],
            "GKGRECORDID": ["REC1"],
        }).write_parquet(folder / "20200101000000.gkg.parquet")
        return str(folder)

    def test_default_full_schema_projection_narrows_with_a_warning(self, tmp_path, caplog):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_pruned_gkg_v2(tmp_path)

        with caplog.at_level("WARNING"):
            result = crossref_events_gkg_v2(
                self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS
            )

        assert sorted(result["GlobalEventID"]) == [2001]
        assert "GKG_GKGRECORDID" in result.columns
        assert "GKG_V1THEMES" not in result.columns
        assert any("V1THEMES" in r.message for r in caplog.records)

    def test_join_key_genuinely_missing_raises_clearly(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        folder = tmp_path / "gkg_v2_no_join_key"
        folder.mkdir()
        pl.DataFrame({"GKGRECORDID": ["REC1"]}).write_parquet(folder / "20200101000000.gkg.parquet")

        with pytest.raises(ValueError, match="required column.*V2DOCUMENTIDENTIFIER"):
            crossref_events_gkg_v2(self._events_df(), mentions_folder, folder, GKG_V2_COLUMNS)


class TestCrossrefEventsGkgV2DuplicateHandling:
    """
    Two separate axes of "the same event, the same article, more than
    once": on_duplicate_document (GKG 2.1 carrying more than one record
    for one URL) and dedupe_mentions (Mentions carrying more than one
    raw row for one (event, article) pair, since it records one row per
    sentence that references an event). Confirmed as real, distinct
    phenomena against live GDELT data before adding these parameters;
    see docs/crossref-join-semantics.md.
    """

    @staticmethod
    def _events_df():
        return pl.DataFrame({"GlobalEventID": [3001, 3002]})

    @staticmethod
    def _write_mentions_with_sentence_duplicate(tmp_path):
        # Event 3001 is mentioned twice in the same article (two
        # sentences), with different Confidence, matching the real
        # pattern confirmed against live GDELT data. Event 3002's single
        # mention of a different article must be unaffected.
        folder = tmp_path / "mentions_dup"
        folder.mkdir()
        pl.DataFrame({
            "GLOBALEVENTID": [3001, 3001, 3002],
            "MentionIdentifier": [
                "http://dup.com/article",
                "http://dup.com/article",
                "http://other.com/article",
            ],
            "MentionTimeDate": [20200101120000, 20200101120000, 20200101130000],
            "Confidence": [60, 95, 80],
        }).write_parquet(folder / "20200101120000.mentions.parquet")
        return str(folder)

    @staticmethod
    def _write_gkg_v2_single(tmp_path):
        folder = tmp_path / "gkg_v2_dup"
        folder.mkdir()
        pl.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://dup.com/article", "http://other.com/article"],
            "GKGRECORDID": ["REC-DUP", "REC-OTHER"],
            "V1THEMES": ["THEME_DUP", "THEME_OTHER"],
        }).write_parquet(folder / "20200101120000.gkg.parquet")
        return str(folder)

    def test_sentence_level_duplicate_is_kept_uncollapsed_by_default(self, tmp_path):
        mentions_folder = self._write_mentions_with_sentence_duplicate(tmp_path)
        gkg_folder = self._write_gkg_v2_single(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS
        )

        event_3001_rows = result.filter(pl.col("GlobalEventID") == 3001)
        assert len(event_3001_rows) == 2
        assert "Mention_Count" not in result.columns
        assert sorted(event_3001_rows["Mention_Confidence"]) == [60, 95]

    def test_dedupe_mentions_true_collapses_sentence_level_duplicates(self, tmp_path):
        mentions_folder = self._write_mentions_with_sentence_duplicate(tmp_path)
        gkg_folder = self._write_gkg_v2_single(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            dedupe_mentions=True,
        )

        event_3001_rows = result.filter(pl.col("GlobalEventID") == 3001)
        assert len(event_3001_rows) == 1
        assert "Mention_Count" in result.columns
        assert event_3001_rows.row(0, named=True)["Mention_Count"] == 2

    def test_dedupe_mentions_true_keeps_the_highest_confidence_row(self, tmp_path):
        mentions_folder = self._write_mentions_with_sentence_duplicate(tmp_path)
        gkg_folder = self._write_gkg_v2_single(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            dedupe_mentions=True,
        )

        event_3001_row = result.filter(pl.col("GlobalEventID") == 3001).row(0, named=True)
        assert event_3001_row["Mention_Confidence"] == 95

    def test_dedupe_mentions_true_leaves_unrelated_event_at_count_one(self, tmp_path):
        mentions_folder = self._write_mentions_with_sentence_duplicate(tmp_path)
        gkg_folder = self._write_gkg_v2_single(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            dedupe_mentions=True,
        )

        event_3002_rows = result.filter(pl.col("GlobalEventID") == 3002)
        assert len(event_3002_rows) == 1
        assert event_3002_rows.row(0, named=True)["Mention_Count"] == 1

    def test_dedupe_mentions_false_matches_the_no_argument_default(self, tmp_path):
        # False is now the actual default (see the class above); this
        # pins that an *explicit* False produces the identical result,
        # so the two can't quietly drift apart if the signature's
        # default value ever changes without the behavior following.
        mentions_folder = self._write_mentions_with_sentence_duplicate(tmp_path)
        gkg_folder = self._write_gkg_v2_single(tmp_path)

        explicit = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            dedupe_mentions=False,
        )
        default = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
        )

        pl_testing.assert_frame_equal(explicit, default)

    @staticmethod
    def _write_gkg_v2_reprocessed(tmp_path):
        folder = tmp_path / "gkg_v2_reprocessed"
        folder.mkdir()
        pl.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://dup.com/article"],
            "GKGRECORDID": ["REC-EARLY"],
            "V1THEMES": ["THEME_EARLY"],
        }).write_parquet(folder / "20200101000000.gkg.parquet")
        pl.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://dup.com/article"],
            "GKGRECORDID": ["REC-LATE"],
            "V1THEMES": ["THEME_LATE"],
        }).write_parquet(folder / "20200102000000.gkg.parquet")
        return str(folder)

    @staticmethod
    def _write_mentions_single(tmp_path):
        folder = tmp_path / "mentions_single"
        folder.mkdir()
        pl.DataFrame({
            "GLOBALEVENTID": [3001],
            "MentionIdentifier": ["http://dup.com/article"],
        }).write_parquet(folder / "20200101120000.mentions.parquet")
        return str(folder)

    def test_on_duplicate_document_earliest_keeps_the_first_record(self, tmp_path):
        mentions_folder = self._write_mentions_single(tmp_path)
        gkg_folder = self._write_gkg_v2_reprocessed(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="earliest",
        )

        assert len(result) == 1
        assert result["GKG_GKGRECORDID"][0] == "REC-EARLY"

    def test_on_duplicate_document_all_keeps_every_record(self, tmp_path):
        mentions_folder = self._write_mentions_single(tmp_path)
        gkg_folder = self._write_gkg_v2_reprocessed(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="all",
        )

        assert len(result) == 2
        assert set(result["GKG_GKGRECORDID"]) == {"REC-EARLY", "REC-LATE"}

    def test_on_duplicate_document_all_matches_the_no_argument_default(self, tmp_path):
        # "all" is now the actual default (see the class above); this
        # pins that an *explicit* "all" produces the identical result,
        # so the two can't quietly drift apart if the signature's
        # default value ever changes without the behavior following.
        mentions_folder = self._write_mentions_single(tmp_path)
        gkg_folder = self._write_gkg_v2_reprocessed(tmp_path)

        explicit = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="all",
        )
        default = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
        )

        pl_testing.assert_frame_equal(explicit, default)

    def test_on_duplicate_document_invalid_value_raises(self, tmp_path):
        mentions_folder = self._write_mentions_single(tmp_path)
        gkg_folder = self._write_gkg_v2_reprocessed(tmp_path)

        with pytest.raises(ValueError, match="on_duplicate_document"):
            crossref_events_gkg_v2(
                self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
                on_duplicate_document="nonsense",  # type: ignore[arg-type]  # deliberately invalid, exercising the runtime check
            )


# ------------------------------------------------------------
# crossref_events_gkg_auto: attempts every eligible event against both
# GKG generations, DATEADDED only decides eligibility, not which single
# path is allowed to match
# ------------------------------------------------------------
class TestCrossrefEventsGkgAuto:
    @staticmethod
    def _write_gkg_v1(tmp_path):
        folder = tmp_path / "gkg_v1"
        folder.mkdir()
        pl.DataFrame({
            "Date": [20130401],
            "EventIds": ["1001"],
            "Themes": ["TAX_FNCACT"],
        }).write_parquet(folder / "20130401.gkg.parquet")
        return str(folder)

    @staticmethod
    def _write_mentions(tmp_path):
        folder = tmp_path / "mentions"
        folder.mkdir()
        pl.DataFrame({
            "GLOBALEVENTID": [2001],
            "MentionIdentifier": ["http://a.com/article1"],
        }).write_parquet(folder / "20200101120000.mentions.parquet")
        return str(folder)

    @staticmethod
    def _write_gkg_v2(tmp_path):
        folder = tmp_path / "gkg_v2"
        folder.mkdir()
        pl.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://a.com/article1"],
            "V1THEMES": ["THEME_X"],
        }).write_parquet(folder / "20200101000000.gkg.parquet")
        return str(folder)

    def _paths(self, tmp_path):
        return {
            "gkg_v1_folder": self._write_gkg_v1(tmp_path),
            "gkg_v1_columns": ["Date", "EventIds", "Themes"],
            "mentions_folder": self._write_mentions(tmp_path),
            "gkg_v2_folder": self._write_gkg_v2(tmp_path),
            "gkg_v2_columns": ["V2DOCUMENTIDENTIFIER", "V1THEMES"],
        }

    def test_v1_era_event_with_only_a_v1_match_gets_a_v1_row(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({"GlobalEventID": [1001], "DATEADDED": [20130401]})

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        # Event 1001 is also attempted against GKG 2.1/Mentions (which
        # only ever has event 2001 in this fixture), finds nothing there,
        # and ends up with exactly the one v1 row.
        assert len(result) == 1
        assert result["CrossrefSource"][0] == "v1"
        assert result["GKG_Themes"][0] == "TAX_FNCACT"

    def test_v2_era_event_with_only_a_v2_match_gets_a_v2_row(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({"GlobalEventID": [2001], "DATEADDED": [20200101]})

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        # Event 2001 is also attempted against GKG 1.0 (which only ever
        # has event 1001 in this fixture), finds nothing there, and ends
        # up with exactly the one v2 row.
        assert len(result) == 1
        assert result["CrossrefSource"][0] == "v2"
        assert result["GKG_V1THEMES"][0] == "THEME_X"

    def test_an_old_event_with_a_real_v2_only_match_now_finds_it(self, tmp_path):
        # The actual gap this module used to have, confirmed against real
        # data before this fix: an event with DATEADDED in the GKG 1.0
        # era used to be routed to GKG 1.0 only and never attempted
        # against GKG 2.1/Mentions at all, even when a real v2 match
        # existed (a real 2019-origin event was found referenced by an
        # actual Mentions row dated 2020, well after its own DATEADDED).
        # Event 2001 here has a GKG-1.0-era DATEADDED but its
        # GlobalEventID is the one present in the fixture's Mentions/GKG
        # 2.1 data, not GKG 1.0's.
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({"GlobalEventID": [2001], "DATEADDED": [20130401]})

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        assert len(result) == 1
        assert result["CrossrefSource"][0] == "v2"
        assert result["GKG_V1THEMES"][0] == "THEME_X"

    def test_a_new_event_with_a_real_v1_only_match_now_finds_it(self, tmp_path):
        # Symmetric case: GKG 1.0 remains live and daily-published today,
        # so a recent event isn't guaranteed to be GKG-2.1-only either.
        # Event 1001 here has a GKG-2.1-era DATEADDED but its
        # GlobalEventID is the one present in the fixture's GKG 1.0 data.
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({"GlobalEventID": [1001], "DATEADDED": [20200101]})

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        assert len(result) == 1
        assert result["CrossrefSource"][0] == "v1"
        assert result["GKG_Themes"][0] == "TAX_FNCACT"

    def test_a_single_event_matching_both_paths_contributes_a_row_to_each(self, tmp_path):
        # The other real shape the old routing couldn't produce at all:
        # one event, genuinely covered by both a GKG 1.0 record and a
        # GKG 2.1/Mentions record, must appear twice in the output, once
        # per source, not merged into one row or arbitrarily dropped
        # down to a single path.
        gkg_v1_folder = tmp_path / "gkg_v1"
        gkg_v1_folder.mkdir()
        pl.DataFrame({
            "Date": [20130401], "EventIds": ["1001"], "Themes": ["TAX_FNCACT"],
        }).write_parquet(gkg_v1_folder / "20130401.gkg.parquet")

        mentions_folder = tmp_path / "mentions"
        mentions_folder.mkdir()
        pl.DataFrame({
            "GLOBALEVENTID": [1001],
            "MentionIdentifier": ["http://a.com/article1"],
        }).write_parquet(mentions_folder / "20200101120000.mentions.parquet")

        gkg_v2_folder = tmp_path / "gkg_v2"
        gkg_v2_folder.mkdir()
        pl.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://a.com/article1"], "V1THEMES": ["THEME_X"],
        }).write_parquet(gkg_v2_folder / "20200101000000.gkg.parquet")

        # DATEADDED in the GKG 1.0 era; the same GlobalEventID also has a
        # real Mentions/GKG 2.1 match, exactly the shape of the confirmed
        # real-world case (an old event re-mentioned much later).
        events_df = pl.DataFrame({"GlobalEventID": [1001], "DATEADDED": [20130401]})

        result = crossref_events_gkg_auto(
            events_df, str(gkg_v1_folder), ["Date", "EventIds", "Themes"],
            str(mentions_folder), str(gkg_v2_folder), ["V2DOCUMENTIDENTIFIER", "V1THEMES"],
        )

        assert len(result) == 2
        assert set(result["CrossrefSource"]) == {"v1", "v2"}
        assert set(result["GlobalEventID"]) == {1001}
        v1_row = result.filter(pl.col("CrossrefSource") == "v1").row(0, named=True)
        v2_row = result.filter(pl.col("CrossrefSource") == "v2").row(0, named=True)
        assert v1_row["GKG_Themes"] == "TAX_FNCACT"
        assert v2_row["GKG_V1THEMES"] == "THEME_X"
        # Each row still carries null for the other schema's columns.
        assert v1_row["GKG_V1THEMES"] is None
        assert v2_row["GKG_Themes"] is None

    def test_mixed_sample_gets_both_sources_in_one_result(self, tmp_path):
        # Two different events, each with a real match in only one path
        # (1001 in GKG 1.0 only, 2001 in GKG 2.1/Mentions only): both
        # still end up in the same result, one row each. See
        # test_a_single_event_matching_both_paths_contributes_a_row_to_each
        # for the case of one event matching both.
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({
            "GlobalEventID": [1001, 2001],
            "DATEADDED": [20130401, 20200101],
        })

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        assert len(result) == 2
        assert set(result["CrossrefSource"]) == {"v1", "v2"}
        # Neither schema's GKG-side columns overlap: a v1 row must carry
        # null for the v2-only column and vice versa, not raise or drop it.
        v1_row = result.filter(pl.col("CrossrefSource") == "v1").row(0, named=True)
        v2_row = result.filter(pl.col("CrossrefSource") == "v2").row(0, named=True)
        assert v1_row["GKG_V1THEMES"] is None
        assert v2_row["GKG_Themes"] is None

    def test_events_before_gkg_v1_coverage_are_skipped_and_warned(self, tmp_path, caplog):
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({"GlobalEventID": [999], "DATEADDED": [20100101]})

        with caplog.at_level("WARNING"):
            result = crossref_events_gkg_auto(
                events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
                paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            )

        assert result.is_empty()
        assert any(
            "1 of 1" in r.message and "20130401" in r.message for r in caplog.records
        )

    def test_partial_pre_coverage_still_routes_the_valid_events(self, tmp_path, caplog):
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({
            "GlobalEventID": [999, 1001],
            "DATEADDED": [20100101, 20130401],
        })

        with caplog.at_level("WARNING"):
            result = crossref_events_gkg_auto(
                events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
                paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            )

        assert len(result) == 1
        assert result["GlobalEventID"][0] == 1001
        assert any("1 of 2" in r.message for r in caplog.records)

    def test_start_date_is_forwarded_to_the_v1_path(self, tmp_path):
        # gkg_v1_folder gets a second, earlier file that also names event
        # 1001, distinguished by its own Themes value; mentions/gkg_v2
        # keep _paths()'s single 2020-01-01 fixture untouched (2020-01-01
        # still satisfies file_end >= start_date with no upper bound
        # set), isolating this to proving start_date actually reaches
        # crossref_events_gkg_v1 through the auto path, not just
        # crossref_events_gkg_v2.
        paths = self._paths(tmp_path)
        pl.DataFrame({
            "Date": [20130101], "EventIds": ["1001"], "Themes": ["EXCLUDED_BY_START_DATE"],
        }).write_parquet(Path(paths["gkg_v1_folder"]) / "20130101.gkg.parquet")
        events_df = pl.DataFrame({"GlobalEventID": [1001], "DATEADDED": [20130401]})

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            start_date=date(2013, 4, 1),
        )

        v1_rows = result.filter(pl.col("CrossrefSource") == "v1")
        assert "EXCLUDED_BY_START_DATE" not in set(v1_rows["GKG_Themes"])
        assert set(v1_rows["GKG_Themes"]) == {"TAX_FNCACT"}

    def test_missing_dateadded_raises(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({"GlobalEventID": [1001]})

        with pytest.raises(ValueError, match="DATEADDED"):
            crossref_events_gkg_auto(
                events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
                paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            )

    def test_missing_global_event_id_raises(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({"DATEADDED": [20130401]})

        with pytest.raises(ValueError, match="GlobalEventID"):
            crossref_events_gkg_auto(
                events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
                paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            )

    def test_no_matches_in_either_path_returns_empty_dataframe(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({
            "GlobalEventID": [424242, 434343],
            "DATEADDED": [20130401, 20200101],
        })

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        assert result.is_empty()

    def test_v1_columns_and_v2_columns_restrict_each_path_independently(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({
            "GlobalEventID": [1001, 2001],
            "DATEADDED": [20130401, 20200101],
        })

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            v1_columns={"EventIds"}, v2_columns={"V2DOCUMENTIDENTIFIER"},
        )

        assert "GKG_Themes" not in result.columns
        assert "GKG_V1THEMES" not in result.columns

    def test_warns_twice_for_a_large_events_df_once_per_generation_attempted(
        self, tmp_path, caplog, monkeypatch
    ):
        # auto calls crossref_events_gkg_v1 and crossref_events_gkg_v2
        # internally, and each already warns independently about
        # pre-coverage events (see the DATEADDED coverage tests above);
        # the large-events warning follows that same established
        # pattern rather than trying to fire it once for "the auto
        # call" as a whole, since auto genuinely runs two separate
        # expensive joins, not one.
        monkeypatch.setattr(crossref_module, "_LARGE_EVENTS_JOIN_WARNING_THRESHOLD", 1)
        paths = self._paths(tmp_path)
        events_df = pl.DataFrame({
            "GlobalEventID": [1001, 2001],
            "DATEADDED": [20130401, 20200101],
        })

        with caplog.at_level("WARNING"):
            crossref_events_gkg_auto(
                events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
                paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            )

        large_join_warnings = [r for r in caplog.records if "gdeltforge sample" in r.message]
        assert len(large_join_warnings) == 2
