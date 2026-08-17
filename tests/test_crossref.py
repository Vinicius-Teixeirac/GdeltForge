import logging
from pathlib import Path

import pandas as pd
import pytest

from gdeltforge.crossref.crossref import (
    GKG_V1_COVERAGE_START,
    GKG_V2_COVERAGE_START,
    OPTIONAL_MENTIONS_PAYLOAD_COLUMNS,
    REQUIRED_JOIN_COLUMNS,
    crossref_events_gkg_auto,
    crossref_events_gkg_v1,
    crossref_events_gkg_v2,
    warn_if_events_predate_gkg_coverage,
    warn_if_output_columns_drops_join_key,
)

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
        events_df = pd.DataFrame({"DATEADDED": [20200101, 20200102]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        assert caplog.records == []

    def test_warns_when_all_events_predate_coverage(self, caplog):
        events_df = pd.DataFrame({"DATEADDED": [20100101, 20120101]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        assert any(
            "All 2" in r.message and "GKG 1.0" in r.message and "20130401" in r.message
            for r in caplog.records
        )

    def test_warns_with_a_partial_count_when_only_some_events_predate_coverage(self, caplog):
        events_df = pd.DataFrame({"DATEADDED": [20100101, 20200101, 20200102]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        assert any("1 of 3" in r.message for r in caplog.records)

    def test_no_warning_when_dateadded_column_is_absent(self, caplog):
        # A sample built with --columns that excluded DATEADDED: this is
        # a diagnostic on top of the join, not something the join itself
        # depends on, so it must degrade silently, not error.
        events_df = pd.DataFrame({"GlobalEventID": [1, 2]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        assert caplog.records == []

    def test_no_warning_on_empty_dateadded(self, caplog):
        events_df = pd.DataFrame({"DATEADDED": pd.Series([], dtype="float64")})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        assert caplog.records == []

    def test_null_dateadded_values_are_excluded_from_the_count(self, caplog):
        events_df = pd.DataFrame({"DATEADDED": [20100101, None, 20200101]})
        with caplog.at_level(logging.WARNING):
            warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
        # 1 real pre-coverage row out of 2 non-null rows, not 3.
        assert any("1 of 2" in r.message for r in caplog.records)


# ------------------------------------------------------------
# crossref_events_gkg_v1: direct join on EventIds
# ------------------------------------------------------------
class TestCrossrefEventsGkgV1:
    @staticmethod
    def _events_df():
        return pd.DataFrame({
            "GlobalEventID": [1001, 1002, 1003],
            "Actor1Name": ["Alice", "Bob", "Carol"],
            "NumArticles": [5, 3, 7],
        })

    @staticmethod
    def _write_gkg_v1(tmp_path):
        folder = tmp_path / "gkg_v1"
        folder.mkdir()
        pd.DataFrame({
            "Date": [20130401, 20130401],
            "EventIds": ["1001,1002", "9999"],
            "NumArticles": [10, 2],
            "Themes": ["TAX_FNCACT", "UNRELATED"],
        }).to_parquet(folder / "20130401.gkg.parquet")
        pd.DataFrame({
            "Date": [20130402, 20130402],
            "EventIds": ["1001", None],
            "NumArticles": [4, 1],
            "Themes": ["ECON_STOCKMARKET", "EMPTY_TEST"],
        }).to_parquet(folder / "20130402.gkg.parquet")
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
        shared = result[result["GKG_EventIds"] == "1001,1002"]
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

        row = result[
            (result["GlobalEventID"] == 1001) & (result["GKG_EventIds"] == "1001,1002")
        ].iloc[0]
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
        bad_events = self._events_df().drop(columns=["GlobalEventID"])
        with pytest.raises(ValueError, match="GlobalEventID"):
            crossref_events_gkg_v1(bad_events, folder, GKG_V1_COLUMNS)

    def test_missing_eventids_in_schema_raises(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        with pytest.raises(ValueError, match="EventIds"):
            crossref_events_gkg_v1(self._events_df(), folder, ["Date", "NumArticles"])

    def test_no_matches_returns_empty_dataframe(self, tmp_path):
        folder = self._write_gkg_v1(tmp_path)
        events_df = pd.DataFrame({"GlobalEventID": [424242], "NumArticles": [1]})
        result = crossref_events_gkg_v1(events_df, folder, GKG_V1_COLUMNS)
        assert result.empty

    def test_warns_when_some_events_predate_gkg_v1_coverage(self, tmp_path, caplog):
        # Event 1001 (real match, DATEADDED within coverage) must still
        # join normally alongside event 1002 (pre-coverage, gets warned
        # about): the warning is a diagnostic, not a filter.
        folder = self._write_gkg_v1(tmp_path)
        events_df = pd.DataFrame({
            "GlobalEventID": [1001, 1002],
            "DATEADDED": [20130401, 20120101],
        })

        with caplog.at_level("WARNING"):
            result = crossref_events_gkg_v1(events_df, folder, GKG_V1_COLUMNS)

        assert any(
            "1 of 2" in r.message and "GKG 1.0" in r.message for r in caplog.records
        )
        assert 1001 in set(result["GlobalEventID"])


# ------------------------------------------------------------
# crossref_events_gkg_v2: two-hop join through Mentions
# ------------------------------------------------------------
class TestCrossrefEventsGkgV2:
    @staticmethod
    def _events_df():
        return pd.DataFrame({
            "GlobalEventID": [2001, 2002, 2003],
            "Actor1Name": ["Dave", "Erin", "Frank"],
        })

    @staticmethod
    def _write_mentions(tmp_path):
        folder = tmp_path / "mentions"
        folder.mkdir()
        pd.DataFrame({
            "GLOBALEVENTID": [2001, 2001, 2002, 9999],
            "MentionIdentifier": [
                "http://a.com/article1",
                "http://a.com/article2",
                "http://a.com/article1",   # same article also covers event 2001
                "http://a.com/unrelated",
            ],
            "MentionTimeDate": [20200101120000, 20200102120000, 20200101130000, 20200101000000],
            "Confidence": [80, 90, 70, 50],
        }).to_parquet(folder / "20200101120000.mentions.parquet")
        return str(folder)

    @staticmethod
    def _write_gkg_v2(tmp_path):
        folder = tmp_path / "gkg_v2"
        folder.mkdir()
        pd.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://a.com/article1", "http://c.com/unrelated"],
            "GKGRECORDID": ["REC1-early", "REC-unrelated"],
            "V1THEMES": ["THEME_OLD", "THEME_X"],
        }).to_parquet(folder / "20200101000000.gkg.parquet")
        pd.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://a.com/article1", "http://a.com/article2"],
            "GKGRECORDID": ["REC1-late", "REC2"],
            "V1THEMES": ["THEME_NEW", "THEME_Y"],
        }).to_parquet(folder / "20200102000000.gkg.parquet")
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
        article1_rows = result[result["GKG_V2DOCUMENTIDENTIFIER"] == "http://a.com/article1"]
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
        article1_rows = result[result["GKG_V2DOCUMENTIDENTIFIER"] == "http://a.com/article1"]
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
        article1_rows = result[result["GKG_V2DOCUMENTIDENTIFIER"] == "http://a.com/article1"]
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
        pd.DataFrame({
            "GLOBALEVENTID": [2001, 2002],
            "MentionIdentifier": ["http://a.com/article1", "http://a.com/article1"],
            "MentionTimeDate": [20200101120000, 20200101130000],
        }).to_parquet(folder / "20200101120000.mentions.parquet")
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
        pd.DataFrame({
            "GLOBALEVENTID": [2001],
            "MentionIdentifier": ["http://a.com/article1"],
        }).to_parquet(folder / "20200101120000.mentions.parquet")
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), str(folder), gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="latest",
        )

        assert len(result) == 1
        assert result["GlobalEventID"].iloc[0] == 2001
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
        bad_events = self._events_df().drop(columns=["GlobalEventID"])
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
        pd.DataFrame({
            "MentionIdentifier": ["http://a.com/article1"],
            "MentionTimeDate": [20200101120000],
            "Confidence": [80],
        }).to_parquet(folder / "20200101120000.mentions.parquet")
        gkg_folder = self._write_gkg_v2(tmp_path)

        with pytest.raises(ValueError, match="GLOBALEVENTID"):
            crossref_events_gkg_v2(
                self._events_df(), str(folder), gkg_folder, GKG_V2_COLUMNS
            )

    def test_missing_mention_identifier_in_mentions_schema_raises_cleanly(self, tmp_path):
        folder = tmp_path / "mentions_missing_identifier"
        folder.mkdir()
        pd.DataFrame({
            "GLOBALEVENTID": [2001],
            "MentionTimeDate": [20200101120000],
            "Confidence": [80],
        }).to_parquet(folder / "20200101120000.mentions.parquet")
        gkg_folder = self._write_gkg_v2(tmp_path)

        with pytest.raises(ValueError, match="MentionIdentifier"):
            crossref_events_gkg_v2(
                self._events_df(), str(folder), gkg_folder, GKG_V2_COLUMNS
            )

    def test_no_matching_mentions_returns_empty_dataframe(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)
        events_df = pd.DataFrame({"GlobalEventID": [424242]})
        result = crossref_events_gkg_v2(events_df, mentions_folder, gkg_folder, GKG_V2_COLUMNS)
        assert result.empty

    def test_warns_when_some_events_predate_gdelt_2_coverage(self, tmp_path, caplog):
        # Event 2001 (real match, DATEADDED within coverage) must still
        # join normally alongside event 2002 (pre-coverage, gets warned
        # about): the warning is a diagnostic, not a filter.
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)
        events_df = pd.DataFrame({
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
        return pd.DataFrame({"GlobalEventID": [3001, 3002]})

    @staticmethod
    def _write_mentions_with_sentence_duplicate(tmp_path):
        # Event 3001 is mentioned twice in the same article (two
        # sentences), with different Confidence, matching the real
        # pattern confirmed against live GDELT data. Event 3002's single
        # mention of a different article must be unaffected.
        folder = tmp_path / "mentions_dup"
        folder.mkdir()
        pd.DataFrame({
            "GLOBALEVENTID": [3001, 3001, 3002],
            "MentionIdentifier": [
                "http://dup.com/article",
                "http://dup.com/article",
                "http://other.com/article",
            ],
            "MentionTimeDate": [20200101120000, 20200101120000, 20200101130000],
            "Confidence": [60, 95, 80],
        }).to_parquet(folder / "20200101120000.mentions.parquet")
        return str(folder)

    @staticmethod
    def _write_gkg_v2_single(tmp_path):
        folder = tmp_path / "gkg_v2_dup"
        folder.mkdir()
        pd.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://dup.com/article", "http://other.com/article"],
            "GKGRECORDID": ["REC-DUP", "REC-OTHER"],
            "V1THEMES": ["THEME_DUP", "THEME_OTHER"],
        }).to_parquet(folder / "20200101120000.gkg.parquet")
        return str(folder)

    def test_sentence_level_duplicate_is_kept_uncollapsed_by_default(self, tmp_path):
        mentions_folder = self._write_mentions_with_sentence_duplicate(tmp_path)
        gkg_folder = self._write_gkg_v2_single(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS
        )

        event_3001_rows = result[result["GlobalEventID"] == 3001]
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

        event_3001_rows = result[result["GlobalEventID"] == 3001]
        assert len(event_3001_rows) == 1
        assert "Mention_Count" in result.columns
        assert event_3001_rows["Mention_Count"].iloc[0] == 2

    def test_dedupe_mentions_true_keeps_the_highest_confidence_row(self, tmp_path):
        mentions_folder = self._write_mentions_with_sentence_duplicate(tmp_path)
        gkg_folder = self._write_gkg_v2_single(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            dedupe_mentions=True,
        )

        event_3001_row = result[result["GlobalEventID"] == 3001].iloc[0]
        assert event_3001_row["Mention_Confidence"] == 95

    def test_dedupe_mentions_true_leaves_unrelated_event_at_count_one(self, tmp_path):
        mentions_folder = self._write_mentions_with_sentence_duplicate(tmp_path)
        gkg_folder = self._write_gkg_v2_single(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            dedupe_mentions=True,
        )

        event_3002_rows = result[result["GlobalEventID"] == 3002]
        assert len(event_3002_rows) == 1
        assert event_3002_rows["Mention_Count"].iloc[0] == 1

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

        pd.testing.assert_frame_equal(
            explicit.reset_index(drop=True), default.reset_index(drop=True)
        )

    @staticmethod
    def _write_gkg_v2_reprocessed(tmp_path):
        folder = tmp_path / "gkg_v2_reprocessed"
        folder.mkdir()
        pd.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://dup.com/article"],
            "GKGRECORDID": ["REC-EARLY"],
            "V1THEMES": ["THEME_EARLY"],
        }).to_parquet(folder / "20200101000000.gkg.parquet")
        pd.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://dup.com/article"],
            "GKGRECORDID": ["REC-LATE"],
            "V1THEMES": ["THEME_LATE"],
        }).to_parquet(folder / "20200102000000.gkg.parquet")
        return str(folder)

    @staticmethod
    def _write_mentions_single(tmp_path):
        folder = tmp_path / "mentions_single"
        folder.mkdir()
        pd.DataFrame({
            "GLOBALEVENTID": [3001],
            "MentionIdentifier": ["http://dup.com/article"],
        }).to_parquet(folder / "20200101120000.mentions.parquet")
        return str(folder)

    def test_on_duplicate_document_earliest_keeps_the_first_record(self, tmp_path):
        mentions_folder = self._write_mentions_single(tmp_path)
        gkg_folder = self._write_gkg_v2_reprocessed(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
            on_duplicate_document="earliest",
        )

        assert len(result) == 1
        assert result["GKG_GKGRECORDID"].iloc[0] == "REC-EARLY"

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

        pd.testing.assert_frame_equal(
            explicit.reset_index(drop=True), default.reset_index(drop=True)
        )

    def test_on_duplicate_document_invalid_value_raises(self, tmp_path):
        mentions_folder = self._write_mentions_single(tmp_path)
        gkg_folder = self._write_gkg_v2_reprocessed(tmp_path)

        with pytest.raises(ValueError, match="on_duplicate_document"):
            crossref_events_gkg_v2(
                self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS,
                on_duplicate_document="nonsense",
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
        pd.DataFrame({
            "Date": [20130401],
            "EventIds": ["1001"],
            "Themes": ["TAX_FNCACT"],
        }).to_parquet(folder / "20130401.gkg.parquet")
        return str(folder)

    @staticmethod
    def _write_mentions(tmp_path):
        folder = tmp_path / "mentions"
        folder.mkdir()
        pd.DataFrame({
            "GLOBALEVENTID": [2001],
            "MentionIdentifier": ["http://a.com/article1"],
        }).to_parquet(folder / "20200101120000.mentions.parquet")
        return str(folder)

    @staticmethod
    def _write_gkg_v2(tmp_path):
        folder = tmp_path / "gkg_v2"
        folder.mkdir()
        pd.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://a.com/article1"],
            "V1THEMES": ["THEME_X"],
        }).to_parquet(folder / "20200101000000.gkg.parquet")
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
        events_df = pd.DataFrame({"GlobalEventID": [1001], "DATEADDED": [20130401]})

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        # Event 1001 is also attempted against GKG 2.1/Mentions (which
        # only ever has event 2001 in this fixture), finds nothing there,
        # and ends up with exactly the one v1 row.
        assert len(result) == 1
        assert result["CrossrefSource"].iloc[0] == "v1"
        assert result["GKG_Themes"].iloc[0] == "TAX_FNCACT"

    def test_v2_era_event_with_only_a_v2_match_gets_a_v2_row(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pd.DataFrame({"GlobalEventID": [2001], "DATEADDED": [20200101]})

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        # Event 2001 is also attempted against GKG 1.0 (which only ever
        # has event 1001 in this fixture), finds nothing there, and ends
        # up with exactly the one v2 row.
        assert len(result) == 1
        assert result["CrossrefSource"].iloc[0] == "v2"
        assert result["GKG_V1THEMES"].iloc[0] == "THEME_X"

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
        events_df = pd.DataFrame({"GlobalEventID": [2001], "DATEADDED": [20130401]})

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        assert len(result) == 1
        assert result["CrossrefSource"].iloc[0] == "v2"
        assert result["GKG_V1THEMES"].iloc[0] == "THEME_X"

    def test_a_new_event_with_a_real_v1_only_match_now_finds_it(self, tmp_path):
        # Symmetric case: GKG 1.0 remains live and daily-published today,
        # so a recent event isn't guaranteed to be GKG-2.1-only either.
        # Event 1001 here has a GKG-2.1-era DATEADDED but its
        # GlobalEventID is the one present in the fixture's GKG 1.0 data.
        paths = self._paths(tmp_path)
        events_df = pd.DataFrame({"GlobalEventID": [1001], "DATEADDED": [20200101]})

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        assert len(result) == 1
        assert result["CrossrefSource"].iloc[0] == "v1"
        assert result["GKG_Themes"].iloc[0] == "TAX_FNCACT"

    def test_a_single_event_matching_both_paths_contributes_a_row_to_each(self, tmp_path):
        # The other real shape the old routing couldn't produce at all:
        # one event, genuinely covered by both a GKG 1.0 record and a
        # GKG 2.1/Mentions record, must appear twice in the output, once
        # per source, not merged into one row or arbitrarily dropped
        # down to a single path.
        gkg_v1_folder = tmp_path / "gkg_v1"
        gkg_v1_folder.mkdir()
        pd.DataFrame({
            "Date": [20130401], "EventIds": ["1001"], "Themes": ["TAX_FNCACT"],
        }).to_parquet(gkg_v1_folder / "20130401.gkg.parquet")

        mentions_folder = tmp_path / "mentions"
        mentions_folder.mkdir()
        pd.DataFrame({
            "GLOBALEVENTID": [1001],
            "MentionIdentifier": ["http://a.com/article1"],
        }).to_parquet(mentions_folder / "20200101120000.mentions.parquet")

        gkg_v2_folder = tmp_path / "gkg_v2"
        gkg_v2_folder.mkdir()
        pd.DataFrame({
            "V2DOCUMENTIDENTIFIER": ["http://a.com/article1"], "V1THEMES": ["THEME_X"],
        }).to_parquet(gkg_v2_folder / "20200101000000.gkg.parquet")

        # DATEADDED in the GKG 1.0 era; the same GlobalEventID also has a
        # real Mentions/GKG 2.1 match, exactly the shape of the confirmed
        # real-world case (an old event re-mentioned much later).
        events_df = pd.DataFrame({"GlobalEventID": [1001], "DATEADDED": [20130401]})

        result = crossref_events_gkg_auto(
            events_df, str(gkg_v1_folder), ["Date", "EventIds", "Themes"],
            str(mentions_folder), str(gkg_v2_folder), ["V2DOCUMENTIDENTIFIER", "V1THEMES"],
        )

        assert len(result) == 2
        assert set(result["CrossrefSource"]) == {"v1", "v2"}
        assert set(result["GlobalEventID"]) == {1001}
        v1_row = result[result["CrossrefSource"] == "v1"].iloc[0]
        v2_row = result[result["CrossrefSource"] == "v2"].iloc[0]
        assert v1_row["GKG_Themes"] == "TAX_FNCACT"
        assert v2_row["GKG_V1THEMES"] == "THEME_X"
        # Each row still carries NaN for the other schema's columns.
        assert pd.isna(v1_row["GKG_V1THEMES"])
        assert pd.isna(v2_row["GKG_Themes"])

    def test_mixed_sample_gets_both_sources_in_one_result(self, tmp_path):
        # Two different events, each with a real match in only one path
        # (1001 in GKG 1.0 only, 2001 in GKG 2.1/Mentions only): both
        # still end up in the same result, one row each. See
        # test_a_single_event_matching_both_paths_contributes_a_row_to_each
        # for the case of one event matching both.
        paths = self._paths(tmp_path)
        events_df = pd.DataFrame({
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
        # NaN for the v2-only column and vice versa, not raise or drop it.
        v1_row = result[result["CrossrefSource"] == "v1"].iloc[0]
        v2_row = result[result["CrossrefSource"] == "v2"].iloc[0]
        assert pd.isna(v1_row["GKG_V1THEMES"])
        assert pd.isna(v2_row["GKG_Themes"])

    def test_events_before_gkg_v1_coverage_are_skipped_and_warned(self, tmp_path, caplog):
        paths = self._paths(tmp_path)
        events_df = pd.DataFrame({"GlobalEventID": [999], "DATEADDED": [20100101]})

        with caplog.at_level("WARNING"):
            result = crossref_events_gkg_auto(
                events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
                paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            )

        assert result.empty
        assert any(
            "1 of 1" in r.message and "20130401" in r.message for r in caplog.records
        )

    def test_partial_pre_coverage_still_routes_the_valid_events(self, tmp_path, caplog):
        paths = self._paths(tmp_path)
        events_df = pd.DataFrame({
            "GlobalEventID": [999, 1001],
            "DATEADDED": [20100101, 20130401],
        })

        with caplog.at_level("WARNING"):
            result = crossref_events_gkg_auto(
                events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
                paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            )

        assert len(result) == 1
        assert result["GlobalEventID"].iloc[0] == 1001
        assert any("1 of 2" in r.message for r in caplog.records)

    def test_missing_dateadded_raises(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pd.DataFrame({"GlobalEventID": [1001]})

        with pytest.raises(ValueError, match="DATEADDED"):
            crossref_events_gkg_auto(
                events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
                paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            )

    def test_missing_global_event_id_raises(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pd.DataFrame({"DATEADDED": [20130401]})

        with pytest.raises(ValueError, match="GlobalEventID"):
            crossref_events_gkg_auto(
                events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
                paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
            )

    def test_no_matches_in_either_path_returns_empty_dataframe(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pd.DataFrame({
            "GlobalEventID": [424242, 434343],
            "DATEADDED": [20130401, 20200101],
        })

        result = crossref_events_gkg_auto(
            events_df, paths["gkg_v1_folder"], paths["gkg_v1_columns"],
            paths["mentions_folder"], paths["gkg_v2_folder"], paths["gkg_v2_columns"],
        )

        assert result.empty

    def test_v1_columns_and_v2_columns_restrict_each_path_independently(self, tmp_path):
        paths = self._paths(tmp_path)
        events_df = pd.DataFrame({
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
