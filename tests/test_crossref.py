import logging

import pandas as pd
import pytest

from gdeltforge.crossref.crossref import (
    REQUIRED_JOIN_COLUMNS,
    crossref_events_gkg_v1,
    crossref_events_gkg_v2,
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
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS
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
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS
        )
        article1_rows = result[result["GKG_V2DOCUMENTIDENTIFIER"] == "http://a.com/article1"]
        assert sorted(article1_rows["GlobalEventID"]) == [2001, 2002]

    def test_reprocessed_article_deduped_keeping_the_latest_batch(self, tmp_path):
        mentions_folder = self._write_mentions(tmp_path)
        gkg_folder = self._write_gkg_v2(tmp_path)

        result = crossref_events_gkg_v2(
            self._events_df(), mentions_folder, gkg_folder, GKG_V2_COLUMNS
        )
        article1_rows = result[result["GKG_V2DOCUMENTIDENTIFIER"] == "http://a.com/article1"]
        # Both the 2001 and 2002 rows for article1 must carry the SAME,
        # latest GKG record (REC1-late), not the earlier REC1-early, and
        # article1 must never contribute two GKG-side variants at once.
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
