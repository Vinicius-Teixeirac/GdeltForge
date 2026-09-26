from datetime import date

import polars as pl
import pytest

from gdeltforge.cleaning.steps import (
    STEP_ORDER,
    Date1920Repair,
    DeriveColumns,
    EventMarkers,
    FileContext,
    NarrowFloat32,
    NormalizeStrings,
    ProjectColumns,
    RequireColumns,
    ordered,
)

CTX = FileContext("f")


class TestOrder:
    def test_steps_run_in_the_fixed_order_whatever_order_they_are_built_in(self):
        built = [NarrowFloat32(("a",)), RequireColumns(("a",)), ProjectColumns(("a",))]
        assert [s.name for s in ordered(built)] == ["require", "project", "narrow"]

    def test_every_step_name_has_a_place_in_the_order(self):
        for step in (RequireColumns(), ProjectColumns(), NarrowFloat32()):
            assert step.name in STEP_ORDER


class TestLossyDeclarations:
    @pytest.mark.parametrize("step", [RequireColumns(("a",)), ProjectColumns(("a",)),
                                      NarrowFloat32(("a",))])
    def test_the_original_operations_are_lossy(self, step):
        # Each discards something the output can't recover: rows, columns,
        # float precision.
        assert step.lossy is True


class TestRequireColumns:
    def test_drops_rows_with_a_null_in_any_required_column(self):
        lf = pl.LazyFrame({"a": [1, None, 3], "b": [1, 2, None]})
        out = RequireColumns(("a", "b")).apply(lf, CTX).collect()
        assert out["a"].to_list() == [1]

    def test_empty_list_is_a_no_op(self):
        lf = pl.LazyFrame({"a": [None]})
        assert RequireColumns(()).apply(lf, CTX).collect().height == 1

    def test_no_configured_column_present_raises(self):
        with pytest.raises(ValueError, match="nothing to filter on"):
            RequireColumns(("zz",)).apply(pl.LazyFrame({"a": [1]}), CTX)


class TestNarrowFloat32:
    def test_only_float_columns_present_after_earlier_steps_are_cast(self):
        lf = pl.LazyFrame({"x": [1.5], "n": [1]})
        out = NarrowFloat32(("x", "n", "gone")).apply(lf, CTX).collect()
        assert out.schema["x"] == pl.Float32
        assert out.schema["n"] == pl.Int64


def _events(days, added, codes=None):
    n = len(days)
    return pl.LazyFrame({
        "Day": days,
        "MonthYear": [d // 100 for d in days],
        "Year": [d // 10000 for d in days],
        "FractionDate": [d // 10000 + 0.0027 for d in days],
        "DATEADDED": added,
        "EventCode": codes or ["010"] * n,
        "EventBaseCode": codes or ["010"] * n,
        "EventRootCode": [c[:2] if c not in ("X",) else c for c in (codes or ["010"] * n)],
    })


class TestDate1920Repair:
    def test_repairs_only_pre_1979_rows_added_in_the_window(self):
        lf = _events(
            [19200101, 19200105, 20200105, 19200101],
            [20191231, 20200105, 20200105, 20200106],
        )
        out = Date1920Repair().apply(lf, CTX).collect()
        # Window edges: added 2019-12-31 and 2020-01-05 are repaired; a real
        # 2020-01-05 date is untouched; a row added 2020-01-06 is outside.
        assert out["Day"].to_list() == [20200101, 20200105, 20200105, 19200101]
        assert out["MonthYear"].to_list() == [202001, 202001, 202001, 192001]
        assert out["Year"].to_list() == [2020, 2020, 2020, 1920]
        assert out["FractionDate"].to_list()[:2] == pytest.approx([2020.0027, 2020.0027])

    def test_keeps_gdelts_values_on_repaired_rows_only(self):
        lf = _events([19200101, 20200105], [20200101, 20200105])
        out = Date1920Repair(keep_original=True).apply(lf, CTX).collect()
        assert out["Day_original"].to_list() == [19200101, None]
        assert out["Year_original"].to_list() == [1920, None]

    def test_reads_the_15_minute_feeds_14_digit_dateadded(self):
        lf = _events([19200102], [20200102120000])
        assert Date1920Repair().apply(lf, CTX).collect()["Day"].to_list() == [20200102]

    def test_a_file_whose_period_misses_the_window_is_left_alone(self):
        lf = _events([19200101], [20200101])
        ctx = FileContext("20210101.export.parquet", date(2021, 1, 1), date(2021, 1, 1))
        out = Date1920Repair().apply(lf, ctx).collect()
        assert out["Day"].to_list() == [19200101]
        assert "Day_original" not in out.columns

    def test_counts_the_rows_it_repairs(self):
        lf = _events([19200101, 20200105], [20200101, 20200105])
        exprs = Date1920Repair().counts(lf, CTX)
        assert lf.select(**exprs).collect().row(0, named=True) == {"errata.date_1920": 1}

    def test_lossy_only_without_the_originals(self):
        assert Date1920Repair(keep_original=True).lossy is False
        assert Date1920Repair(keep_original=False).lossy is True


class TestEventMarkers:
    def _lf(self):
        return _events([20200101] * 4, [20200101] * 4, codes=["010", "---", "X", "190"])

    def test_keep_leaves_every_row_and_counts_the_markers(self):
        step = EventMarkers("keep")
        lf = self._lf()
        assert step.apply(lf, CTX).collect().height == 4
        assert lf.select(**step.counts(lf, CTX)).collect().row(0, named=True) == {
            "errata.event_markers_keep": 2
        }
        assert step.lossy is False

    def test_drop_removes_them_and_is_lossy(self):
        step = EventMarkers("drop")
        out = step.apply(self._lf(), CTX).collect()
        assert out["EventCode"].to_list() == ["010", "190"]
        assert step.lossy is True


class TestNormalizeStrings:
    def _lf(self):
        return pl.LazyFrame({"Actor2Code": [" USA", " ", "GOV", None], "n": [1, 2, 3, 4]})

    def test_trim_strips_padding(self):
        out = NormalizeStrings(trim=True).apply(self._lf(), CTX).collect()
        assert out["Actor2Code"].to_list() == ["USA", "", "GOV", None]

    def test_blank_to_null_nulls_whitespace_only_values(self):
        out = NormalizeStrings(blank_to_null=True).apply(self._lf(), CTX).collect()
        assert out["Actor2Code"].to_list() == [" USA", None, "GOV", None]

    def test_both_together(self):
        out = NormalizeStrings(trim=True, blank_to_null=True).apply(self._lf(), CTX).collect()
        assert out["Actor2Code"].to_list() == ["USA", None, "GOV", None]

    def test_counts_what_it_changes(self):
        lf = self._lf()
        exprs = NormalizeStrings(trim=True, blank_to_null=True).counts(lf, CTX)
        assert lf.select(**exprs).collect().row(0, named=True) == {
            "normalize.trimmed": 1, "normalize.blank_to_null": 1,
        }

    def test_is_lossy(self):
        assert NormalizeStrings(trim=True).lossy is True

    def test_runs_before_the_null_check(self):
        # The reason for the fixed order: a blank becomes null first, so the
        # null check drops its row.
        steps = ordered([RequireColumns(("Actor2Code",)), NormalizeStrings(blank_to_null=True)])
        lf = self._lf()
        for step in steps:
            lf = step.apply(lf, CTX)
        assert lf.collect()["n"].to_list() == [1, 3]


class TestDeriveColumns:
    def test_event_date_is_a_real_date_from_day(self):
        lf = pl.LazyFrame({"Day": [20200105, 19790101]})
        out = DeriveColumns(event_date=True).apply(lf, CTX).collect()
        assert out["EventDate"].to_list() == [date(2020, 1, 5), date(1979, 1, 1)]

    def test_counts_day_values_that_are_not_dates(self):
        lf = pl.LazyFrame({"Day": [20200105, 20201341, None]})
        exprs = DeriveColumns(event_date=True).counts(lf, CTX)
        assert lf.select(**exprs).collect().row(0, named=True) == {"derive.event_date_invalid": 1}

    def test_labels_match_case_insensitively_and_leave_unknown_codes_null(self):
        lf = pl.LazyFrame({"Actor1EthnicCode": ["kur", "KUR", "zzz", None]})
        step = DeriveColumns(label_maps=(("Actor1EthnicCode", (("KUR", "Kurd"),)),))
        out = step.apply(lf, CTX).collect()
        assert out["Actor1EthnicCode_Label"].to_list() == ["Kurd", "Kurd", None, None]
        # Added, never replacing the code column.
        assert out["Actor1EthnicCode"].to_list() == ["kur", "KUR", "zzz", None]

    def test_is_not_lossy(self):
        assert DeriveColumns(event_date=True).lossy is False

    def test_settings_name_the_columns_not_the_tables(self):
        step = DeriveColumns(label_maps=(("EventRootCode", (("01", "MAKE PUBLIC STATEMENT"),)),))
        assert step.settings() == {"event_date": False, "labels": ["EventRootCode"]}
