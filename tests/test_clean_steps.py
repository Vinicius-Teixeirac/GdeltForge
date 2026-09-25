import polars as pl
import pytest

from gdeltforge.cleaning.steps import (
    STEP_ORDER,
    NarrowFloat32,
    ProjectColumns,
    RequireColumns,
    ordered,
)


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
        out = RequireColumns(("a", "b")).apply(lf, "f").collect()
        assert out["a"].to_list() == [1]

    def test_empty_list_is_a_no_op(self):
        lf = pl.LazyFrame({"a": [None]})
        assert RequireColumns(()).apply(lf, "f").collect().height == 1

    def test_no_configured_column_present_raises(self):
        with pytest.raises(ValueError, match="nothing to filter on"):
            RequireColumns(("zz",)).apply(pl.LazyFrame({"a": [1]}), "f")


class TestNarrowFloat32:
    def test_only_float_columns_present_after_earlier_steps_are_cast(self):
        lf = pl.LazyFrame({"x": [1.5], "n": [1]})
        out = NarrowFloat32(("x", "n", "gone")).apply(lf, "f").collect()
        assert out.schema["x"] == pl.Float32
        assert out.schema["n"] == pl.Int64
