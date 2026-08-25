import logging
from datetime import date

import numpy as np
import pandas as pd
import pytest

from gdeltforge.sampling.samplers import DailySampler, FilteredSampler, IndexedSampler
from gdeltforge.scraping.scraper import parse_gdelt_gkg_v1_file_date

# A small, hand-verifiable dataset covering equality, IN-list, range, and
# AND/OR combinations. Every filter test below asserts an exact expected
# GlobalEventID set computed by hand against this table:
#
# ID  QuadClass  IsRootEvent  Actor1CC  Actor2CC  ActionGeoCC  Goldstein  NumArticles
# 1   1          1            USA       CHN       US           -5.0       1
# 2   2          0            BRA       USA       BR            0.0      20
# 3   3          1            USA       BRA       US            3.0       5
# 4   4          0            CHN       USA       CH            5.0      50
# 5   1          1            RUS       USA       RU           -2.0       3
# 6   2          1            BRA       RUS       BR            1.0      10
GDELT_COLUMNS = [
    "GlobalEventID", "Day", "QuadClass", "IsRootEvent",
    "Actor1CountryCode", "Actor2CountryCode", "ActionGeo_CountryCode",
    "GoldsteinScale", "NumArticles",
]


def _make_dataset(folder):
    pd.DataFrame({
        "GlobalEventID": [1, 2, 3, 4, 5, 6],
        "Day": [20200101] * 6,
        "QuadClass": [1, 2, 3, 4, 1, 2],
        "IsRootEvent": [1, 0, 1, 0, 1, 1],
        "Actor1CountryCode": ["USA", "BRA", "USA", "CHN", "RUS", "BRA"],
        "Actor2CountryCode": ["CHN", "USA", "BRA", "USA", "USA", "RUS"],
        "ActionGeo_CountryCode": ["US", "BR", "US", "CH", "RU", "BR"],
        "GoldsteinScale": [-5.0, 0.0, 3.0, 5.0, -2.0, 1.0],
        "NumArticles": [1, 20, 5, 50, 3, 10],
    }).to_parquet(folder / "data.parquet")


class TestIndexedSampler:
    def test_sample_size_and_uniqueness(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({"GlobalEventID": range(20)}).to_parquet(folder / "a.parquet")

        sampler = IndexedSampler(str(folder), random_state=42)
        df = sampler.get_random_sample(5)

        assert len(df) == 5
        assert df["GlobalEventID"].is_unique

    def test_raises_when_n_exceeds_total_rows(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({"GlobalEventID": range(5)}).to_parquet(folder / "a.parquet")

        sampler = IndexedSampler(str(folder), random_state=1)
        with pytest.raises(ValueError):
            sampler.get_random_sample(10)

    def test_reproducible_with_same_seed(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({"GlobalEventID": range(50)}).to_parquet(folder / "a.parquet")

        s1 = IndexedSampler(str(folder), random_state=7).get_random_sample(10)
        s2 = IndexedSampler(str(folder), random_state=7).get_random_sample(10)

        assert sorted(s1["GlobalEventID"]) == sorted(s2["GlobalEventID"])

    def test_no_files_raises(self, tmp_path):
        folder = tmp_path / "empty"
        folder.mkdir()
        with pytest.raises(FileNotFoundError):
            IndexedSampler(str(folder), random_state=1)

    def test_columns_restricts_output(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(20), "QuadClass": [1] * 20, "GoldsteinScale": [0.0] * 20,
        }).to_parquet(folder / "a.parquet")

        sampler = IndexedSampler(str(folder), random_state=42, columns={"GlobalEventID"})
        df = sampler.get_random_sample(5)

        assert list(df.columns) == ["GlobalEventID"]

    def test_no_columns_arg_returns_everything(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(10), "QuadClass": [1] * 10,
        }).to_parquet(folder / "a.parquet")

        sampler = IndexedSampler(str(folder), random_state=1)
        df = sampler.get_random_sample(5)

        assert set(df.columns) == {"GlobalEventID", "QuadClass"}


class TestDailySampler:
    def test_caps_at_samples_per_day(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(10),
            "Day": [20200101] * 3 + [20200102] * 7,
        }).to_parquet(folder / "a.parquet")

        sampler = DailySampler(str(folder), random_state=1)
        df = sampler.get_daily_samples(samples_per_day=2)

        counts = df.groupby("Day").size()
        assert counts[20200101] == 2
        assert counts[20200102] == 2

    def test_takes_all_rows_when_fewer_than_requested(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame(
            {"GlobalEventID": [1, 2], "Day": [20200101, 20200101]}
        ).to_parquet(folder / "a.parquet")

        sampler = DailySampler(str(folder), random_state=1)
        df = sampler.get_daily_samples(samples_per_day=10)

        assert len(df) == 2

    def test_skips_files_without_day_column(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({"GlobalEventID": [1, 2]}).to_parquet(folder / "no_day.parquet")

        sampler = DailySampler(str(folder), random_state=1)
        df = sampler.get_daily_samples(samples_per_day=5)

        assert df.empty

    def test_columns_restricts_output(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(6), "Day": [20200101] * 6, "GoldsteinScale": [0.0] * 6,
        }).to_parquet(folder / "a.parquet")

        sampler = DailySampler(str(folder), random_state=1, columns={"GlobalEventID"})
        df = sampler.get_daily_samples(samples_per_day=3)

        # Day rides along even though it wasn't requested, see
        # test_day_is_kept_even_when_not_requested for why that's correct.
        # GoldsteinScale, not requested and not needed for grouping, is
        # the one that should actually be pruned.
        assert set(df.columns) == {"GlobalEventID", "Day"}

    def test_day_is_kept_even_when_not_requested(self, tmp_path):
        # Day drives the grouping itself; omitting it from --columns must
        # not silently make every file look like it has no Day column.
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(6), "Day": [20200101] * 6, "GoldsteinScale": [0.0] * 6,
        }).to_parquet(folder / "a.parquet")

        sampler = DailySampler(str(folder), random_state=1, columns={"GlobalEventID"})
        df = sampler.get_daily_samples(samples_per_day=3)

        assert not df.empty
        assert "Day" in df.columns

    def test_date_column_can_be_overridden_for_non_events_schemas(self, tmp_path):
        # Events uses "Day"; other GDELT datasets (GKG, Mentions) use a
        # differently-named date field, so the grouping column must be
        # configurable rather than hardcoded.
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "DocumentIdentifier": range(10),
            "V21Date": [20200101] * 3 + [20200102] * 7,
        }).to_parquet(folder / "a.parquet")

        sampler = DailySampler(str(folder), random_state=1, date_column="V21Date")
        df = sampler.get_daily_samples(samples_per_day=2)

        counts = df.groupby("V21Date").size()
        assert counts[20200101] == 2
        assert counts[20200102] == 2

    def test_default_date_column_is_still_day(self, tmp_path):
        # Regression guard: the new date_column parameter must default to
        # "Day" so existing Events callers are unaffected.
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({"GlobalEventID": [1], "Day": [20200101]}).to_parquet(folder / "a.parquet")

        sampler = DailySampler(str(folder), random_state=1)
        assert sampler.date_column == "Day"


class TestFilteredSamplerValidation:
    def test_rejects_unknown_column_in_columns(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_dataset(folder)

        with pytest.raises(ValueError):
            FilteredSampler(str(folder), GDELT_COLUMNS, columns={"NotAColumn"})

    def test_rejects_unknown_column_in_filter(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_dataset(folder)

        with pytest.raises(ValueError):
            FilteredSampler(str(folder), GDELT_COLUMNS, filter_dict={"NotAColumn": 1})

    def test_a_corrupt_parquet_file_raises_a_clear_error_not_a_bare_arrow_one(
        self, tmp_path
    ):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_dataset(folder)
        (folder / "corrupt.parquet").write_text("not actually parquet content")

        sampler = FilteredSampler(
            str(folder), GDELT_COLUMNS, filter_dict={"Actor1CountryCode": "USA"},
        )
        with pytest.raises(RuntimeError, match="filtered sample dataset"):
            sampler.filter_dataset()


class TestCountryCodeWarnings:
    """
    Regression coverage for the original real-world bug this session found:
    a 3-letter CAMEO code ("USA") used against a 2-letter FIPS geo column
    (ActionGeo_CountryCode) silently matched zero rows. These check that a
    mismatch warns (not raises: FIPS 10-4 was retired in 2008 and can
    lag reality) while correct and unrelated usage stays silent.
    """

    def test_warns_on_cameo_code_used_against_a_geo_column(self, tmp_path, caplog):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_dataset(folder)

        with caplog.at_level(logging.WARNING):
            FilteredSampler(
                str(folder), GDELT_COLUMNS, filter_dict={"ActionGeo_CountryCode": ["USA"]}
            )

        assert "ActionGeo_CountryCode" in caplog.text
        assert "USA" in caplog.text
        assert "FIPS geo" in caplog.text

    def test_warns_on_geo_code_used_against_an_actor_column(self, tmp_path, caplog):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_dataset(folder)

        with caplog.at_level(logging.WARNING):
            FilteredSampler(str(folder), GDELT_COLUMNS, filter_dict={"Actor1CountryCode": "US"})

        assert "Actor1CountryCode" in caplog.text
        assert "CAMEO actor" in caplog.text

    def test_no_warning_for_correct_codes(self, tmp_path, caplog):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_dataset(folder)

        with caplog.at_level(logging.WARNING):
            FilteredSampler(
                str(folder), GDELT_COLUMNS,
                filter_dict={"ActionGeo_CountryCode": ["US", "BR"], "Actor1CountryCode": "USA"},
            )

        assert caplog.text == ""

    def test_no_warning_for_columns_without_a_code_family(self, tmp_path, caplog):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_dataset(folder)

        with caplog.at_level(logging.WARNING):
            FilteredSampler(str(folder), GDELT_COLUMNS, filter_dict={"QuadClass": [1, 2]})

        assert caplog.text == ""

    def test_no_warning_for_lowercase_code_against_uppercase_reference(self, tmp_path, caplog):
        # Real GDELT data stores Actor1/2EthnicCode lowercase while the
        # bundled reference uses uppercase keys; a filter value in either
        # case against a real code should stay silent.
        folder = tmp_path / "data"
        folder.mkdir()
        _make_dataset(folder)

        with caplog.at_level(logging.WARNING):
            FilteredSampler(str(folder), GDELT_COLUMNS, filter_dict={"Actor1CountryCode": "usa"})

        assert caplog.text == ""


class TestFilterExpressions:
    def _filtered_ids(self, tmp_path, filter_dict):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_dataset(folder)
        sampler = FilteredSampler(
            str(folder), GDELT_COLUMNS, filter_dict=filter_dict, random_state=1
        )
        return set(sampler.filter_dataset()["GlobalEventID"])

    def test_equality(self, tmp_path):
        assert self._filtered_ids(tmp_path, {"Actor1CountryCode": "USA"}) == {1, 3}

    def test_in_list(self, tmp_path):
        assert self._filtered_ids(tmp_path, {"QuadClass": [1, 2]}) == {1, 2, 5, 6}

    def test_between_range(self, tmp_path):
        cond = {"GoldsteinScale": {"op": "between", "min": -2, "max": 1}}
        assert self._filtered_ids(tmp_path, cond) == {2, 5, 6}

    def test_greater_than(self, tmp_path):
        cond = {"NumArticles": {"op": "gt", "value": 10}}
        assert self._filtered_ids(tmp_path, cond) == {2, 4}

    def test_less_than(self, tmp_path):
        cond = {"NumArticles": {"op": "lt", "value": 5}}
        assert self._filtered_ids(tmp_path, cond) == {1, 5}

    def test_explicit_equals_op(self, tmp_path):
        cond = {"IsRootEvent": {"op": "equals", "value": 1}}
        assert self._filtered_ids(tmp_path, cond) == {1, 3, 5, 6}

    def test_explicit_in_list_op(self, tmp_path):
        cond = {"QuadClass": {"op": "in_list", "values": [3, 4]}}
        assert self._filtered_ids(tmp_path, cond) == {3, 4}

    def test_top_level_and_is_implicit(self, tmp_path):
        cond = {"Actor1CountryCode": "USA", "QuadClass": [1, 3]}
        assert self._filtered_ids(tmp_path, cond) == {1, 3}

    def test_top_level_or(self, tmp_path):
        cond = {"OR": {"Actor1CountryCode": "USA", "Actor2CountryCode": "USA"}}
        assert self._filtered_ids(tmp_path, cond) == {1, 2, 3, 4, 5}

    def test_nested_and_inside_or(self, tmp_path):
        cond = {
            "OR": {
                "Actor1CountryCode": "RUS",
                "AND": {"Actor1CountryCode": "BRA", "ActionGeo_CountryCode": "BR"},
            }
        }
        assert self._filtered_ids(tmp_path, cond) == {2, 5, 6}

    def test_nested_or_inside_and(self, tmp_path):
        cond = {
            "AND": {
                "QuadClass": [1, 2],
                "OR": {"Actor1CountryCode": "BRA", "Actor2CountryCode": "BRA"},
            }
        }
        assert self._filtered_ids(tmp_path, cond) == {2, 6}


class TestFilteredSamplerReservoirSampling:
    def test_sample_size_matches_request(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(30), "QuadClass": [1] * 30,
        }).to_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        df = sampler.get_random_sample(10)

        assert len(df) == 10
        assert df["GlobalEventID"].is_unique

    def test_takes_everything_when_n_equals_total(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(15), "QuadClass": [1] * 15,
        }).to_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        df = sampler.get_random_sample(15)

        assert set(df["GlobalEventID"]) == set(range(15))

    def test_reproducible_with_same_seed(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(100), "QuadClass": [1] * 100,
        }).to_parquet(folder / "a.parquet")

        cols = ["GlobalEventID", "QuadClass"]
        s1 = FilteredSampler(str(folder), cols, random_state=99).get_random_sample(20)
        s2 = FilteredSampler(str(folder), cols, random_state=99).get_random_sample(20)

        assert sorted(s1["GlobalEventID"]) == sorted(s2["GlobalEventID"])

    def test_respects_filter_before_sampling(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(20),
            "QuadClass": [1 if i < 5 else 2 for i in range(20)],
        }).to_parquet(folder / "a.parquet")

        sampler = FilteredSampler(
            str(folder), ["GlobalEventID", "QuadClass"],
            filter_dict={"QuadClass": 1}, random_state=1,
        )
        df = sampler.get_random_sample(5)

        assert len(df) == 5
        assert (df["QuadClass"] == 1).all()
        assert set(df["GlobalEventID"]).issubset(set(range(5)))


class TestDedupLastWritePerSlot:
    """
    True sequential Algorithm R applies draws in position order, so when
    two rows in the same batch draw the same slot, the higher-position
    (later) one must be the one that survives. target_slots is built from
    rand_slots in ascending position order, so "last occurrence in the
    array" is the same thing as "highest position" here.
    """

    def test_dedups_to_last_occurrence_per_slot(self):
        # positions 0,1,2,3,4 draw slots 5,3,5,3,5: slot 5 should keep
        # position 4 (last), slot 3 should keep position 3 (last).
        rand_slots = np.array([5, 3, 5, 3, 5])
        target_slots, source_pos = FilteredSampler._dedup_last_write_per_slot(
            rand_slots, capacity=10
        )
        result = dict(zip(target_slots.tolist(), source_pos.tolist(), strict=True))
        assert result == {5: 4, 3: 3}

    def test_rejects_slots_at_or_beyond_capacity(self):
        rand_slots = np.array([0, 5, 10, 11])
        target_slots, _source_pos = FilteredSampler._dedup_last_write_per_slot(
            rand_slots, capacity=10
        )
        assert sorted(target_slots.tolist()) == [0, 5]

    def test_empty_when_nothing_accepted(self):
        rand_slots = np.array([10, 11, 12])
        target_slots, source_pos = FilteredSampler._dedup_last_write_per_slot(
            rand_slots, capacity=10
        )
        assert target_slots.size == 0
        assert source_pos.size == 0


class TestAssignColumn:
    def test_plain_assignment_when_dtypes_already_compatible(self):
        arr = np.array([1, 2, 3], dtype=np.int64)
        out = FilteredSampler._assign_column(arr, np.array([0, 2]), np.array([10, 30]))
        assert out.dtype == np.int64
        assert out.tolist() == [10, 2, 30]

    def test_upcasts_int_array_when_incoming_values_have_nan(self):
        arr = np.array([1, 2, 3], dtype=np.int64)
        out = FilteredSampler._assign_column(
            arr, np.array([0, 1]), np.array([5.0, np.nan])
        )
        assert out.dtype == np.float64
        assert out[0] == 5.0
        assert np.isnan(out[1])
        assert out[2] == 3  # untouched slot survives the upcast copy


class TestApplyReservoirReplacements:
    """
    Regression coverage for a real bug: naively bulk-assigning
    reservoir.iloc[dup_slots] = values when multiple rows in the same batch
    draw the same slot let pandas resolve the duplicate independently per
    column block, desyncing string columns from numeric ones in the
    result. These tests force heavy in-batch collisions and check against
    a true sequential (one row at a time, in position order) reference.

    The reservoir here is a dict of per-column numpy arrays (what
    get_random_sample/get_stratified_sample actually hold mid-scan), not a
    DataFrame; _apply_reservoir_replacements writes into it in place.
    """

    @staticmethod
    def _make_df(n, seed):
        rng = np.random.default_rng(seed)
        return pd.DataFrame({
            "GlobalEventID": rng.integers(0, 10**9, n),
            "QuadClass": rng.integers(1, 5, n),
            "Actor1CountryCode": rng.choice(["USA", "BRA", "CHN", "RUS", "FRA"], n),
            "GoldsteinScale": rng.uniform(-10, 10, n),
        })

    @staticmethod
    def _to_cols(df):
        return {c: df[c].to_numpy(copy=True) for c in df.columns}

    def test_matches_true_sequential_application_under_heavy_collisions(self):
        n_reservoir = 50
        batch_size = 500

        reservoir_ref = self._make_df(n_reservoir, seed=1)
        reservoir_cols = self._to_cols(reservoir_ref)
        batch = self._make_df(batch_size, seed=2)

        rng = np.random.default_rng(7)
        positions = np.arange(10, 10 + batch_size)
        # Fold into a small slot range to force many in-batch collisions.
        rand_slots = rng.integers(0, positions + 1) % n_reservoir

        for k in range(batch_size):
            if rand_slots[k] < n_reservoir:
                reservoir_ref.iloc[int(rand_slots[k])] = batch.iloc[k]

        FilteredSampler._apply_reservoir_replacements(
            reservoir_cols, batch, rand_slots, n_reservoir
        )

        reservoir_vec = pd.DataFrame(reservoir_cols)
        assert reservoir_ref.equals(reservoir_vec)
        assert (reservoir_ref.dtypes == reservoir_vec.dtypes).all()

    def test_upcasts_int_column_when_incoming_row_has_a_null(self):
        # A batch row can carry NaN in a column that's int64 in the
        # reservoir (nullable numeric GDELT fields, e.g. NumArticles).
        # Plain numpy assignment raises rather than upcasting on its own;
        # _assign_column upcasts just that column and retries.
        reservoir_cols = {
            "GlobalEventID": np.array([1, 2, 3], dtype=np.int64),
            "NumArticles": np.array([10, 20, 30], dtype=np.int64),
            "Actor1CountryCode": np.array(["USA", "BRA", "CHN"], dtype=object),
        }
        batch = pd.DataFrame({
            "GlobalEventID": [101, 102],
            "NumArticles": [5, np.nan],
            "Actor1CountryCode": ["RUS", "FRA"],
        })
        rand_slots = np.array([0, 1])

        FilteredSampler._apply_reservoir_replacements(reservoir_cols, batch, rand_slots, 3)

        assert reservoir_cols["NumArticles"].dtype == np.float64
        assert reservoir_cols["NumArticles"][0] == 5.0
        assert np.isnan(reservoir_cols["NumArticles"][1])
        assert reservoir_cols["GlobalEventID"][2] == 3  # untouched slot survives

    def test_no_accepted_rows_leaves_reservoir_unchanged(self):
        reservoir_cols = self._to_cols(self._make_df(10, seed=1))
        original = {c: arr.copy() for c, arr in reservoir_cols.items()}
        batch = self._make_df(5, seed=2)
        rand_slots = np.array([100, 101, 102, 103, 104])  # all >= capacity

        FilteredSampler._apply_reservoir_replacements(reservoir_cols, batch, rand_slots, 10)

        for c in reservoir_cols:
            assert np.array_equal(reservoir_cols[c], original[c])


class TestStratifiedSampling:
    def test_exact_n_per_group(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(40),
            "QuadClass": [1] * 10 + [2] * 10 + [3] * 10 + [4] * 10,
        }).to_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        df = sampler.get_stratified_sample("QuadClass", n_per_group=4)

        counts = df.groupby("QuadClass").size()
        assert (counts == 4).all()
        assert len(df) == 16

    def test_takes_all_when_group_smaller_than_requested(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": range(12),
            "QuadClass": [1] * 2 + [2] * 10,
        }).to_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        df = sampler.get_stratified_sample("QuadClass", n_per_group=5)

        counts = df.groupby("QuadClass").size()
        assert counts[1] == 2
        assert counts[2] == 5


# ----------------------------------------------------------
# --start-date/--end-date file-level pre-filtering
# ----------------------------------------------------------
def _write_daily_file(folder, day: str, ids: list[int]):
    """day is YYYYMMDD; filename matches parse_file_date's daily pattern."""
    pd.DataFrame({"GlobalEventID": ids, "Day": [int(day)] * len(ids)}).to_parquet(
        folder / f"{day}.export.parquet"
    )


class TestIndexedSamplerDateFiltering:
    def test_only_in_range_files_are_sampled(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])
        _write_daily_file(folder, "20150102", [3, 4])
        _write_daily_file(folder, "20150103", [5, 6])

        sampler = IndexedSampler(
            str(folder), random_state=1,
            start_date=date(2015, 1, 2), end_date=date(2015, 1, 2),
        )
        df = sampler.get_random_sample(2)

        assert sorted(df["GlobalEventID"]) == [3, 4]

    def test_open_ended_start_date(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])
        _write_daily_file(folder, "20150102", [3, 4])
        _write_daily_file(folder, "20150103", [5, 6])

        sampler = IndexedSampler(str(folder), random_state=1, start_date=date(2015, 1, 2))
        df = sampler.get_random_sample(4)

        assert sorted(df["GlobalEventID"]) == [3, 4, 5, 6]

    def test_open_ended_end_date(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])
        _write_daily_file(folder, "20150102", [3, 4])
        _write_daily_file(folder, "20150103", [5, 6])

        sampler = IndexedSampler(str(folder), random_state=1, end_date=date(2015, 1, 2))
        df = sampler.get_random_sample(4)

        assert sorted(df["GlobalEventID"]) == [1, 2, 3, 4]

    def test_unparseable_filename_is_kept_regardless_of_range(self, tmp_path):
        # A file whose name carries no date the parser understands (e.g. a
        # pre-daily historical archive) is kept rather than excluded, same
        # as filter_paths_by_date already does for convert/filter/crossref.
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])
        pd.DataFrame({"GlobalEventID": [99], "Day": [19790101]}).to_parquet(
            folder / "misc.parquet"
        )

        sampler = IndexedSampler(
            str(folder), random_state=1,
            start_date=date(2015, 6, 1), end_date=date(2015, 6, 30),
        )
        df = sampler.get_random_sample(1)

        assert df["GlobalEventID"].tolist() == [99]

    def test_fully_excluded_range_raises(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])

        with pytest.raises(FileNotFoundError):
            IndexedSampler(
                str(folder), random_state=1,
                start_date=date(2016, 1, 1), end_date=date(2016, 1, 31),
            )

    def test_custom_date_parser_is_honored(self, tmp_path):
        # "20150101.parquet" isn't parseable by the default parse_file_date
        # (no .export.parquet/.zip suffix, wrong length for monthly/yearly),
        # but is a valid GKG 1.0-style filename under parse_gdelt_gkg_v1_file_date.
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({"GlobalEventID": [1, 2]}).to_parquet(folder / "20150101.parquet")
        pd.DataFrame({"GlobalEventID": [3, 4]}).to_parquet(folder / "20150201.parquet")

        sampler = IndexedSampler(
            str(folder), random_state=1,
            start_date=date(2015, 1, 1), end_date=date(2015, 1, 31),
            date_parser=parse_gdelt_gkg_v1_file_date,
        )
        df = sampler.get_random_sample(2)

        assert sorted(df["GlobalEventID"]) == [1, 2]


class TestDailySamplerDateFiltering:
    def test_only_in_range_files_contribute(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])
        _write_daily_file(folder, "20150102", [3, 4])
        _write_daily_file(folder, "20150103", [5, 6])

        sampler = DailySampler(
            str(folder), random_state=1,
            start_date=date(2015, 1, 2), end_date=date(2015, 1, 2),
        )
        df = sampler.get_daily_samples(samples_per_day=10)

        assert sorted(df["GlobalEventID"]) == [3, 4]

    def test_unparseable_filename_is_kept_regardless_of_range(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])
        pd.DataFrame({"GlobalEventID": [99], "Day": [19790101]}).to_parquet(
            folder / "misc.parquet"
        )

        sampler = DailySampler(
            str(folder), random_state=1,
            start_date=date(2015, 6, 1), end_date=date(2015, 6, 30),
        )
        df = sampler.get_daily_samples(samples_per_day=10)

        assert df["GlobalEventID"].tolist() == [99]

    def test_fully_excluded_range_raises(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])

        sampler = DailySampler(
            str(folder), random_state=1,
            start_date=date(2016, 1, 1), end_date=date(2016, 1, 31),
        )
        with pytest.raises(FileNotFoundError):
            sampler.get_daily_samples(samples_per_day=10)


class TestFilteredSamplerDateFiltering:
    def test_only_in_range_files_are_scanned(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])
        _write_daily_file(folder, "20150102", [3, 4])
        _write_daily_file(folder, "20150103", [5, 6])

        sampler = FilteredSampler(
            str(folder), ["GlobalEventID", "Day"], random_state=1,
            start_date=date(2015, 1, 2), end_date=date(2015, 1, 2),
        )
        df = sampler.filter_dataset()

        assert sorted(df["GlobalEventID"]) == [3, 4]

    def test_stacks_with_row_level_filter_dict(self, tmp_path):
        # A file inside the date range can still have its rows narrowed
        # further by --filter; the two mechanisms compose rather than
        # one replacing the other.
        folder = tmp_path / "data"
        folder.mkdir()
        pd.DataFrame({
            "GlobalEventID": [1, 2, 3, 4],
            "Day": [20150102] * 4,
            "QuadClass": [1, 2, 1, 2],
        }).to_parquet(folder / "20150102.export.parquet")
        pd.DataFrame({
            "GlobalEventID": [5, 6],
            "Day": [20150103] * 2,
            "QuadClass": [1, 1],
        }).to_parquet(folder / "20150103.export.parquet")

        sampler = FilteredSampler(
            str(folder), ["GlobalEventID", "Day", "QuadClass"], random_state=1,
            filter_dict={"QuadClass": [1]},
            start_date=date(2015, 1, 2), end_date=date(2015, 1, 2),
        )
        df = sampler.filter_dataset()

        # 20150103 is excluded by the date range even though its rows would
        # otherwise pass QuadClass == 1; only 20150102's QuadClass == 1 rows
        # (GlobalEventID 1, 3) survive both filters.
        assert sorted(df["GlobalEventID"]) == [1, 3]

    def test_fully_excluded_range_raises(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])

        sampler = FilteredSampler(
            str(folder), ["GlobalEventID", "Day"], random_state=1,
            start_date=date(2016, 1, 1), end_date=date(2016, 1, 31),
        )
        with pytest.raises(FileNotFoundError):
            sampler.filter_dataset()
