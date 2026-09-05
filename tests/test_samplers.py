import logging
from datetime import date

import numpy as np
import polars as pl
import pytest

from gdeltforge.sampling.samplers import (
    CalendarSampler,
    FilteredSampler,
    IndexedSampler,
    _apply_reservoir_replacements,
    _assign_column,
    _dedup_last_write_per_slot,
    _reservoir_to_dataframe,
)
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
    pl.DataFrame({
        "GlobalEventID": [1, 2, 3, 4, 5, 6],
        "Day": [20200101] * 6,
        "QuadClass": [1, 2, 3, 4, 1, 2],
        "IsRootEvent": [1, 0, 1, 0, 1, 1],
        "Actor1CountryCode": ["USA", "BRA", "USA", "CHN", "RUS", "BRA"],
        "Actor2CountryCode": ["CHN", "USA", "BRA", "USA", "USA", "RUS"],
        "ActionGeo_CountryCode": ["US", "BR", "US", "CH", "RU", "BR"],
        "GoldsteinScale": [-5.0, 0.0, 3.0, 5.0, -2.0, 1.0],
        "NumArticles": [1, 20, 5, 50, 3, 10],
    }).write_parquet(folder / "data.parquet")


def _size_by_group(df: pl.DataFrame, col: str) -> dict:
    """pandas' groupby(col).size() returns a Series indexed by group key;
    polars' group_by(col).len() returns a plain [col, "len"] DataFrame
    instead, so this reshapes it into the same key -> count mapping the
    tests below index into."""
    grouped = df.group_by(col).len()
    return dict(zip(grouped[col].to_list(), grouped["len"].to_list(), strict=True))


class TestIndexedSampler:
    def test_sample_size_and_uniqueness(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({"GlobalEventID": range(20)}).write_parquet(folder / "a.parquet")

        sampler = IndexedSampler(str(folder), random_state=42)
        df = sampler.get_random_sample(5)

        assert len(df) == 5
        assert df["GlobalEventID"].n_unique() == len(df)

    def test_raises_when_n_exceeds_total_rows(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({"GlobalEventID": range(5)}).write_parquet(folder / "a.parquet")

        sampler = IndexedSampler(str(folder), random_state=1)
        with pytest.raises(ValueError):
            sampler.get_random_sample(10)

    def test_reproducible_with_same_seed(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({"GlobalEventID": range(50)}).write_parquet(folder / "a.parquet")

        s1 = IndexedSampler(str(folder), random_state=7).get_random_sample(10)
        s2 = IndexedSampler(str(folder), random_state=7).get_random_sample(10)

        assert sorted(s1["GlobalEventID"]) == sorted(s2["GlobalEventID"])

    def test_no_files_raises(self, tmp_path):
        folder = tmp_path / "empty"
        folder.mkdir()
        with pytest.raises(FileNotFoundError):
            IndexedSampler(str(folder), random_state=1)

    def test_works_with_a_historical_only_tree_and_no_flat_siblings(self, tmp_path):
        # gdelt_event_reduced always writes Hive-partitioned by Year and
        # never has a flat sibling at all (see converter.py's
        # dataset_is_always_historical), unlike every other dataset,
        # which always has at least some flat files even when
        # partitioning is enabled for part of its output. folder_path
        # here is empty (not even created), the same as gdelt_event_
        # reduced's always-unused flat directory in a real run.
        flat_folder = tmp_path / "flat"
        historical_folder = tmp_path / "historical" / "Year=1979"
        historical_folder.mkdir(parents=True)
        pl.DataFrame({"Date": range(20)}).write_parquet(historical_folder / "a.parquet")

        sampler = IndexedSampler(
            str(flat_folder), historical_folder=str(tmp_path / "historical"),
            random_state=42,
        )
        df = sampler.get_random_sample(5)

        assert len(df) == 5
        assert df["Date"].n_unique() == len(df)

    def test_columns_restricts_output(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(20), "QuadClass": [1] * 20, "GoldsteinScale": [0.0] * 20,
        }).write_parquet(folder / "a.parquet")

        sampler = IndexedSampler(str(folder), random_state=42, columns={"GlobalEventID"})
        df = sampler.get_random_sample(5)

        assert list(df.columns) == ["GlobalEventID"]

    def test_no_columns_arg_returns_everything(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(10), "QuadClass": [1] * 10,
        }).write_parquet(folder / "a.parquet")

        sampler = IndexedSampler(str(folder), random_state=1)
        df = sampler.get_random_sample(5)

        assert set(df.columns) == {"GlobalEventID", "QuadClass"}


class TestCalendarSampler:
    def test_caps_at_samples_per_period(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(10),
            "Day": [20200101] * 3 + [20200102] * 7,
        }).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1)
        df = sampler.get_calendar_samples(samples_per_period=2)

        counts = _size_by_group(df, "Day")
        assert counts[20200101] == 2
        assert counts[20200102] == 2

    def test_takes_all_rows_when_fewer_than_requested(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame(
            {"GlobalEventID": [1, 2], "Day": [20200101, 20200101]}
        ).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1)
        df = sampler.get_calendar_samples(samples_per_period=10)

        assert len(df) == 2

    def test_zero_samples_per_period_is_a_clean_empty_success(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame(
            {"GlobalEventID": [1, 2], "Day": [20200101, 20200101]}
        ).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1)
        df = sampler.get_calendar_samples(samples_per_period=0)

        assert len(df) == 0

    def test_negative_samples_per_period_raises_instead_of_a_nonsensical_success(
        self, tmp_path
    ):
        # A negative value used to reach the reservoir machinery
        # unchecked, same as 0, logging "Saved calendar sample (0 rows,
        # period=day)" as though -5 were a valid, deliberately chosen
        # configuration rather than a mistake.
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame(
            {"GlobalEventID": [1, 2], "Day": [20200101, 20200101]}
        ).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1)
        with pytest.raises(ValueError, match="non-negative"):
            sampler.get_calendar_samples(samples_per_period=-5)

    def test_date_column_missing_from_every_file_raises_clearly(self, tmp_path):
        # Replaces the old DailySampler's own flaw of silently skipping a
        # file missing its date column and returning an empty result: a
        # misconfigured --date-column (or a dataset's real date column
        # renamed upstream) should fail with a message naming the actual
        # problem, not a quietly empty sample indistinguishable from
        # "correctly found nothing."
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({"GlobalEventID": [1, 2]}).write_parquet(folder / "no_day.parquet")

        sampler = CalendarSampler(str(folder), random_state=1)
        with pytest.raises(ValueError, match="'Day' is not a column"):
            sampler.get_calendar_samples(samples_per_period=5)

    def test_columns_restricts_output(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(6), "Day": [20200101] * 6, "GoldsteinScale": [0.0] * 6,
        }).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1, columns={"GlobalEventID"})
        df = sampler.get_calendar_samples(samples_per_period=3)

        # Day rides along even though it wasn't requested, see
        # test_date_column_is_kept_even_when_not_requested for why that's
        # correct. GoldsteinScale, not requested and not needed for
        # grouping, is the one that should actually be pruned.
        assert set(df.columns) == {"GlobalEventID", "Day"}

    def test_date_column_is_kept_even_when_not_requested(self, tmp_path):
        # Day drives the grouping itself; omitting it from --columns must
        # not silently make every file look like it has no Day column.
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(6), "Day": [20200101] * 6, "GoldsteinScale": [0.0] * 6,
        }).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1, columns={"GlobalEventID"})
        df = sampler.get_calendar_samples(samples_per_period=3)

        assert not df.is_empty()
        assert "Day" in df.columns

    def test_date_column_can_be_overridden_for_non_events_schemas(self, tmp_path):
        # Events uses "Day"; other GDELT datasets (GKG, Mentions) use a
        # differently-named date field, so the grouping column must be
        # configurable rather than hardcoded.
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "DocumentIdentifier": range(10),
            "V21Date": [20200101] * 3 + [20200102] * 7,
        }).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1, date_column="V21Date")
        df = sampler.get_calendar_samples(samples_per_period=2)

        counts = _size_by_group(df, "V21Date")
        assert counts[20200101] == 2
        assert counts[20200102] == 2

    def test_default_date_column_is_still_day(self, tmp_path):
        # Regression guard: the date_column parameter must default to
        # "Day" so existing Events callers are unaffected.
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({"GlobalEventID": [1], "Day": [20200101]}).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1)
        assert sampler.date_column == "Day"

    def test_default_period_is_day(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({"GlobalEventID": [1], "Day": [20200101]}).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1)
        assert sampler.period == "day"

    def test_rejects_unknown_period(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({"GlobalEventID": [1], "Day": [20200101]}).write_parquet(folder / "a.parquet")

        with pytest.raises(ValueError):
            CalendarSampler(str(folder), random_state=1, period="week")

    def test_period_month_groups_across_days(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(10),
            "Day": [20200101, 20200115] * 3 + [20200201] * 4,
        }).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1, period="month")
        df = sampler.get_calendar_samples(samples_per_period=3)

        assert len(df) == 6  # 3 rows for 202001, 3 (capped) for 202002
        # All sampled rows' Day values genuinely fall in one of the two months.
        assert set(df["Day"].to_list()) <= {20200101, 20200115, 20200201}

    def test_period_year_groups_across_months(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(6),
            "Day": [20200101, 20200601, 20201225, 20210101, 20210601, 20211225],
        }).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1, period="year")
        df = sampler.get_calendar_samples(samples_per_period=2)

        assert len(df) == 4  # 2 for 2020, 2 for 2021: neither year exceeds its cap

    def test_14_digit_timestamp_column_groups_by_date_prefix(self, tmp_path):
        # GKG 2.1's V2.1DATE and Mentions' MentionTimeDate are 14-digit
        # YYYYMMDDHHMMSS timestamps, not plain 8-digit dates; period="day"
        # must still group by just the date portion, not treat every
        # distinct timestamp as its own period of one.
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GLOBALEVENTID": range(4),
            "MentionTimeDate": [
                20200101000000, 20200101120000, 20200101235959, 20200102000000,
            ],
        }).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(
            str(folder), random_state=1, date_column="MentionTimeDate"
        )
        df = sampler.get_calendar_samples(samples_per_period=10)

        counts = _size_by_group(df, "MentionTimeDate")
        assert sum(counts.values()) == 4
        # All three 20200101 timestamps land in the same period as each
        # other (not split into three periods of one), confirmed by the
        # full sample containing rows from both real days without
        # exceeding either day's own row count.

    def test_period_correct_when_a_period_spans_multiple_files(self, tmp_path):
        # The actual bug this sampler fixes, versus the old DailySampler:
        # a period's rows spread across more than one file must still be
        # capped once for the whole period, not once per contributing
        # file (which would multiply the true per-period total by however
        # many files happen to cover it).
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(5), "Day": [20200101] * 5,
        }).write_parquet(folder / "a.parquet")
        pl.DataFrame({
            "GlobalEventID": range(5, 10), "Day": [20200101] * 5,
        }).write_parquet(folder / "b.parquet")

        sampler = CalendarSampler(str(folder), random_state=1)
        df = sampler.get_calendar_samples(samples_per_period=3)

        # 10 total rows for 20200101, spread across two files: capped at
        # 3 for the whole day, not 3 per file (which would give 6).
        assert len(df) == 3

    def test_unparseable_date_is_dropped_with_a_warning(self, tmp_path, caplog):
        # A null date_column value has nothing meaningful to be grouped
        # under. Polars' own group_by, unlike pandas' groupby's dropna=
        # True default, keeps a null key as its own group rather than
        # excluding it, so this has to be handled explicitly rather than
        # assumed: dropped and counted, not sampled as if "unparseable"
        # were itself a real calendar period.
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": [1, 2, 3],
            "Day": [20200101, None, 20200102],
        }).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), date_column="Day", random_state=1)
        with caplog.at_level(logging.WARNING):
            df = sampler.get_calendar_samples(samples_per_period=10)

        assert sorted(df["GlobalEventID"].to_list()) == [1, 3]
        assert "1 row(s) with an unparseable Day" in caplog.text

    def test_reconciles_dtype_when_periods_disagree_on_nullability(self, tmp_path):
        # Regression test for a real bug found in QA against events-reduced:
        # _apply_reservoir_replacements/_assign_column upcasts a numeric
        # column from Int64 to Float64 the moment a null lands in THAT
        # period's own reservoir (numpy has no native nullable int, so
        # to_numpy() forces float64+NaN). Two periods can legitimately
        # disagree on the same column's dtype this way, with nothing
        # wrong in either reservoir on its own; concatenating them with a
        # plain vertical concat raised ("type Int64 is incompatible with
        # expected type Float64") instead of reconciling them.
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": [1, 2, 3, 4],
            "Day": [20200101, 20200101, 20200102, 20200102],
            "NumArticles": [1, 2, 3, None],
        }).write_parquet(folder / "a.parquet")

        sampler = CalendarSampler(str(folder), random_state=1)
        df = sampler.get_calendar_samples(samples_per_period=2)

        assert len(df) == 4
        assert df["NumArticles"].dtype == pl.Float64
        assert sorted(df["NumArticles"].drop_nulls().to_list()) == [1.0, 2.0, 3.0]
        assert df["NumArticles"].null_count() == 1


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


def _make_pruned_dataset(folder):
    """The same rows _make_dataset writes, but as convert/filter's own
    output_columns pruning would leave them: fewer columns than
    GDELT_COLUMNS declares, not all of them. Exercises the real gap this
    covers, not output_columns itself: --columns/self.columns still
    defaults to the FULL declared schema, so a file actually missing
    some of it is what used to crash rather than narrow."""
    pl.DataFrame({
        "GlobalEventID": [1, 2, 3, 4, 5, 6],
        "Day": [20200101] * 6,
        "ActionGeo_CountryCode": ["US", "BR", "US", "CH", "RU", "BR"],
    }).write_parquet(folder / "data.parquet")


class TestGracefulColumnNarrowing:
    """
    Regression coverage for a real gap found via a live comprehensive
    QA pass: sample --mode filtered/stratified and crossref (see
    crossref.py's own tests) default their column projection to a
    dataset's full declared schema (columns.<dataset> in config) unless
    --columns is passed explicitly, which isn't the same thing as what a
    real, possibly output_columns-pruned file on disk actually has.
    That mismatch used to crash with a raw, unhelpful polars error at
    the eventual .select() ("unable to find column ...") instead of
    either working around it or explaining what happened.

    A column the caller has no usable path forward without at all (a
    --filter condition's own column, --stratify's grouping column)
    still raises a clear error if genuinely missing, rather than being
    silently narrowed away into a query that would just quietly find
    nothing. See utils/io.py's own TestNarrowToAvailableColumns for the
    narrowing/warning logic itself; these only need to confirm each
    sampler wires it in correctly, not re-verify the logic.
    """

    def test_default_full_schema_projection_narrows_with_a_warning(self, tmp_path, caplog):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_pruned_dataset(folder)

        sampler = FilteredSampler(str(folder), GDELT_COLUMNS)
        with caplog.at_level(logging.WARNING):
            df = sampler.filter_dataset()

        assert set(df.columns) == {"GlobalEventID", "Day", "ActionGeo_CountryCode"}
        assert len(df) == 6
        assert any("output_columns" in r.message for r in caplog.records)

    def test_get_random_sample_narrows_with_a_warning(self, tmp_path, caplog):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_pruned_dataset(folder)

        sampler = FilteredSampler(str(folder), GDELT_COLUMNS, random_state=1)
        with caplog.at_level(logging.WARNING):
            df = sampler.get_random_sample(3)

        assert set(df.columns) == {"GlobalEventID", "Day", "ActionGeo_CountryCode"}
        assert any("output_columns" in r.message for r in caplog.records)

    def test_a_filter_column_genuinely_missing_raises_clearly(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_pruned_dataset(folder)

        # NumArticles is in GDELT_COLUMNS (so this passes __init__'s own
        # validation against the declared schema) but isn't in the
        # pruned file: the filter can't be evaluated at all without it,
        # so this must raise rather than silently narrow it away.
        sampler = FilteredSampler(
            str(folder), GDELT_COLUMNS, filter_dict={"NumArticles": 5},
        )
        with pytest.raises(ValueError, match="required column.*NumArticles"):
            sampler.filter_dataset()

    def test_stratify_column_genuinely_missing_raises_clearly(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_pruned_dataset(folder)

        sampler = FilteredSampler(str(folder), GDELT_COLUMNS, random_state=1)
        with pytest.raises(ValueError, match="required column.*QuadClass"):
            sampler.get_stratified_sample("QuadClass", n_per_group=2)

    def test_calendar_sampler_narrows_an_explicit_columns_request(self, tmp_path, caplog):
        folder = tmp_path / "data"
        folder.mkdir()
        _make_pruned_dataset(folder)

        sampler = CalendarSampler(
            str(folder), columns={"ActionGeo_CountryCode", "GoldsteinScale"},
            date_column="Day", period="day", random_state=1,
        )
        with caplog.at_level(logging.WARNING):
            df = sampler.get_calendar_samples(samples_per_period=10)

        assert set(df.columns) == {"ActionGeo_CountryCode", "Day"}
        assert any("GoldsteinScale" in r.message for r in caplog.records)

    def test_calendar_sampler_with_no_explicit_columns_is_unaffected(self, tmp_path, caplog):
        # self.columns is None here (no --columns passed at all): this
        # already read whatever a file actually has, with nothing
        # declared-but-missing to warn about, both before and after
        # this fix existed.
        folder = tmp_path / "data"
        folder.mkdir()
        _make_pruned_dataset(folder)

        sampler = CalendarSampler(str(folder), date_column="Day", period="day", random_state=1)
        with caplog.at_level(logging.WARNING):
            df = sampler.get_calendar_samples(samples_per_period=10)

        assert set(df.columns) == {"GlobalEventID", "Day", "ActionGeo_CountryCode"}
        assert not any(r.levelno >= logging.WARNING for r in caplog.records)


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
        pl.DataFrame({
            "GlobalEventID": range(30), "QuadClass": [1] * 30,
        }).write_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        df = sampler.get_random_sample(10)

        assert len(df) == 10
        assert df["GlobalEventID"].n_unique() == len(df)

    def test_n_zero_is_a_clean_empty_success(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(15), "QuadClass": [1] * 15,
        }).write_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        df = sampler.get_random_sample(0)

        assert len(df) == 0

    def test_negative_n_raises_instead_of_a_nonsensical_success(self, tmp_path):
        # A negative n used to reach the reservoir machinery unchecked,
        # same as 0, producing a nonsensical but "successful" empty
        # result rather than an error naming the actual mistake.
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(15), "QuadClass": [1] * 15,
        }).write_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        with pytest.raises(ValueError, match="non-negative"):
            sampler.get_random_sample(-5)

    def test_takes_everything_when_n_equals_total(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(15), "QuadClass": [1] * 15,
        }).write_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        df = sampler.get_random_sample(15)

        assert set(df["GlobalEventID"]) == set(range(15))

    def test_reproducible_with_same_seed(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(100), "QuadClass": [1] * 100,
        }).write_parquet(folder / "a.parquet")

        cols = ["GlobalEventID", "QuadClass"]
        s1 = FilteredSampler(str(folder), cols, random_state=99).get_random_sample(20)
        s2 = FilteredSampler(str(folder), cols, random_state=99).get_random_sample(20)

        assert sorted(s1["GlobalEventID"]) == sorted(s2["GlobalEventID"])

    def test_respects_filter_before_sampling(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(20),
            "QuadClass": [1 if i < 5 else 2 for i in range(20)],
        }).write_parquet(folder / "a.parquet")

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
        target_slots, source_pos = _dedup_last_write_per_slot(
            rand_slots, capacity=10
        )
        result = dict(zip(target_slots.tolist(), source_pos.tolist(), strict=True))
        assert result == {5: 4, 3: 3}

    def test_rejects_slots_at_or_beyond_capacity(self):
        rand_slots = np.array([0, 5, 10, 11])
        target_slots, _source_pos = _dedup_last_write_per_slot(
            rand_slots, capacity=10
        )
        assert sorted(target_slots.tolist()) == [0, 5]

    def test_empty_when_nothing_accepted(self):
        rand_slots = np.array([10, 11, 12])
        target_slots, source_pos = _dedup_last_write_per_slot(
            rand_slots, capacity=10
        )
        assert target_slots.size == 0
        assert source_pos.size == 0


class TestAssignColumn:
    def test_plain_assignment_when_dtypes_already_compatible(self):
        arr = np.array([1, 2, 3], dtype=np.int64)
        out = _assign_column(arr, np.array([0, 2]), np.array([10, 30]))
        assert out.dtype == np.int64
        assert out.tolist() == [10, 2, 30]

    def test_upcasts_int_array_when_incoming_values_have_nan(self):
        arr = np.array([1, 2, 3], dtype=np.int64)
        out = _assign_column(
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
        return pl.DataFrame({
            "GlobalEventID": rng.integers(0, 10**9, n),
            "QuadClass": rng.integers(1, 5, n),
            "Actor1CountryCode": rng.choice(["USA", "BRA", "CHN", "RUS", "FRA"], n),
            "GoldsteinScale": rng.uniform(-10, 10, n),
        })

    @staticmethod
    def _to_cols(df):
        # writable=True forces a genuinely independent copy: polars' own
        # to_numpy() otherwise returns a read-only array sharing memory
        # with the source column (confirmed directly; assigning into it
        # without this raises "assignment destination is read-only"),
        # unlike pandas' to_numpy(copy=True) equivalent this replaces.
        return {c: df[c].to_numpy(writable=True) for c in df.columns}

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

        # polars frames have no in-place row setter the way pandas' iloc
        # does; the true-sequential reference is instead built by mutating
        # plain Python row dicts one at a time, in position order, then
        # rebuilding a DataFrame from the result. Independent of the
        # vectorized implementation under test either way.
        reservoir_rows = reservoir_ref.to_dicts()
        batch_rows = batch.to_dicts()
        for k in range(batch_size):
            if rand_slots[k] < n_reservoir:
                reservoir_rows[int(rand_slots[k])] = batch_rows[k]
        reservoir_ref = pl.DataFrame(reservoir_rows)

        _apply_reservoir_replacements(
            reservoir_cols, batch, rand_slots, n_reservoir
        )

        reservoir_vec = pl.DataFrame(reservoir_cols)
        assert reservoir_ref.equals(reservoir_vec)
        assert reservoir_ref.schema == reservoir_vec.schema

    def test_upcasts_int_column_when_incoming_row_has_a_null(self):
        # A batch row can carry a null in a column that's int64 in the
        # reservoir (nullable numeric GDELT fields, e.g. NumArticles).
        # Plain numpy assignment raises rather than upcasting on its own;
        # _assign_column upcasts just that column and retries.
        reservoir_cols = {
            "GlobalEventID": np.array([1, 2, 3], dtype=np.int64),
            "NumArticles": np.array([10, 20, 30], dtype=np.int64),
            "Actor1CountryCode": np.array(["USA", "BRA", "CHN"], dtype=object),
        }
        # None (not float("nan")), so polars infers a genuine nullable
        # Int64 column here rather than an already-float64 one: real
        # GDELT nullable numeric fields carry an actual null in the
        # source parquet, not a float NaN literal.
        batch = pl.DataFrame({
            "GlobalEventID": [101, 102],
            "NumArticles": [5, None],
            "Actor1CountryCode": ["RUS", "FRA"],
        })
        rand_slots = np.array([0, 1])

        _apply_reservoir_replacements(reservoir_cols, batch, rand_slots, 3)

        assert reservoir_cols["NumArticles"].dtype == np.float64
        assert reservoir_cols["NumArticles"][0] == 5.0
        assert np.isnan(reservoir_cols["NumArticles"][1])
        assert reservoir_cols["GlobalEventID"][2] == 3  # untouched slot survives

    def test_no_accepted_rows_leaves_reservoir_unchanged(self):
        reservoir_cols = self._to_cols(self._make_df(10, seed=1))
        original = {c: arr.copy() for c, arr in reservoir_cols.items()}
        batch = self._make_df(5, seed=2)
        rand_slots = np.array([100, 101, 102, 103, 104])  # all >= capacity

        _apply_reservoir_replacements(reservoir_cols, batch, rand_slots, 10)

        for c in reservoir_cols:
            assert np.array_equal(reservoir_cols[c], original[c])


class TestReservoirToDataFrame:
    """
    Regression coverage for a real, non-deterministic failure found
    against live scraped data: CalendarSampler/get_random_sample/
    get_stratified_sample all rebuild a DataFrame from a reservoir's
    plain numpy arrays (see _apply_reservoir_replacements's own
    docstring for why the reservoir is numpy, not a DataFrame, mid-scan)
    via pl.DataFrame(reservoir_cols). Without an explicit schema, a
    string column's reservoir whose sampled slots all happened to be
    null gets inferred as pl.Object instead of pl.Utf8 from its numpy
    content alone, since polars can't tell "meant to be a string column"
    from an all-None object array; concatenating it against another
    period/group's reservoir where the column correctly inferred as
    Utf8 then fails ("... incompatible with expected type String").

    Passing schema= directly to pl.DataFrame for every column uniformly
    looked like the fix, but confirmed directly against the exact array
    shapes real data produced to still fail non-deterministically a
    different way ("cannot cast 'Object' type"): schema= there only
    kicks in a cast after polars' own content-based inference already
    ran, and Object can't be cast to String at all, so a reservoir
    polars' own guess landed on Object for (not only the all-null case;
    a normal mixed None/string array triggered it too) failed outright
    instead of succeeding alone and only failing later at concat.
    Forcing that schema onto a NUMERIC column was also actively wrong,
    not just unnecessary, once _apply_reservoir_replacements' own
    mid-scan Int64 -> Float64 upcast (see _assign_column) makes the
    schema captured once at fill time stale by reconstruction time.

    _reservoir_to_dataframe only reaches for the captured schema on an
    object-dtype array (the one genuinely ambiguous case); a numeric
    array's own current dtype already uniquely determines the right
    polars type, so that path stays exactly as fast, and as immune to
    the stale-schema trap, as it was before this fix existed.
    """

    def test_all_null_string_column_reservoir_concats_cleanly(self):
        schema: dict[str, pl.DataType] = {"ActionGeo_CountryCode": pl.Utf8()}
        all_null = _reservoir_to_dataframe(
            {"ActionGeo_CountryCode": np.array([None, None, None], dtype=object)}, schema
        )
        mixed = _reservoir_to_dataframe(
            {"ActionGeo_CountryCode": np.array(["US", None, "FR"], dtype=object)}, schema
        )

        assert all_null.schema["ActionGeo_CountryCode"] == pl.Utf8
        result = pl.concat([all_null, mixed])
        assert result["ActionGeo_CountryCode"].to_list() == [None, None, None, "US", None, "FR"]

    def test_mixed_string_column_from_a_real_to_numpy_array_reconstructs_cleanly(self):
        # The "cannot cast 'Object' type" failure needed a numpy array
        # that actually went through polars' own to_numpy(), not a hand-
        # built np.array literal: confirmed directly, only the former
        # reproduced it against real data. Rebuilt here the same way
        # the real reservoir does (fill phase, then a passthrough
        # replacement round), rather than asserting against a literal
        # that might not carry the same internal array flags.
        schema: dict[str, pl.DataType] = {"c": pl.Utf8()}
        source = pl.DataFrame({"c": [None, "PS", "CA", "CA", "US"] * 4}, schema=schema)
        arr = source["c"].to_numpy(writable=True)

        result = _reservoir_to_dataframe({"c": arr}, schema)

        assert result.schema["c"] == pl.Utf8
        assert result["c"].to_list() == source["c"].to_list()

    def test_a_numeric_column_ignores_a_stale_captured_dtype(self):
        # _assign_column upcasts a reservoir column's real dtype mid-
        # scan the moment a null needs writing into a column that was
        # int64 at fill time (see its own docstring), so the schema
        # captured once at fill time can already be wrong for a numeric
        # column by the time this runs. Forcing it back on would raise
        # (a real float64 NaN has no valid Int64 representation to cast
        # to); reconstructing from the array's own current dtype instead
        # sidesteps the staleness entirely, matching what a numeric
        # column always did here even before this fix existed.
        stale_schema: dict[str, pl.DataType] = {"n": pl.Int64()}
        arr = np.array([1.0, np.nan, 3.0], dtype=np.float64)  # already upcast

        result = _reservoir_to_dataframe({"n": arr}, stale_schema)

        assert result.schema["n"] == pl.Float64
        assert result["n"][0] == 1.0
        # nan_to_null=True: the upcast's NaN is standing in for a real
        # source null (GDELT's numeric fields have no domain concept of a
        # computed NaN distinct from "missing"), so it must reconstruct
        # as an actual polars null, not survive as a literal float NaN a
        # plain is_null() check would miss entirely.
        assert result["n"][1] is None
        assert result["n"].null_count() == 1
        assert result["n"][2] == 3.0

    def test_a_normal_numeric_column_reconstructs_unchanged(self):
        result = _reservoir_to_dataframe(
            {"n": np.array([1, 2, 3], dtype=np.int64)}, {"n": pl.Int64()}
        )

        assert result.schema["n"] == pl.Int64
        assert result["n"].to_list() == [1, 2, 3]


class TestStratifiedSampling:
    def test_exact_n_per_group(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(40),
            "QuadClass": [1] * 10 + [2] * 10 + [3] * 10 + [4] * 10,
        }).write_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        df = sampler.get_stratified_sample("QuadClass", n_per_group=4)

        counts = _size_by_group(df, "QuadClass")
        assert all(v == 4 for v in counts.values())
        assert len(df) == 16

    def test_takes_all_when_group_smaller_than_requested(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(12),
            "QuadClass": [1] * 2 + [2] * 10,
        }).write_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        df = sampler.get_stratified_sample("QuadClass", n_per_group=5)

        counts = _size_by_group(df, "QuadClass")
        assert counts[1] == 2
        assert counts[2] == 5

    def test_zero_n_per_group_is_a_clean_empty_success(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(4),
            "QuadClass": [1] * 2 + [2] * 2,
        }).write_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        df = sampler.get_stratified_sample("QuadClass", n_per_group=0)

        assert len(df) == 0

    def test_negative_n_per_group_raises_instead_of_a_nonsensical_success(self, tmp_path):
        # A negative value used to reach the reservoir machinery
        # unchecked, same as 0, logging "Saved stratified sample (0 rows)
        # stratified by 'QuadClass' (-3 per group)" as though -3 were a
        # valid, chosen configuration.
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": range(4),
            "QuadClass": [1] * 2 + [2] * 2,
        }).write_parquet(folder / "a.parquet")

        sampler = FilteredSampler(str(folder), ["GlobalEventID", "QuadClass"], random_state=1)
        with pytest.raises(ValueError, match="non-negative"):
            sampler.get_stratified_sample("QuadClass", n_per_group=-3)

    def test_reconciles_dtype_when_groups_disagree_on_nullability(self, tmp_path):
        # Same root cause as CalendarSampler's identical regression test
        # (test_reconciles_dtype_when_periods_disagree_on_nullability):
        # a numeric column upcasts to Float64 only within whichever
        # group's own reservoir actually drew a null; a sibling group
        # with no null stays Int64. Plain vertical concat raised on that
        # disagreement instead of reconciling it.
        folder = tmp_path / "data"
        folder.mkdir()
        pl.DataFrame({
            "GlobalEventID": [1, 2, 3, 4],
            "QuadClass": [1, 1, 2, 2],
            "NumArticles": [1, 2, 3, None],
        }).write_parquet(folder / "a.parquet")

        sampler = FilteredSampler(
            str(folder), ["GlobalEventID", "QuadClass", "NumArticles"], random_state=1
        )
        df = sampler.get_stratified_sample("QuadClass", n_per_group=2)

        assert len(df) == 4
        assert df["NumArticles"].dtype == pl.Float64
        assert sorted(df["NumArticles"].drop_nulls().to_list()) == [1.0, 2.0, 3.0]
        assert df["NumArticles"].null_count() == 1


# ----------------------------------------------------------
# --start-date/--end-date file-level pre-filtering
# ----------------------------------------------------------
def _write_daily_file(folder, day: str, ids: list[int]):
    """day is YYYYMMDD; filename matches parse_file_date's daily pattern."""
    pl.DataFrame({"GlobalEventID": ids, "Day": [int(day)] * len(ids)}).write_parquet(
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
        pl.DataFrame({"GlobalEventID": [99], "Day": [19790101]}).write_parquet(
            folder / "misc.parquet"
        )

        sampler = IndexedSampler(
            str(folder), random_state=1,
            start_date=date(2015, 6, 1), end_date=date(2015, 6, 30),
        )
        df = sampler.get_random_sample(1)

        assert df["GlobalEventID"].to_list() == [99]

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
        pl.DataFrame({"GlobalEventID": [1, 2]}).write_parquet(folder / "20150101.parquet")
        pl.DataFrame({"GlobalEventID": [3, 4]}).write_parquet(folder / "20150201.parquet")

        sampler = IndexedSampler(
            str(folder), random_state=1,
            start_date=date(2015, 1, 1), end_date=date(2015, 1, 31),
            date_parser=parse_gdelt_gkg_v1_file_date,
        )
        df = sampler.get_random_sample(2)

        assert sorted(df["GlobalEventID"]) == [1, 2]


class TestCalendarSamplerDateFiltering:
    def test_only_in_range_files_contribute(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])
        _write_daily_file(folder, "20150102", [3, 4])
        _write_daily_file(folder, "20150103", [5, 6])

        sampler = CalendarSampler(
            str(folder), random_state=1,
            start_date=date(2015, 1, 2), end_date=date(2015, 1, 2),
        )
        df = sampler.get_calendar_samples(samples_per_period=10)

        assert sorted(df["GlobalEventID"]) == [3, 4]

    def test_unparseable_filename_is_kept_regardless_of_range(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])
        pl.DataFrame({"GlobalEventID": [99], "Day": [19790101]}).write_parquet(
            folder / "misc.parquet"
        )

        sampler = CalendarSampler(
            str(folder), random_state=1,
            start_date=date(2015, 6, 1), end_date=date(2015, 6, 30),
        )
        df = sampler.get_calendar_samples(samples_per_period=10)

        assert df["GlobalEventID"].to_list() == [99]

    def test_fully_excluded_range_raises(self, tmp_path):
        folder = tmp_path / "data"
        folder.mkdir()
        _write_daily_file(folder, "20150101", [1, 2])

        sampler = CalendarSampler(
            str(folder), random_state=1,
            start_date=date(2016, 1, 1), end_date=date(2016, 1, 31),
        )
        with pytest.raises(FileNotFoundError):
            sampler.get_calendar_samples(samples_per_period=10)


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
        pl.DataFrame({
            "GlobalEventID": [1, 2, 3, 4],
            "Day": [20150102] * 4,
            "QuadClass": [1, 2, 1, 2],
        }).write_parquet(folder / "20150102.export.parquet")
        pl.DataFrame({
            "GlobalEventID": [5, 6],
            "Day": [20150103] * 2,
            "QuadClass": [1, 1],
        }).write_parquet(folder / "20150103.export.parquet")

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
