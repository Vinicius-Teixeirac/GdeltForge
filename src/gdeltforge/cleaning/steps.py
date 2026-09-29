"""
steps.py

The clean stage's steps, applied to each file in one fixed order,
STEP_ORDER, whatever order a configuration lists them in. A fixed order
means no configuration can produce an order-dependent result by accident
(for instance a null check that runs before whitespace-only strings
become null). docs/data-cleaning.md documents each step and the reasons
behind it.

Every step declares whether it is lossy: whether the cleaned output can
no longer tell what the converted input held. Steps added from 0.12.0 on
are also `guarded`: combined with --delete-source, a lossy guarded step
is refused unless explicitly allowed. The stage's three original steps
keep their earlier behavior there, a warning.

Provides:
    - STEP_ORDER: the fixed order of step names
    - FileContext: what a step knows about the file it runs on
    - Step: base class
    - RequireColumns, ProjectColumns, NarrowFloat32: the stage's
      original operations
    - Date1920Repair, EventMarkers: repairs of known GDELT errors
      (the errata step)
    - NormalizeStrings: optional whitespace normalization
    - DeriveColumns: optional derived columns (a real event date, code
      labels)
    - ordered: sort steps into STEP_ORDER
"""

from __future__ import annotations

import operator
from dataclasses import asdict, dataclass
from datetime import date
from functools import reduce

import polars as pl

from gdeltforge.sampling import cameo_codes
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

# errata: repairs of known GDELT errors. normalize: optional value
# normalization. require: drop rows missing a required value. derive:
# add columns. project: keep a column subset. narrow: smaller encodings.
STEP_ORDER = ("errata", "normalize", "require", "derive", "project", "narrow")

# Bumped whenever an errata rule is added or changed, and recorded in the
# resumability fingerprint, so data cleaned under an older rule set is
# cleaned again on the next run. 2: date_1920's *_original columns are
# written to every file, not only to files around the window.
ERRATA_VERSION = 2


@dataclass(frozen=True)
class FileContext:
    """The file a step runs on: its name, for messages, and the period its
    name says it covers (None where the name can't be parsed)."""

    name: str
    period_start: date | None = None
    period_end: date | None = None


@dataclass(frozen=True)
class Step:
    """
    One step. `name` places it in STEP_ORDER; `lossy` says whether its
    output can no longer tell what its input held; `guarded` says whether
    --delete-source must refuse it when lossy. apply() receives the frame
    as every earlier step left it and returns the frame for the next step.
    counts() returns named expressions over that same input frame, summed
    into the run audit and the dry-run report.
    """

    name = ""
    lossy = False
    guarded = True

    def apply(self, lf: pl.LazyFrame, ctx: FileContext) -> pl.LazyFrame:
        raise NotImplementedError

    def counts(self, lf: pl.LazyFrame, ctx: FileContext) -> dict[str, pl.Expr]:
        return {}

    def settings(self) -> dict:
        """The step's settings as JSON-ready values, for the cleaned-file
        marker and the run audit."""
        return {k: list(v) if isinstance(v, tuple) else v for k, v in asdict(self).items()}


@dataclass(frozen=True)
class RequireColumns(Step):
    """
    Drop rows with a null in any of `columns` (clean.columns_to_check).
    Lossy: dropped rows are gone from the output.
    """

    columns: tuple[str, ...] = ()
    name = "require"
    lossy = True
    guarded = False

    def _present(self, lf: pl.LazyFrame, ctx: FileContext) -> list[str]:
        schema_cols = lf.collect_schema().names()
        existing = [c for c in self.columns if c in schema_cols]
        if not existing:
            # Configured columns, none present: the null check can't run
            # at all. Raising routes the file to "Files failed" and a
            # non-zero exit. Returning quietly used to report it as cleaned
            # at 100% retention, silently skipping the requested check.
            raise ValueError(
                f"{ctx.name}: none of the configured columns_to_check "
                f"{list(self.columns)} exist in this file's schema. There is "
                f"nothing to filter on."
            )
        return existing

    def apply(self, lf: pl.LazyFrame, ctx: FileContext) -> pl.LazyFrame:
        if not self.columns:
            # An empty list (the bundled default ships one per dataset) is
            # a deliberate no-op: every row survives. It must not reach
            # any_horizontal, which polars rejects for an empty list
            # ("cannot return empty fold").
            return lf
        existing = self._present(lf, ctx)
        missing = [c for c in self.columns if c not in existing]
        if missing:
            logger.warning(f"{ctx.name}: Missing {len(missing)} column(s): {missing}")
        return lf.filter(~pl.any_horizontal([pl.col(c).is_null() for c in existing]))

    def counts(self, lf: pl.LazyFrame, ctx: FileContext) -> dict[str, pl.Expr]:
        if not self.columns:
            return {}
        existing = self._present(lf, ctx)
        return {"require.rows_dropped": pl.any_horizontal(
            [pl.col(c).is_null() for c in existing]
        ).sum()}


@dataclass(frozen=True)
class ProjectColumns(Step):
    """
    Keep only `columns` (clean.output_columns). A configured column absent
    from the file is warned about and skipped, since schemas drift across
    GDELT's eras. Lossy: dropped columns are gone from the output.
    """

    columns: tuple[str, ...] = ()
    name = "project"
    lossy = True
    guarded = False

    def apply(self, lf: pl.LazyFrame, ctx: FileContext) -> pl.LazyFrame:
        schema_cols = lf.collect_schema().names()
        missing = [c for c in self.columns if c not in schema_cols]
        if missing:
            # Warned by name: a typo here used to discard the column from
            # every run with no trace at any log level.
            logger.warning(
                f"{ctx.name}: output_columns names {len(missing)} "
                f"column(s) not present in this file's schema: {missing}. "
                f"They will be excluded from the output."
            )
        return lf.select([c for c in self.columns if c in schema_cols])


@dataclass(frozen=True)
class NarrowFloat32(Step):
    """
    Cast `columns` (clean.float32_columns) from float64 to float32, where
    present and floating-point after the earlier steps. Lossy: GDELT
    floats carry up to 15 significant figures (AvgTone), float32 about 7.
    """

    columns: tuple[str, ...] = ()
    name = "narrow"
    lossy = True
    guarded = False

    def apply(self, lf: pl.LazyFrame, ctx: FileContext) -> pl.LazyFrame:
        schema = lf.collect_schema()
        targets = [c for c in self.columns if c in schema and schema[c].is_float()]
        if not targets:
            return lf
        return lf.with_columns([pl.col(c).cast(pl.Float32) for c in targets])


# GDELT wrote year 1920 for 2020 in every event dated in 2020 that it
# added between the 2019-12-31 23:00 UTC and 2020-01-05 15:00 UTC updates,
# in both its daily export and its 15-minute feed (Mentions is unaffected).
# Rows it added outside that window carry no pre-1979 date anywhere in the
# 1979 to 2026 archive, so the rule is exact.
_DATE_1920_WINDOW = (date(2019, 12, 31), date(2020, 1, 5))
_DATE_1920_OFFSETS = {"Day": 1_000_000, "MonthYear": 10_000, "Year": 100, "FractionDate": 100}


@dataclass(frozen=True)
class Date1920Repair(Step):
    """
    Add 100 years to Day, MonthYear, Year and FractionDate where Day is
    before 1979 and DATEADDED falls from 2019-12-31 to 2020-01-05.
    keep_original also writes each repaired column's GDELT value to
    <column>_original on repaired rows, null elsewhere; with it the repair
    is not lossy. Those four columns go into every file that has the date
    columns, null throughout in files outside the window, so a cleaned
    directory has one schema: polars refuses a multi-file read whose files
    disagree on columns, and pandas and pyarrow silently drop the extra
    ones.
    """

    keep_original: bool = True
    name = "errata"

    @property
    def lossy(self) -> bool:  # type: ignore[override]
        return not self.keep_original

    @staticmethod
    def _has_date_columns(lf: pl.LazyFrame) -> bool:
        schema = lf.collect_schema().names()
        return all(c in schema for c in _DATE_1920_OFFSETS)

    @classmethod
    def _applies(cls, lf: pl.LazyFrame, ctx: FileContext) -> bool:
        if not cls._has_date_columns(lf) or "DATEADDED" not in lf.collect_schema().names():
            return False
        # A file whose name puts it wholly outside the window can't hold
        # the bug: its rows were added in its own period.
        start, end = ctx.period_start, ctx.period_end
        lo, hi = _DATE_1920_WINDOW
        return start is None or end is None or not (end < lo or start > hi)

    @staticmethod
    def _condition() -> pl.Expr:
        added = pl.col("DATEADDED").cast(pl.Int64, strict=False)
        # Daily files write DATEADDED as YYYYMMDD, the 15-minute feed as
        # YYYYMMDDHHMMSS.
        added_day = pl.when(added > 99_999_999).then(added // 1_000_000).otherwise(added)
        lo, hi = (int(d.strftime("%Y%m%d")) for d in _DATE_1920_WINDOW)
        return (pl.col("Day") < 19790101) & added_day.is_between(lo, hi)

    def apply(self, lf: pl.LazyFrame, ctx: FileContext) -> pl.LazyFrame:
        if not self._has_date_columns(lf):
            return lf
        applies = self._applies(lf, ctx)
        exprs = []
        if self.keep_original:
            schema = lf.collect_schema()
            exprs += [
                (
                    pl.when(self._condition()).then(pl.col(c)).otherwise(None)
                    if applies else pl.lit(None, dtype=schema[c])
                ).alias(f"{c}_original")
                for c in _DATE_1920_OFFSETS
            ]
        if applies:
            hit = self._condition()
            exprs += [
                pl.when(hit).then(pl.col(c) + offset).otherwise(pl.col(c)).alias(c)
                for c, offset in _DATE_1920_OFFSETS.items()
            ]
        return lf.with_columns(exprs) if exprs else lf

    def settings(self) -> dict:
        # Both errata rules are steps named "errata"; the rule name tells
        # them apart in the marker.
        return {"rule": "date_1920", **super().settings()}

    def counts(self, lf: pl.LazyFrame, ctx: FileContext) -> dict[str, pl.Expr]:
        names = lf.collect_schema().names()
        if not all(c in names for c in (*_DATE_1920_OFFSETS, "DATEADDED")):
            # A converted file pruned by converter.output_columns: the rule
            # can't run, and the cleaner warns once per run with the count.
            return {"errata.date_1920_skipped_files": pl.lit(1)}
        if not self._applies(lf, ctx):
            return {}
        return {"errata.date_1920": self._condition().sum()}


_EVENT_MARKERS = {"EventCode": ("---", "X"), "EventBaseCode": ("---", "X"),
                  "EventRootCode": ("--", "X")}


@dataclass(frozen=True)
class EventMarkers(Step):
    """
    Rows whose event code is "---"/"--" (the CAMEO null code: a matched
    verb pattern that generates no event) or "X" (undocumented). mode
    "keep" leaves them, only counting them for the audit; "drop" removes
    them, which is lossy.
    """

    mode: str = "keep"
    name = "errata"

    @property
    def lossy(self) -> bool:  # type: ignore[override]
        return self.mode == "drop"

    def settings(self) -> dict:
        return {"rule": "event_markers", **super().settings()}

    @staticmethod
    def _condition(lf: pl.LazyFrame) -> pl.Expr | None:
        schema = lf.collect_schema().names()
        parts = [
            pl.col(c).cast(pl.String).is_in(list(markers))
            for c, markers in _EVENT_MARKERS.items() if c in schema
        ]
        return pl.any_horizontal(parts).fill_null(False) if parts else None

    def apply(self, lf: pl.LazyFrame, ctx: FileContext) -> pl.LazyFrame:
        cond = self._condition(lf)
        if self.mode != "drop" or cond is None:
            return lf
        return lf.filter(~cond)

    def counts(self, lf: pl.LazyFrame, ctx: FileContext) -> dict[str, pl.Expr]:
        cond = self._condition(lf)
        return {} if cond is None else {f"errata.event_markers_{self.mode}": cond.sum()}


@dataclass(frozen=True)
class NormalizeStrings(Step):
    """
    Optional whitespace normalization of every string column. trim strips
    leading and trailing whitespace (" USA" -> "USA"); blank_to_null turns
    whitespace-only values (" ") into null. Around 2013 GDELT padded 11.3M
    Actor2Code values and wrote 3.2M as a single space, so an exact match on
    the code misses them. Lossy: GDELT's own spelling of the value is gone.
    """

    trim: bool = False
    blank_to_null: bool = False
    name = "normalize"
    lossy = True

    @staticmethod
    def _string_columns(lf: pl.LazyFrame) -> list[str]:
        schema = lf.collect_schema()
        return [c for c in schema.names() if schema[c] == pl.String]

    def apply(self, lf: pl.LazyFrame, ctx: FileContext) -> pl.LazyFrame:
        exprs = []
        for c in self._string_columns(lf):
            value = pl.col(c).str.strip_chars() if self.trim else pl.col(c)
            if self.blank_to_null:
                value = pl.when(pl.col(c).str.strip_chars() == "").then(None).otherwise(value)
            exprs.append(value.alias(c))
        return lf.with_columns(exprs) if exprs else lf

    def settings(self) -> dict:
        # Under the names clean.normalize.<dataset> uses, so a marker reads
        # like the configuration that produced it.
        return {"trim_strings": self.trim, "blank_to_null": self.blank_to_null}

    def counts(self, lf: pl.LazyFrame, ctx: FileContext) -> dict[str, pl.Expr]:
        columns = self._string_columns(lf)
        if not columns:
            return {}
        out = {}
        if self.trim:
            # Every value trimming changes, " " -> "" included, except those
            # blank_to_null turns into null instead; those are its count.
            def trimmed(c: str) -> pl.Expr:
                changed = pl.col(c) != pl.col(c).str.strip_chars()
                if self.blank_to_null:
                    changed = changed & (pl.col(c).str.strip_chars() != "")
                return changed.sum()

            out["normalize.trimmed"] = reduce(operator.add, [trimmed(c) for c in columns])
        if self.blank_to_null:
            out["normalize.blank_to_null"] = reduce(operator.add, [
                (pl.col(c).str.strip_chars() == "").sum() for c in columns
            ])
        return out


@dataclass(frozen=True)
class DeriveColumns(Step):
    """
    Optional columns computed from existing ones, added and never replacing
    anything. event_date adds EventDate, a real date parsed from Day (after
    errata, so the 1920 repair is in it). labels adds <column>_Label for
    each listed CAMEO-coded column, the code's name from the bundled code
    tables, matched case-insensitively; a code with no name gets null.
    `label_maps` holds, per column, those tables with upper-cased keys.
    """

    event_date: bool = False
    label_maps: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = ()
    name = "derive"
    lossy = False

    def apply(self, lf: pl.LazyFrame, ctx: FileContext) -> pl.LazyFrame:
        schema = lf.collect_schema().names()
        exprs = []
        if self.event_date and "Day" in schema:
            exprs.append(
                pl.col("Day").cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False)
                .alias("EventDate")
            )
        for column, pairs in self.label_maps:
            if column not in schema:
                continue
            exprs.append(
                pl.col(column).cast(pl.String).str.to_uppercase()
                .replace_strict(dict(pairs), default=None, return_dtype=pl.String)
                .alias(f"{column}_Label")
            )
        return lf.with_columns(exprs) if exprs else lf

    def settings(self) -> dict:
        # The column names, not the code tables behind them: the tables
        # would add tens of KB to every cleaned file's metadata.
        return {"event_date": self.event_date, "labels": [c for c, _ in self.label_maps]}

    def counts(self, lf: pl.LazyFrame, ctx: FileContext) -> dict[str, pl.Expr]:
        if not self.event_date or "Day" not in lf.collect_schema().names():
            return {}
        # Day values present but not a real date (a malformed record).
        parsed = pl.col("Day").cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False)
        return {"derive.event_date_invalid": (pl.col("Day").is_not_null() & parsed.is_null()).sum()}


def added_columns(declared: list[str]) -> list[str]:
    """
    Every column the clean stage can add to a dataset whose declared
    schema (config columns.<dataset>) is `declared`: date_1920's
    *_original columns, derive's EventDate, and one <column>_Label per
    CAMEO-coded column. None of them is in the declared schema, yet all
    are real columns of a cleaned file, so a reader that checks names
    against the declared schema (sample --mode filtered) accepts these
    too and keeps whichever the files actually have.
    """
    names = []
    if all(c in declared for c in _DATE_1920_OFFSETS):
        names += [f"{c}_original" for c in _DATE_1920_OFFSETS]
    if "Day" in declared:
        names.append("EventDate")
    names += [f"{c}_Label" for c in declared if cameo_codes.code_family_for_column(c)]
    return names


def ordered(steps: list[Step]) -> list[Step]:
    """steps sorted into STEP_ORDER; the sort is stable within one name."""
    return sorted(steps, key=lambda s: STEP_ORDER.index(s.name))
