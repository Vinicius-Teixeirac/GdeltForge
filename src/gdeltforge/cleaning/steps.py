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
    - ordered: sort steps into STEP_ORDER
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl

from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

# errata: repairs of known GDELT errors. normalize: optional value
# normalization. require: drop rows missing a required value. derive:
# add columns. project: keep a column subset. narrow: smaller encodings.
STEP_ORDER = ("errata", "normalize", "require", "derive", "project", "narrow")

# Bumped whenever an errata rule is added or changed, and recorded in the
# resumability fingerprint, so data cleaned under an older rule set is
# cleaned again on the next run.
ERRATA_VERSION = 1


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
    <column>_original on repaired rows (null elsewhere), in files whose
    period touches the window; with it the repair is not lossy.
    """

    keep_original: bool = True
    name = "errata"

    @property
    def lossy(self) -> bool:  # type: ignore[override]
        return not self.keep_original

    @staticmethod
    def _applies(lf: pl.LazyFrame, ctx: FileContext) -> bool:
        schema = lf.collect_schema().names()
        if not all(c in schema for c in (*_DATE_1920_OFFSETS, "DATEADDED")):
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
        if not self._applies(lf, ctx):
            return lf
        hit = self._condition()
        exprs = []
        if self.keep_original:
            exprs += [
                pl.when(hit).then(pl.col(c)).otherwise(None).alias(f"{c}_original")
                for c in _DATE_1920_OFFSETS
            ]
        exprs += [
            pl.when(hit).then(pl.col(c) + offset).otherwise(pl.col(c)).alias(c)
            for c, offset in _DATE_1920_OFFSETS.items()
        ]
        return lf.with_columns(exprs)

    def counts(self, lf: pl.LazyFrame, ctx: FileContext) -> dict[str, pl.Expr]:
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


def ordered(steps: list[Step]) -> list[Step]:
    """steps sorted into STEP_ORDER; the sort is stable within one name."""
    return sorted(steps, key=lambda s: STEP_ORDER.index(s.name))
