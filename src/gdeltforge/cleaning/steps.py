"""
steps.py

The clean stage's steps, applied to each file in one fixed order,
STEP_ORDER, whatever order a configuration lists them in. A fixed order
means no configuration can produce an order-dependent result by accident
(for instance a null check that runs before whitespace-only strings
become null). docs/data-cleaning.md documents each step and the reasons
behind it.

Every step declares whether it is lossy: whether the cleaned output can
no longer tell what the converted input held. The --delete-source
safeguard and the dry-run report rely on that declaration.

Provides:
    - STEP_ORDER: the fixed order of step names
    - Step: base class
    - RequireColumns, ProjectColumns, NarrowFloat32: the stage's
      original operations, as steps
    - ordered: sort steps into STEP_ORDER
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

# errata: repairs of known GDELT errors. normalize: optional value
# normalization. require: drop rows missing a required value. derive:
# add columns. project: keep a column subset. narrow: smaller encodings.
STEP_ORDER = ("errata", "normalize", "require", "derive", "project", "narrow")


@dataclass(frozen=True)
class Step:
    """
    One step. `name` places it in STEP_ORDER; `lossy` says whether its
    output can no longer tell what its input held. apply() receives the
    frame as every earlier step left it, plus the source file's name for
    messages, and returns the frame for the next step.
    """

    name = ""
    lossy = False

    def apply(self, lf: pl.LazyFrame, file_name: str) -> pl.LazyFrame:
        raise NotImplementedError


@dataclass(frozen=True)
class RequireColumns(Step):
    """
    Drop rows with a null in any of `columns` (clean.columns_to_check).
    Lossy: dropped rows are gone from the output.
    """

    columns: tuple[str, ...] = ()
    name = "require"
    lossy = True

    def apply(self, lf: pl.LazyFrame, file_name: str) -> pl.LazyFrame:
        if not self.columns:
            # An empty list (the bundled default ships one per dataset) is
            # a deliberate no-op: every row survives. It must not reach
            # any_horizontal, which polars rejects for an empty list
            # ("cannot return empty fold").
            return lf
        schema_cols = lf.collect_schema().names()
        existing = [c for c in self.columns if c in schema_cols]
        missing = [c for c in self.columns if c not in schema_cols]
        if missing:
            logger.warning(f"{file_name}: Missing {len(missing)} column(s): {missing}")
        if not existing:
            # Configured columns, none present: the null check can't run
            # at all. Raising routes the file to "Files failed" and a
            # non-zero exit. Returning quietly used to report it as cleaned
            # at 100% retention, silently skipping the requested check.
            raise ValueError(
                f"{file_name}: none of the configured columns_to_check "
                f"{list(self.columns)} exist in this file's schema. There is "
                f"nothing to filter on."
            )
        return lf.filter(~pl.any_horizontal([pl.col(c).is_null() for c in existing]))


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

    def apply(self, lf: pl.LazyFrame, file_name: str) -> pl.LazyFrame:
        schema_cols = lf.collect_schema().names()
        missing = [c for c in self.columns if c not in schema_cols]
        if missing:
            # Warned by name: a typo here used to discard the column from
            # every run with no trace at any log level.
            logger.warning(
                f"{file_name}: output_columns names {len(missing)} "
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

    def apply(self, lf: pl.LazyFrame, file_name: str) -> pl.LazyFrame:
        schema = lf.collect_schema()
        targets = [c for c in self.columns if c in schema and schema[c].is_float()]
        if not targets:
            return lf
        return lf.with_columns([pl.col(c).cast(pl.Float32) for c in targets])


def ordered(steps: list[Step]) -> list[Step]:
    """steps sorted into STEP_ORDER; the sort is stable within one name."""
    return sorted(steps, key=lambda s: STEP_ORDER.index(s.name))
