"""
crossref.py

Joins a sampled/filtered Events DataFrame against GKG, enriching each
event with the article-level context (themes, tone, people, organizations)
GDELT extracted from the news coverage of it.

Takes an already-materialized events_df (e.g. the output of `gdeltforge
sample`), not the full Events archive: joining against 542M rows would be
a different, much heavier operation than enriching a bounded sample, and
this matches the rest of the pipeline's "sample first, then work with the
sample" shape. Nothing stops a caller from passing the full archive
anyway (a large sample, or genuinely wanting an archive-scale join, are
both legitimate), so crossref_events_gkg_v1/_v2 both warn, via
warn_if_events_df_is_large, once events_df crosses a size where that
stops being cheap, without ever blocking it outright. The Mentions/GKG
side has the same shape of risk independent of events_df entirely: both
join functions also warn, via warn_if_directory_is_large, when the
configured Mentions/GKG directory itself has enough files that just
listing and opening them is a real cost.

start_date/end_date (all three functions, CLI: --start-date/--end-date)
narrow which files in the configured Mentions/GKG directories get listed
and opened at all, the same [start, end] overlap semantics scrape/
convert/filter already use, reusing their own filter_paths_by_date and
filename date parsers rather than a separate mechanism. This narrows the
corpus being joined against, not events_df, which is unaffected either
way: a Mentions row is timestamped by when it was recorded, not by its
event's DATEADDED (crossref_events_gkg_auto's docstring below has a real
example of a mention created a year after its event), so narrowing the
corpus by date is a real scope decision that can exclude a legitimate
late mention of an in-range event, not a risk-free filter. Left at their
None default, every file in the configured directory is still opened,
same as before this existed.

Two join strategies, because GKG 1.0 and GKG 2.1 relate to Events
differently (see docs/comparison.md):

    - crossref_events_gkg_v1: GKG 1.0 (and its separate Counts file) carry
      EventIds directly on each row, a comma-delimited string, so this
      is a direct join. Not expressible as a pyarrow filter-pushdown
      predicate (EventIds is a packed string, not a scalar column), so the
      GKG dataset is scanned with column projection only, and the id list
      is split/matched in memory.
    - crossref_events_gkg_v2: GKG 2.1 carries no event id at all, only the
      source article's URL, so this is a two-hop join through Mentions
      (the bridge table): Events -> Mentions on GlobalEventID, then
      Mentions -> GKG 2.1 on that URL. Both hops use real pyarrow filter
      pushdown, so neither the full Mentions nor the full GKG 2.1 archive
      is ever materialized, only the rows relevant to events_df.

Both preserve the underlying many-to-many structure rather than collapsing
it: one event can join to several GKG rows (several articles covered it),
and one GKG row/article can join to several events (it covered several).

Both join functions also warn (not error) when some or all of events_df
predates the target GKG generation's real coverage start
(GKG_V1_COVERAGE_START / GKG_V2_COVERAGE_START), since those events cannot
find a match no matter how anything is configured: the target dataset has
no rows from before it existed.

A third function, crossref_events_gkg_auto, attempts every eligible event
against both GKG generations instead of requiring the caller to pick one
for the whole sample: DATEADDED only decides whether an event is within
either generation's coverage window at all (before GKG_V1_COVERAGE_START,
neither has any data), not which single path is allowed to match it. A
Mentions row is timestamped by when it was created, not by its event's
DATEADDED, so an event from the GKG 1.0 era can still have a real GKG 2.1
match created much later, and GKG 1.0 remains live today, so a recent
event isn't guaranteed to be GKG-2.1-only either. A sample spanning both
eras (e.g. covering the 2013-2015 window where only GKG 1.0 exists,
alongside more recent events) gets the richest available enrichment for
every event this way, including an event that genuinely matches both.

Provides:
    - crossref_events_gkg_v1
    - crossref_events_gkg_v2
    - crossref_events_gkg_auto
"""

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Literal

import polars as pl
from tqdm import tqdm

from gdeltforge.scraping.scraper import (
    filter_paths_by_date,
    parse_gdelt_gkg_v1_file_date,
    parse_gdeltv2_file_date,
)
from gdeltforge.utils.io import clearer_dataset_errors
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

# The column(s) each dataset must retain for crossref to be possible at
# all: the join key on the Events side, and each dataset's half of the
# bridge on the GKG/Mentions side. Single source of truth for both the
# _require_column checks below (which enforce it here, with a clear
# error) and filter.output_columns' proactive warning (which flags it
# earlier, at filter time, before a sample/crossref run downstream
# discovers the column is already gone).
REQUIRED_JOIN_COLUMNS: dict[str, tuple[str, ...]] = {
    "gdelt_event": ("GlobalEventID",),
    "gdelt_gkg_v1": ("EventIds",),
    "gdelt_gkg_v1_counts": ("EventIds",),
    "gdelt_gkg_v2": ("V2DOCUMENTIDENTIFIER",),
    "gdelt_mentions": ("GLOBALEVENTID", "MentionIdentifier"),
}

# Mentions columns crossref_events_gkg_v2 carries through as payload
# (renamed "Mention_<name>") when present, but doesn't require the way
# REQUIRED_JOIN_COLUMNS's "gdelt_mentions" entry does: neither one
# participates in matching a mention to an event or an article, so a
# Mentions dataset missing one just means that field is absent from the
# output, not a failed join.
OPTIONAL_MENTIONS_PAYLOAD_COLUMNS: tuple[str, ...] = ("MentionTimeDate", "Confidence")

# crossref_events_gkg_v2's on_duplicate_document values. GKG 2.1 can carry
# more than one record for the same V2DOCUMENTIDENTIFIER: confirmed for
# real against a live GKG 2.1 pull, a URL can be independently crawled
# years apart (a tag/listing page whose content changed between visits,
# not just "the same article reprocessed"), so the two records can hold
# genuinely different content, not a stale duplicate of the same one.
# Picking a single winner is a real editorial choice, not noise removal,
# so it defaults to keeping every record rather than silently discarding
# one on the caller's behalf; "latest"/"earliest" are opt-in for whoever
# specifically wants a single-row-per-URL join instead:
#   "all"      - keep every record (default); a shared URL contributes
#                one row per (event, mention, GKG record) instead of one
#   "latest"   - keep only the chronologically most recent record
#   "earliest" - keep only the chronologically first record
_ON_DUPLICATE_DOCUMENT_MODES = frozenset({"latest", "earliest", "all"})

# When each GKG generation's real data actually begins, as YYYYMMDD ints
# matching Events' own DATEADDED format. Verified against GDELT's real
# file listings, not the codebook alone: GKG 1.0's earliest published file
# is 20130401.gkg.csv.zip; GDELT 2.0 (GKG 2.1, the 15-minute Events feed,
# and Mentions, all launched together) first appears at 2015-02-18
# (20150218230000 is the earliest file in gdeltv2/masterfilelist.txt for
# all three). Events sampled from before the relevant date cannot find a
# match through that crossref path no matter how columns are configured,
# since the target dataset has no rows at all for that period.
GKG_V1_COVERAGE_START = 20130401
GKG_V2_COVERAGE_START = 20150218

# Above this many events, warn_if_events_df_is_large fires. Measured, not
# guessed: building crossref's own join-key set (set(events_df["Global
# EventID"]) -> list() -> pyarrow's isin() filter, the exact sequence
# crossref_events_gkg_v1/_v2 both do) against synthetic int64 ids, true
# peak memory via tracemalloc:
#   1,000,000 events  -> ~100 MB,  ~1s
#   10,000,000 events -> ~800 MB,  ~5s
#   50,000,000 events -> ~5.2 GB, ~13s
# and that's before the archive scan itself, which opens every file in
# the configured Mentions/GKG directory regardless of events_df size.
# 1M is comfortably past where this is still cheap (the point of the
# warning is "you likely passed the full archive, not a sample"), not
# the point where it becomes expensive.
_LARGE_EVENTS_JOIN_WARNING_THRESHOLD = 1_000_000

# Above this many files, warn_if_directory_is_large fires for a
# configured Mentions/GKG directory. Measured, not guessed, against real
# local data: listing and constructing a pyarrow dataset over 3,127 real
# GKG 2.1 files took 0.20s; over 33,303 real Mentions files, 2.47s, a
# roughly linear ~75 microseconds/file (confirmed against two real,
# differently-sized directories, not a single data point). Extrapolated
# to the full historical GKG 2.1/Mentions archive (~385,728 files, see
# docs/configuration.md's "Capacity planning" section for where that
# number comes from), that's ~29s just to list and open every file,
# before a single row is read or any events-side filter applies. 50,000
# sits comfortably above the measured 33,303-file real Mentions
# directory above (a real, unremarkable local archive) and comfortably
# below where the listing cost alone is clearly worth a heads-up.
_LARGE_GKG_DIRECTORY_WARNING_THRESHOLD = 50_000


def _list_files(
    folder: str,
    date_parser: Callable[[str], tuple[date | None, date | None]],
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[Path]:
    # Sorted explicitly: Path.glob's return order is filesystem-dependent,
    # not guaranteed sorted, and pyarrow reads a multi-file dataset in the
    # order the file list is given. crossref_events_gkg_v2's "keep the
    # most recently reprocessed article" dedup (drop_duplicates(keep=
    # "last")) depends on that order matching each file's real position
    # in time. On NTFS, glob happened to come back alphabetical, which
    # for GDELT's YYYYMMDDHHMMSS filenames is also chronological, so this
    # worked by coincidence in local testing; on ext4 (GitHub Actions'
    # Linux runners) it doesn't, so the wrong, stale GKG record could
    # silently win the dedup instead of erroring. Sorting makes the file
    # order deterministic and matches filename order on every platform.
    files = sorted(Path(folder).glob("*.parquet"))
    # No-ops when both bounds are None, same as scrape/convert/filter's
    # own use of this function: a file whose name doesn't carry a
    # parseable date is kept regardless (see filter_paths_by_date's
    # docstring), rather than silently dropped just because a range was
    # given.
    return filter_paths_by_date(files, start_date, end_date, date_parser=date_parser)


def _dataset(
    folder: str,
    date_parser: Callable[[str], tuple[date | None, date | None]],
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.LazyFrame:
    files = _list_files(folder, date_parser, start_date, end_date)
    if not files:
        raise FileNotFoundError(
            f"No parquet files found in {folder}"
            + (
                f" within [{start_date} - {end_date}]"
                if start_date or end_date
                else ""
            )
        )
    # polars' scan_parquet infers its schema from a single file rather
    # than the union of every file's schema the way pyarrow.dataset.
    # dataset() does, confirmed directly: a column present in some files
    # and absent from others (exactly OPTIONAL_MENTIONS_PAYLOAD_COLUMNS'
    # reason for existing, e.g. an older Mentions file predating
    # Confidence/MentionTimeDate) raises "extra column ... outside of
    # expected schema" for whichever file doesn't match the schema
    # scan_parquet happened to infer, regardless of file order.
    # missing_columns="insert" alone only null-fills a column the SCHEMA
    # has but a given file lacks; it does nothing for a column a LATER
    # file has that the inferred schema didn't already include. Reading
    # every file's own footer schema first (cheap, metadata-only, the
    # same order of cost pyarrow.dataset's own schema unification already
    # paid) and passing the union explicitly as schema= reproduces that
    # unification regardless of which file scan_parquet would otherwise
    # have picked.
    schema: dict[str, pl.DataType] = {}
    for f in files:
        for name, dtype in pl.read_parquet_schema(f).items():
            schema.setdefault(name, dtype)
    return pl.scan_parquet(files, schema=schema, missing_columns="insert")


def _validate_columns(columns: set[str] | None, available: list[str]) -> set[str] | None:
    if columns is None:
        return None
    invalid = columns - set(available)
    if invalid:
        raise ValueError(f"Invalid columns: {invalid}")
    return columns


def _require_column(df_columns, name: str, df_desc: str) -> None:
    if name not in df_columns:
        raise ValueError(f"{df_desc} must include a {name!r} column")


def warn_if_output_columns_drops_join_key(
    logger, stage: str, dataset: str, output_columns: list[str] | None
) -> None:
    """
    Shared by convert.py's run_converter and filter.py's run_filter, both
    of which expose an output_columns knob that can prune away a column
    this module's REQUIRED_JOIN_COLUMNS says a dataset needs. Nothing
    about picking a lean column set for disk/CPU reasons hints that one
    of those columns is also load-bearing for a later `crossref` run;
    _require_column above already raises a clear error if the key is
    actually missing at join time, but by then convert, filter, and
    possibly sample have all already run to completion without it, only
    for the failure to surface downstream. Warn here instead, at the
    point output_columns is configured, while it's still cheap to fix.

    logger is passed in rather than used from this module, so the
    warning is correctly attributed to whichever stage actually emitted
    it. Only fires when output_columns is actually set (None means every
    column survives, so nothing to warn about) and the dataset is one
    crossref cares about at all (Events, both GKG generations, Mentions).
    """
    if output_columns is None:
        return
    required = REQUIRED_JOIN_COLUMNS.get(dataset)
    if not required:
        return
    missing = [c for c in required if c not in output_columns]
    if missing:
        logger.warning(
            f"{stage}.output_columns.{dataset} omits {missing}, required for "
            f"`gdeltforge crossref` to join this dataset. {stage.capitalize()} will "
            f"proceed, but crossref will fail on this output unless {missing} is added back."
        )


def warn_if_events_predate_gkg_coverage(
    gkg_label: str, coverage_start: int, events_df: pl.DataFrame
) -> None:
    """
    Warn (not error) when some or all of events_df predates a GKG
    generation's real coverage start (GKG_V1_COVERAGE_START /
    GKG_V2_COVERAGE_START above): those specific events cannot find a
    match through this crossref path no matter what, since the target
    dataset simply has no rows from before it existed.

    Checked against DATEADDED, not Day: Day is when an event is reported
    to have occurred, which can be far in the past for retrospective
    reporting (a 2003 event can appear in a 2013 daily file), while
    DATEADDED is when GDELT actually processed the record, matching the
    daily file's own date by construction. Mentions/GKG rows are
    generated from that same processing pass, so DATEADDED is what
    actually determines whether a corresponding GKG/Mentions record
    could exist, not the event's own reported date. Silently skipped if
    events_df has no DATEADDED column (e.g. a sample built with
    --columns that excluded it): this is a diagnostic on top of the
    join, not something crossref itself depends on.
    """
    if "DATEADDED" not in events_df.columns:
        return
    date_added = events_df["DATEADDED"].drop_nulls()
    if date_added.is_empty():
        return
    total = len(date_added)
    too_old = int((date_added < coverage_start).sum())
    if too_old == 0:
        return
    if too_old == total:
        logger.warning(
            f"All {total} sampled event(s) have DATEADDED before {coverage_start}, "
            f"when {gkg_label} coverage begins; this crossref will find nothing."
        )
    else:
        logger.warning(
            f"{too_old} of {total} sampled event(s) have DATEADDED before "
            f"{coverage_start}, when {gkg_label} coverage begins; those specific "
            f"events cannot find a match through this crossref path regardless of "
            f"configuration."
        )


def warn_if_events_df_is_large(events_df: pl.DataFrame) -> None:
    """
    Warn (not error) when events_df looks like the full Events archive
    rather than a bounded sample. This module's docstring already says
    crossref is designed for "an already-materialized events_df ... not
    the full Events archive", but nothing previously enforced that or
    even flagged it: --events happily accepts a directory of files (or
    one huge one) and silently proceeds.

    Both join paths build an in-memory join-key set from GlobalEventID
    (set() -> list() -> a pyarrow isin() filter) before touching the
    Mentions/GKG archive at all, and that step alone was measured (real
    memory via tracemalloc, not estimated) at ~100 MB/1s per million
    events, ~800 MB/5s at 10M, ~5.2 GB/13s at 50M. The archive scan on
    top of that opens every file in the configured Mentions/GKG
    directory regardless of events_df size, since pyarrow's filter
    pushdown prunes rows within a file, not which files get opened.

    Never blocks: a genuine archive-scale join is a real, supported use
    case, not a mistake by definition, and this can't tell the two
    apart. It can only flag that the row count is the kind that usually
    means "forgot to sample first" and say what that costs.
    """
    n = len(events_df)
    if n <= _LARGE_EVENTS_JOIN_WARNING_THRESHOLD:
        return
    logger.warning(
        f"Cross-referencing {n:,} events. crossref is designed for a bounded "
        f"sample (e.g. the output of `gdeltforge sample`), not the full Events "
        f"archive: it scans every file in the configured Mentions/GKG directory "
        f"regardless of events_df size, and just building the join key set "
        f"measured roughly 100 MB and a second of extra memory/CPU per million "
        f"events at this scale (10M events: ~800 MB; 50M: ~5.2 GB), before that "
        f"scan even starts. If this wasn't intentional, sample first with "
        f"`gdeltforge sample` instead of passing the full archive."
    )


def warn_if_directory_is_large(
    folder: str,
    label: str,
    date_parser: Callable[[str], tuple[date | None, date | None]],
    start_date: date | None = None,
    end_date: date | None = None,
) -> None:
    """
    Warn (not error) when a configured Mentions/GKG directory itself has
    enough files that listing and opening them is a real, measurable
    cost, independent of events_df's own size (see
    warn_if_events_df_is_large for that separate, events-side concern).
    crossref does this on every single run, not once and cached: pyarrow's
    filter pushdown narrows which rows get read within a file, not which
    files get opened at all, so the file count itself is what this
    tracks. Counts the same post-start_date/end_date file list
    crossref_events_gkg_v1/_v2 actually open, not the raw directory: once
    those are narrowed, that's the real lever reducing this, on top of
    pointing paths.* at a smaller, already-narrowed directory.

    Deliberately a plain file count, not a byte total: the cost this
    flags is per-file listing/opening overhead (open a footer, read a
    schema), which doesn't scale with how much data is inside each file,
    unlike warn_if_events_df_is_large's memory concern.
    """
    n = len(_list_files(folder, date_parser, start_date, end_date))
    if n <= _LARGE_GKG_DIRECTORY_WARNING_THRESHOLD:
        return
    logger.warning(
        f"{label} directory {folder!r} has {n:,} files"
        f"{' in the given date range' if start_date or end_date else ''}. "
        f"crossref lists and opens every one of them on each run, regardless "
        f"of how selective the join ends up being (~75 microseconds/file "
        f"measured on real GKG 2.1/Mentions data, so roughly {n * 75e-6:.0f}s "
        f"here just to list and open them, before a single row is read). "
        f"--start-date/--end-date narrows which files in this directory get "
        f"touched at all; pointing paths.* at a smaller, already-narrowed "
        f"directory reduces it further."
    )


def crossref_events_gkg_v1(
    events_df: pl.DataFrame,
    gkg_folder: str,
    gkg_columns: list[str],
    columns: set[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.DataFrame:
    """
    Direct join: events_df x a GKG 1.0-family dataset (the main file or
    its separate Counts file, both carry EventIds the same way, so this
    works for either) on EventIds, a comma-delimited string (confirmed
    2026-08-04 against a real downloaded file; the codebook doesn't spell
    out the exact delimiter).

    GKG-side output columns are prefixed "GKG_" to avoid colliding with
    an identically-named Events column (NumArticles exists on both sides).

    start_date/end_date (CLI: --start-date/--end-date) narrow which files
    in gkg_folder get listed and opened at all, independent of
    events_df; see the module docstring for what this does and doesn't
    affect.

    Returns one row per (event, GKG row) pair. An event with no GKG match
    contributes no rows; a GKG row naming several events contributes one
    row per event, not one collapsed row.
    """
    _require_column(events_df.columns, REQUIRED_JOIN_COLUMNS["gdelt_event"][0], "events_df")
    _require_column(gkg_columns, REQUIRED_JOIN_COLUMNS["gdelt_gkg_v1"][0], "gkg_columns")
    warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)
    warn_if_events_df_is_large(events_df)
    warn_if_directory_is_large(
        gkg_folder, "GKG 1.0", parse_gdelt_gkg_v1_file_date, start_date, end_date
    )

    columns = _validate_columns(columns, gkg_columns)
    read_columns = list((columns if columns is not None else set(gkg_columns)) | {"EventIds"})

    event_id_col = events_df["GlobalEventID"].cast(pl.Int64).cast(pl.Utf8)
    event_id_set = set(event_id_col.to_list())
    events_side = events_df.with_columns(event_id_col.alias("_GlobalEventID_str"))

    matches: list[pl.DataFrame] = []
    # Wrapped from dataset construction onward: _dataset() itself can
    # raise (every file's footer schema is read there, to build the
    # union schema= scan_parquet needs), not only the later batch
    # collection.
    with clearer_dataset_errors(f"GKG 1.0 dataset in {gkg_folder}"):
        lf = _dataset(
            gkg_folder, parse_gdelt_gkg_v1_file_date, start_date, end_date
        ).select(read_columns)

        for df_batch in tqdm(
            lf.collect_batches(chunk_size=64_000), desc="Cross-referencing GKG 1.0"
        ):
            if df_batch.is_empty():
                continue

            original_columns = df_batch.columns
            # explode()'s empty_as_null keyword doesn't exist until a
            # polars release newer than this project's own declared
            # minimum (polars>=1.34): confirmed directly, installing
            # exactly 1.34.0 raises "unexpected keyword argument". Its
            # only effect here would be whether a genuinely empty EventIds
            # list (a blank/null source field, split into []) explodes to
            # a null or an empty-string row; either way the is_in() filter
            # below drops it, so filtering an empty list out before
            # exploding at all reaches the same result without needing
            # the keyword. A non-empty list with a blank entry (e.g. a
            # trailing comma splitting "5," into ["5", ""]) isn't affected
            # by this filter at all and explodes normally either way.
            exploded = (
                df_batch
                .with_columns(
                    pl.col("EventIds").fill_null("").str.split(",")
                    .alias("_matched_event_id")
                )
                .filter(pl.col("_matched_event_id").list.len() > 0)
                .explode("_matched_event_id")
                .with_columns(pl.col("_matched_event_id").str.strip_chars())
                .filter(pl.col("_matched_event_id").is_in(event_id_set))
            )

            if exploded.is_empty():
                continue

            gkg_side = exploded.rename({c: f"GKG_{c}" for c in original_columns})
            matches.append(
                events_side.join(
                    gkg_side, left_on="_GlobalEventID_str", right_on="_matched_event_id",
                    how="inner", coalesce=False,
                )
            )

    if not matches:
        return pl.DataFrame()

    result = pl.concat(matches)
    return result.drop(["_GlobalEventID_str", "_matched_event_id"])


def crossref_events_gkg_v2(
    events_df: pl.DataFrame,
    mentions_folder: str,
    gkg_v2_folder: str,
    gkg_v2_columns: list[str],
    columns: set[str] | None = None,
    on_duplicate_document: Literal["latest", "earliest", "all"] = "all",
    dedupe_mentions: bool = False,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.DataFrame:
    """
    Two-hop join for GKG 2.1, which carries no event id:
    Events -[GlobalEventID]-> Mentions -[document URL]-> GKG 2.1.

    GKG-side output columns are prefixed "GKG_"; the Mentions bridge
    fields carried through are prefixed "Mention_" (see
    OPTIONAL_MENTIONS_PAYLOAD_COLUMNS: present ones are carried through,
    a missing one just means its Mention_<name> column isn't in the
    output, not a failed join, unlike REQUIRED_JOIN_COLUMNS's
    GLOBALEVENTID/MentionIdentifier). Returns one row per
    (event, article, GKG record) combination: an event mentioned by
    several distinct articles contributes several rows, and an article
    covering several events contributes one row per event, not one
    collapsed row. See docs/crossref-join-semantics.md for real-data
    numbers on how often each of these actually happens.

    on_duplicate_document controls what happens when GKG 2.1 carries more
    than one record for the same V2DOCUMENTIDENTIFIER (a URL crawled more
    than once, e.g. a tag/listing page whose content changed between
    visits, not always just a stale reprocessing of the same article):
      - "all" (default): keep every record; the shared URL then
        contributes one row per (event, article, GKG record) instead of
        one. Nothing is silently discarded on the caller's behalf.
      - "latest": keep only the chronologically most recent record.
      - "earliest": keep only the chronologically first record.
    This only matters for the rare URL that genuinely has more than one
    GKG record; every other URL is unaffected regardless of the setting.

    dedupe_mentions controls a separate, more common source of repeated
    rows: Mentions records one row per sentence that references an
    event, so an event quoted in several sentences of the same article
    produces several raw Mentions rows for that one (event, article)
    relationship. By default (False) every raw row is kept, so no
    (event, article) granularity is ever silently collapsed away. Set to
    True to instead collapse those rows into one per (event, article),
    keeping the highest-Confidence version when Confidence is available;
    a new Mention_Count column then records how many raw rows collapsed
    into it, so "how many times was this event mentioned in this
    article" survives explicitly rather than being read off row count.

    start_date/end_date (CLI: --start-date/--end-date) narrow which files
    in both mentions_folder and gkg_v2_folder get listed and opened at
    all, independent of events_df; see the module docstring for what
    this does and doesn't affect.
    """
    if on_duplicate_document not in _ON_DUPLICATE_DOCUMENT_MODES:
        raise ValueError(
            f"on_duplicate_document must be one of {sorted(_ON_DUPLICATE_DOCUMENT_MODES)}, "
            f"got {on_duplicate_document!r}"
        )

    _require_column(events_df.columns, REQUIRED_JOIN_COLUMNS["gdelt_event"][0], "events_df")
    _require_column(
        gkg_v2_columns, REQUIRED_JOIN_COLUMNS["gdelt_gkg_v2"][0], "gkg_v2_columns"
    )
    warn_if_events_predate_gkg_coverage(
        "GDELT 2.0 (GKG 2.1 / Mentions)", GKG_V2_COVERAGE_START, events_df
    )
    warn_if_events_df_is_large(events_df)
    warn_if_directory_is_large(
        mentions_folder, "Mentions", parse_gdeltv2_file_date, start_date, end_date
    )
    warn_if_directory_is_large(
        gkg_v2_folder, "GKG 2.1", parse_gdeltv2_file_date, start_date, end_date
    )

    columns = _validate_columns(columns, gkg_v2_columns)
    read_gkg_columns = list(
        (columns if columns is not None else set(gkg_v2_columns)) | {"V2DOCUMENTIDENTIFIER"}
    )

    event_id_col = events_df["GlobalEventID"].cast(pl.Int64)
    event_id_set = set(event_id_col.to_list())

    # Hop 1: Mentions, filter-pushdown on GLOBALEVENTID, a real scalar
    # column unlike GKG 1.0's comma-packed EventIds, so this narrows
    # the scan at the row-group level instead of reading everything.
    # Wrapped from dataset construction onward: _dataset() itself can
    # raise (every file's footer schema is read there), and so can the
    # collect_schema() access just below it, not only the final
    # .collect() read further down.
    with clearer_dataset_errors(f"Mentions dataset in {mentions_folder}"):
        mentions_lf = _dataset(
            mentions_folder, parse_gdeltv2_file_date, start_date, end_date
        )
        mentions_schema_names = mentions_lf.collect_schema().names()
        for required in REQUIRED_JOIN_COLUMNS["gdelt_mentions"]:
            _require_column(mentions_schema_names, required, "mentions_folder")

        # Same existing/missing split as columns_to_check and output_columns
        # elsewhere in the pipeline: read whichever optional payload columns
        # this Mentions dataset actually has, and simply carry through fewer
        # Mention_* fields for the ones it doesn't, rather than failing.
        mentions_payload_columns = [
            c for c in OPTIONAL_MENTIONS_PAYLOAD_COLUMNS if c in mentions_schema_names
        ]
        mentions_read_columns = (
            list(REQUIRED_JOIN_COLUMNS["gdelt_mentions"]) + mentions_payload_columns
        )

        logger.info(f"Cross-referencing {len(event_id_set)} event(s) against Mentions...")
        bridge_df = (
            mentions_lf
            .filter(pl.col("GLOBALEVENTID").is_in(event_id_set))
            .select(mentions_read_columns)
            .collect()
        )

    if bridge_df.is_empty():
        return pl.DataFrame()

    if dedupe_mentions:
        # Mentions records one row per sentence that references an
        # event, so an event quoted in several sentences of one article
        # produces several raw rows here for what is really a single
        # (event, article) relationship. Confirmed on real data: ~1.9%
        # of (event, article) pairs have more than one raw row, and
        # where they do, the rows aren't always identical: Confidence
        # differs across them roughly 23% of the time (different
        # sentences can be extracted with different confidence), so
        # this is picking a representative row, not collapsing true
        # duplicates. Mention_Count preserves how many raw rows were
        # behind it; sorting by Confidence first (stable, so ties keep
        # whichever row the read happened to return first) means the
        # kept row is the highest-confidence one when Confidence is
        # available at all.
        mention_counts = (
            bridge_df.group_by(["GLOBALEVENTID", "MentionIdentifier"], maintain_order=False)
            .len()
            .rename({"len": "Mention_Count"})
        )
        if "Confidence" in bridge_df.columns:
            bridge_df = bridge_df.sort(
                "Confidence", descending=True, nulls_last=True, maintain_order=True
            )
        bridge_df = bridge_df.unique(
            subset=["GLOBALEVENTID", "MentionIdentifier"], keep="first", maintain_order=True
        )
        bridge_df = bridge_df.join(
            mention_counts, on=["GLOBALEVENTID", "MentionIdentifier"], how="left"
        )

    urls = set(bridge_df["MentionIdentifier"].drop_nulls().unique().to_list())
    if not urls:
        return pl.DataFrame()

    # Hop 2: GKG 2.1, filter-pushdown on the document URL: again a real
    # predicate pushed down into the scan, so only rows for articles
    # actually mentioning one of these events get read off disk.
    logger.info(f"Cross-referencing {len(urls)} article URL(s) against GKG 2.1...")
    with clearer_dataset_errors(f"GKG 2.1 dataset in {gkg_v2_folder}"):
        gkg_df = (
            _dataset(gkg_v2_folder, parse_gdeltv2_file_date, start_date, end_date)
            .filter(pl.col("V2DOCUMENTIDENTIFIER").is_in(urls))
            .select(read_gkg_columns)
            .collect()
        )

    if gkg_df.is_empty():
        return pl.DataFrame()

    if on_duplicate_document == "latest":
        gkg_df = gkg_df.unique(subset=["V2DOCUMENTIDENTIFIER"], keep="last", maintain_order=True)
    elif on_duplicate_document == "earliest":
        gkg_df = gkg_df.unique(subset=["V2DOCUMENTIDENTIFIER"], keep="first", maintain_order=True)
    # "all": no dedup, every GKG record for a shared URL flows through.
    gkg_df = gkg_df.rename({c: f"GKG_{c}" for c in gkg_df.columns})

    bridge_df = bridge_df.rename({c: f"Mention_{c}" for c in mentions_payload_columns})

    joined = bridge_df.join(
        gkg_df, left_on="MentionIdentifier", right_on="GKG_V2DOCUMENTIDENTIFIER",
        how="inner", coalesce=False,
    )
    if joined.is_empty():
        return pl.DataFrame()

    events_side = events_df.with_columns(event_id_col.alias("_GlobalEventID_int64"))
    result = events_side.join(
        joined, left_on="_GlobalEventID_int64", right_on="GLOBALEVENTID",
        how="inner", coalesce=False,
    )
    return result.drop(["_GlobalEventID_int64", "GLOBALEVENTID", "MentionIdentifier"])


def crossref_events_gkg_auto(
    events_df: pl.DataFrame,
    gkg_v1_folder: str,
    gkg_v1_columns: list[str],
    mentions_folder: str,
    gkg_v2_folder: str,
    gkg_v2_columns: list[str],
    v1_columns: set[str] | None = None,
    v2_columns: set[str] | None = None,
    on_duplicate_document: Literal["latest", "earliest", "all"] = "all",
    dedupe_mentions: bool = False,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.DataFrame:
    """
    Attempts every sampled event against both GKG generations rather than
    picking exactly one per event by DATEADDED, so a caller doesn't have
    to know GKG's coverage history or which generation is "supposed to"
    cover a given event.

    This used to route each event to exactly one path, on the reasoning
    that an event with DATEADDED before GKG_V2_COVERAGE_START (2015-02-18)
    could only ever match through GKG 1.0. That reasoning doesn't hold:
    a Mentions row is timestamped by when the mention was created, not by
    the event's own DATEADDED, and confirmed for real against live data,
    a Mentions row can reference an event from well over a year before it
    (a 2019-origin event was referenced by a real Mentions row dated
    2020). Routing that event to GKG 1.0 only meant a real GKG 2.1 match
    was never even attempted. The same asymmetry runs the other
    direction too: GKG 1.0 remains live and daily-published today (it
    never stopped when GKG 2.1 launched), so a recent event isn't
    guaranteed to be GKG-2.1-only either. Every event within either
    generation's coverage window (DATEADDED >= GKG_V1_COVERAGE_START,
    2013-04-01) is now attempted against both; a real match through
    either path is kept, and an event that genuinely matches both
    contributes one row per path, not one merged or arbitrarily-chosen
    row. Events before GKG_V1_COVERAGE_START are still skipped and
    logged: that's a genuine absence of any data at all, not a routing
    choice, so trying either path for them would only waste the attempt.

    This is the one entry point in the module that genuinely needs
    events_df to carry DATEADDED, since it's what decides eligibility;
    raises rather than silently skipping the check if it's absent,
    unlike the optional, best-effort warning the two single-version
    functions run.

    Returns a single DataFrame, both paths' results concatenated with a
    new "CrossrefSource" column ("v1" or "v2") marking which one
    produced each row. The two schemas' GKG-side columns don't overlap
    (GKG 1.0's 11 fields vs GKG 2.1's 27, different names throughout,
    e.g. THEMES vs V1THEMES), so this never tries to unify them: a
    concatenated row simply carries NaN for whichever set of GKG-side
    columns its source path didn't produce, rather than silently
    misaligning two incompatible schemas into one.

    v1_columns/v2_columns restrict each path's own GKG-side output
    independently (each validated against that path's own schema, same
    as the "columns" parameter on crossref_events_gkg_v1/_v2 directly);
    there is no single "columns" covering both, since a column name
    meaningful for one schema is usually meaningless for the other.

    on_duplicate_document and dedupe_mentions are forwarded to the GKG
    2.1 path unchanged; see crossref_events_gkg_v2's docstring. GKG 1.0
    has no analogous duplicate-document or per-sentence-mention step
    (it joins directly on EventIds, no Mentions bridge involved), so
    neither setting affects the v1 path.

    start_date/end_date (CLI: --start-date/--end-date) are forwarded to
    both paths, narrowing which files in gkg_v1_folder, mentions_folder,
    and gkg_v2_folder get listed and opened at all; see the module
    docstring for what this does and doesn't affect.
    """
    _require_column(events_df.columns, REQUIRED_JOIN_COLUMNS["gdelt_event"][0], "events_df")
    if "DATEADDED" not in events_df.columns:
        raise ValueError(
            "events_df must include a 'DATEADDED' column: crossref_events_gkg_auto "
            "needs it to determine which events are within either GKG generation's "
            "coverage window at all. Call crossref_events_gkg_v1 or "
            "crossref_events_gkg_v2 directly instead if DATEADDED isn't available "
            "in this sample."
        )

    date_added = events_df["DATEADDED"]

    too_old = date_added < GKG_V1_COVERAGE_START
    if too_old.any():
        logger.warning(
            f"{int(too_old.sum())} of {len(events_df)} sampled event(s) have DATEADDED "
            f"before {GKG_V1_COVERAGE_START}, when GKG 1.0 coverage begins; neither GKG "
            f"generation has any data for them, so crossref_events_gkg_auto skips them."
        )

    eligible_mask = date_added >= GKG_V1_COVERAGE_START
    eligible_events = events_df.filter(eligible_mask)

    results: list[pl.DataFrame] = []

    if not eligible_events.is_empty():
        logger.info(
            f"crossref_events_gkg_auto: attempting {len(eligible_events)} event(s) "
            f"against both GKG 1.0 and GKG 2.1; a DATEADDED before "
            f"{GKG_V2_COVERAGE_START} does not rule out a real GKG 2.1 match, so "
            f"neither path is skipped based on it alone."
        )
        v1_result = crossref_events_gkg_v1(
            eligible_events, gkg_v1_folder, gkg_v1_columns, columns=v1_columns,
            start_date=start_date, end_date=end_date,
        )
        if not v1_result.is_empty():
            results.append(v1_result.with_columns(pl.lit("v1").alias("CrossrefSource")))

        v2_result = crossref_events_gkg_v2(
            eligible_events, mentions_folder, gkg_v2_folder, gkg_v2_columns,
            columns=v2_columns,
            on_duplicate_document=on_duplicate_document,
            dedupe_mentions=dedupe_mentions,
            start_date=start_date, end_date=end_date,
        )
        if not v2_result.is_empty():
            results.append(v2_result.with_columns(pl.lit("v2").alias("CrossrefSource")))

    if not results:
        return pl.DataFrame()
    return pl.concat(results, how="diagonal")
