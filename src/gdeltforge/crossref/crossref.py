"""
crossref.py

Joins a sampled/filtered Events DataFrame against GKG, enriching each
event with the article-level context (themes, tone, people, organizations)
GDELT extracted from the news coverage of it.

Takes an already-materialized events_df (e.g. the output of `gdeltforge
sample`), not the full Events archive: joining against 542M rows would be
a different, much heavier operation than enriching a bounded sample, and
this matches the rest of the pipeline's "sample first, then work with the
sample" shape.

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

Provides:
    - crossref_events_gkg_v1
    - crossref_events_gkg_v2
"""

from pathlib import Path

import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds
from tqdm import tqdm

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


def _dataset(folder: str) -> ds.Dataset:
    files = list(Path(folder).glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {folder}")
    return ds.dataset(files, format="parquet")


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
    gkg_label: str, coverage_start: int, events_df: pd.DataFrame
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
    date_added = events_df["DATEADDED"].dropna()
    if date_added.empty:
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


def crossref_events_gkg_v1(
    events_df: pd.DataFrame,
    gkg_folder: str,
    gkg_columns: list[str],
    columns: set[str] | None = None,
) -> pd.DataFrame:
    """
    Direct join: events_df x a GKG 1.0-family dataset (the main file or
    its separate Counts file, both carry EventIds the same way, so this
    works for either) on EventIds, a comma-delimited string (confirmed
    2026-08-04 against a real downloaded file; the codebook doesn't spell
    out the exact delimiter).

    GKG-side output columns are prefixed "GKG_" to avoid colliding with
    an identically-named Events column (NumArticles exists on both sides).

    Returns one row per (event, GKG row) pair. An event with no GKG match
    contributes no rows; a GKG row naming several events contributes one
    row per event, not one collapsed row.
    """
    _require_column(events_df.columns, REQUIRED_JOIN_COLUMNS["gdelt_event"][0], "events_df")
    _require_column(gkg_columns, REQUIRED_JOIN_COLUMNS["gdelt_gkg_v1"][0], "gkg_columns")
    warn_if_events_predate_gkg_coverage("GKG 1.0", GKG_V1_COVERAGE_START, events_df)

    columns = _validate_columns(columns, gkg_columns)
    read_columns = list((columns if columns is not None else set(gkg_columns)) | {"EventIds"})

    event_id_col = events_df["GlobalEventID"].astype("int64").astype(str)
    event_id_set = set(event_id_col)
    events_side = events_df.assign(_GlobalEventID_str=event_id_col)

    scanner = _dataset(gkg_folder).scanner(columns=read_columns, batch_size=64_000)

    matches: list[pd.DataFrame] = []
    for batch in tqdm(scanner.to_batches(), desc="Cross-referencing GKG 1.0"):
        df_batch = batch.to_pandas()
        if df_batch.empty:
            continue

        exploded = df_batch.assign(
            _matched_event_id=df_batch["EventIds"].fillna("").str.split(",")
        ).explode("_matched_event_id")
        exploded["_matched_event_id"] = exploded["_matched_event_id"].str.strip()
        exploded = exploded[exploded["_matched_event_id"].isin(event_id_set)]

        if exploded.empty:
            continue

        gkg_side = exploded.rename(columns={c: f"GKG_{c}" for c in df_batch.columns})
        matches.append(
            events_side.merge(
                gkg_side, left_on="_GlobalEventID_str", right_on="_matched_event_id", how="inner"
            )
        )

    if not matches:
        return pd.DataFrame()

    result = pd.concat(matches, ignore_index=True)
    return result.drop(columns=["_GlobalEventID_str", "_matched_event_id"])


def crossref_events_gkg_v2(
    events_df: pd.DataFrame,
    mentions_folder: str,
    gkg_v2_folder: str,
    gkg_v2_columns: list[str],
    columns: set[str] | None = None,
) -> pd.DataFrame:
    """
    Two-hop join for GKG 2.1, which carries no event id:
    Events -[GlobalEventID]-> Mentions -[document URL]-> GKG 2.1.

    GKG-side output columns are prefixed "GKG_"; the Mentions bridge
    fields carried through are prefixed "Mention_" (see
    OPTIONAL_MENTIONS_PAYLOAD_COLUMNS: present ones are carried through,
    a missing one just means its Mention_<name> column isn't in the
    output, not a failed join, unlike REQUIRED_JOIN_COLUMNS's
    GLOBALEVENTID/MentionIdentifier). Returns one row per
    (event, mention, GKG row) triple: an event mentioned by several
    articles contributes several rows, and an article covering several
    events contributes one row per event, not one collapsed row.

    The same article can be reprocessed across separate GKG 2.1 batches
    (e.g. a later crawl refines the extraction); rows are deduped on the
    document URL, keeping the most recently seen one, so each article
    contributes exactly one GKG row to the join.
    """
    _require_column(events_df.columns, REQUIRED_JOIN_COLUMNS["gdelt_event"][0], "events_df")
    _require_column(
        gkg_v2_columns, REQUIRED_JOIN_COLUMNS["gdelt_gkg_v2"][0], "gkg_v2_columns"
    )
    warn_if_events_predate_gkg_coverage(
        "GDELT 2.0 (GKG 2.1 / Mentions)", GKG_V2_COVERAGE_START, events_df
    )

    columns = _validate_columns(columns, gkg_v2_columns)
    read_gkg_columns = list(
        (columns if columns is not None else set(gkg_v2_columns)) | {"V2DOCUMENTIDENTIFIER"}
    )

    event_id_col = events_df["GlobalEventID"].astype("int64")
    event_id_set = set(event_id_col)

    # Hop 1: Mentions, filter-pushdown on GLOBALEVENTID, a real scalar
    # column unlike GKG 1.0's comma-packed EventIds, so this narrows
    # the scan at the row-group level instead of reading everything.
    mentions_dataset = _dataset(mentions_folder)
    mentions_schema_names = mentions_dataset.schema.names
    for required in REQUIRED_JOIN_COLUMNS["gdelt_mentions"]:
        _require_column(mentions_schema_names, required, "mentions_folder")

    # Same existing/missing split as columns_to_check and output_columns
    # elsewhere in the pipeline: read whichever optional payload columns
    # this Mentions dataset actually has, and simply carry through fewer
    # Mention_* fields for the ones it doesn't, rather than failing.
    mentions_payload_columns = [
        c for c in OPTIONAL_MENTIONS_PAYLOAD_COLUMNS if c in mentions_schema_names
    ]
    mentions_read_columns = list(REQUIRED_JOIN_COLUMNS["gdelt_mentions"]) + mentions_payload_columns

    logger.info(f"Cross-referencing {len(event_id_set)} event(s) against Mentions...")
    mentions_filter = pc.field("GLOBALEVENTID").isin(list(event_id_set))
    bridge_df = (
        mentions_dataset
        .to_table(columns=mentions_read_columns, filter=mentions_filter)
        .to_pandas()
    )

    if bridge_df.empty:
        return pd.DataFrame()

    urls = set(bridge_df["MentionIdentifier"].dropna().unique())
    if not urls:
        return pd.DataFrame()

    # Hop 2: GKG 2.1, filter-pushdown on the document URL: again a real
    # pyarrow predicate, so only rows for articles actually mentioning one
    # of these events get read off disk.
    logger.info(f"Cross-referencing {len(urls)} article URL(s) against GKG 2.1...")
    gkg_filter = pc.field("V2DOCUMENTIDENTIFIER").isin(list(urls))
    gkg_df = (
        _dataset(gkg_v2_folder).to_table(columns=read_gkg_columns, filter=gkg_filter).to_pandas()
    )

    if gkg_df.empty:
        return pd.DataFrame()

    gkg_df = gkg_df.drop_duplicates(subset=["V2DOCUMENTIDENTIFIER"], keep="last")
    gkg_df = gkg_df.rename(columns={c: f"GKG_{c}" for c in gkg_df.columns})

    bridge_df = bridge_df.rename(columns={c: f"Mention_{c}" for c in mentions_payload_columns})

    joined = bridge_df.merge(
        gkg_df, left_on="MentionIdentifier", right_on="GKG_V2DOCUMENTIDENTIFIER", how="inner"
    )
    if joined.empty:
        return pd.DataFrame()

    events_side = events_df.assign(_GlobalEventID_int64=event_id_col)
    result = events_side.merge(
        joined, left_on="_GlobalEventID_int64", right_on="GLOBALEVENTID", how="inner"
    )
    return result.drop(columns=["_GlobalEventID_int64", "GLOBALEVENTID", "MentionIdentifier"])
