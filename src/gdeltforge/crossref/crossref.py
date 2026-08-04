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
      EventIds directly on each row, a semicolon-delimited string, so this
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


def crossref_events_gkg_v1(
    events_df: pd.DataFrame,
    gkg_folder: str,
    gkg_columns: list[str],
    columns: set[str] | None = None,
) -> pd.DataFrame:
    """
    Direct join: events_df x a GKG 1.0-family dataset (the main file or
    its separate Counts file, both carry EventIds the same way, so this
    works for either) on EventIds.

    GKG-side output columns are prefixed "GKG_" to avoid colliding with
    an identically-named Events column (NumArticles exists on both sides).

    Returns one row per (event, GKG row) pair. An event with no GKG match
    contributes no rows; a GKG row naming several events contributes one
    row per event, not one collapsed row.
    """
    _require_column(events_df.columns, "GlobalEventID", "events_df")
    _require_column(gkg_columns, "EventIds", "gkg_columns")

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
            _matched_event_id=df_batch["EventIds"].fillna("").str.split(";")
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
    fields carried through are prefixed "Mention_". Returns one row per
    (event, mention, GKG row) triple: an event mentioned by several
    articles contributes several rows, and an article covering several
    events contributes one row per event, not one collapsed row.

    The same article can be reprocessed across separate GKG 2.1 batches
    (e.g. a later crawl refines the extraction); rows are deduped on the
    document URL, keeping the most recently seen one, so each article
    contributes exactly one GKG row to the join.
    """
    _require_column(events_df.columns, "GlobalEventID", "events_df")
    _require_column(gkg_v2_columns, "V2DOCUMENTIDENTIFIER", "gkg_v2_columns")

    columns = _validate_columns(columns, gkg_v2_columns)
    read_gkg_columns = list(
        (columns if columns is not None else set(gkg_v2_columns)) | {"V2DOCUMENTIDENTIFIER"}
    )

    event_id_col = events_df["GlobalEventID"].astype("int64")
    event_id_set = set(event_id_col)

    # Hop 1: Mentions, filter-pushdown on GLOBALEVENTID, a real scalar
    # column unlike GKG 1.0's semicolon-packed EventIds, so this narrows
    # the scan at the row-group level instead of reading everything.
    logger.info(f"Cross-referencing {len(event_id_set)} event(s) against Mentions...")
    mentions_filter = pc.field("GLOBALEVENTID").isin(list(event_id_set))
    bridge_df = (
        _dataset(mentions_folder)
        .to_table(
            columns=["GLOBALEVENTID", "MentionIdentifier", "MentionTimeDate", "Confidence"],
            filter=mentions_filter,
        )
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

    bridge_df = bridge_df.rename(
        columns={
            "MentionTimeDate": "Mention_MentionTimeDate",
            "Confidence": "Mention_Confidence",
        }
    )

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
