import os
import zipfile
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pyarrow as pa

# polars genuinely exports this type alias at runtime, under a
# private-looking module name; matches converter.py's own identical
# ParquetCompression import, covered by this project's own
# reportPrivateImportUsage = false.
from polars._typing import PolarsDataType

from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)


def ensure_exists(path: str | Path, description: str) -> Path:
    """Ensure the given folder exists; raise helpful error if not."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{description} does not exist: {p}")
    return p


@contextmanager
def clearer_dataset_errors(what: str):
    """
    Wraps a parquet dataset read, pyarrow.dataset-based (construction,
    fragment metadata access, .schema, .to_table(), .scanner().to_batches(),
    still used directly by indexer.py) or polars-based (scan_parquet/
    read_parquet and anything chained off them, used everywhere else in
    the pipeline), so a bare ArrowInvalid/ComputeError/OSError, typically
    "Could not open Parquet input source '<path>': ..." or "File out of
    specification: ...", gets an actionable message on top instead of
    surfacing as-is. The underlying library's own message already names
    the specific file; this adds what gdeltforge was doing when it hit it
    and the likely causes, since a generic low-level read error with
    nothing else to go on is hard to act on. Both engines share the same
    hazard around laziness: a pyarrow.dataset.Dataset built from an
    explicit file list reads at least the first file's schema at
    construction time, not only later; a polars LazyFrame instead defers
    that to the first .collect_schema()/.collect() call. Either way, a
    corrupt/non-parquet file can surface at a different point in the call
    chain depending on where in the list it lands, confirmed empirically
    for both engines, so callers must wrap starting from construction/
    scan, not just the final materializing call.

    FileNotFoundError is deliberately excluded even though it's an
    OSError subclass: gdeltforge's own "no parquet files matched" checks
    (empty glob, a date range excluding every file) raise it before ever
    touching pyarrow or polars, and that's a real, already-clear error in
    its own right, not a dataset read failure to be reclassified as one.

    Chained via `from`, so the original traceback is still there
    underneath.
    """
    try:
        yield
    except FileNotFoundError:
        raise
    except (pa.ArrowException, pl.exceptions.ComputeError, OSError) as e:
        raise RuntimeError(
            f"Failed reading {what}: {e}\n"
            f"The error above names the specific file that couldn't be "
            f"opened as parquet. Common causes: an interrupted download "
            f"or write left a corrupt/incomplete file, or a non-parquet "
            f"file ended up matching the *.parquet glob. Removing or "
            f"re-fetching the named file and rerunning (with --force if "
            f"it was already marked done) usually resolves it."
        ) from e


def write_parquet_atomic(df: pl.DataFrame, out: str | Path, **write_parquet_kwargs) -> None:
    """
    Write a DataFrame to Parquet via a temp file plus an atomic rename, so a
    process killed mid-write leaves either a complete file at the
    destination path or no file at all there, never a corrupt or empty one.

    write_parquet_kwargs are passed straight through to DataFrame.
    write_parquet (e.g. compression), for callers that need more control
    than polars' own default (zstd, already matching this project's own).
    """
    out = Path(out)
    tmp_path = out.with_name(out.name + ".tmp")

    if tmp_path.exists():
        logger.warning(
            f"Found a leftover incomplete file from a previous interrupted "
            f"run: {tmp_path}. It will be overwritten."
        )

    try:
        df.write_parquet(tmp_path, **write_parquet_kwargs)
        os.replace(tmp_path, out)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


_EXPORT_FORMATS = ("parquet", "csv")


def write_dataframe_atomic(
    df: pl.DataFrame, out: str | Path, export_format: str = "parquet", **kwargs
) -> None:
    """
    Same atomic tmp-then-rename guarantee as write_parquet_atomic, generalized
    to sample/crossref's --export-format. export_format="parquet" (the
    default) delegates straight to write_parquet_atomic; convert/filter's own
    writes (_save_parquet, the lazy sink pipeline) are untouched and never
    call this: their per-file streaming architecture has no reason to share a
    code path with a single already-in-memory DataFrame's final write.
    """
    if export_format == "parquet":
        write_parquet_atomic(df, out, **kwargs)
        return

    if export_format != "csv":
        raise ValueError(
            f"Unsupported export format: {export_format!r} "
            f"(expected one of {_EXPORT_FORMATS})"
        )

    out = Path(out)
    tmp_path = out.with_name(out.name + ".tmp")

    if tmp_path.exists():
        logger.warning(
            f"Found a leftover incomplete file from a previous interrupted "
            f"run: {tmp_path}. It will be overwritten."
        )

    try:
        df.write_csv(tmp_path, **kwargs)
        os.replace(tmp_path, out)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def read_parquet_path(path: str | Path) -> pl.DataFrame:
    """
    Read a single Parquet file, or every Parquet file anywhere under a
    directory, concatenated into one DataFrame. A directory is globbed to
    *.parquet explicitly rather than handed to polars as-is: convert and
    filter's own resumability markers (mark_done above writes them as a
    dot-prefixed sibling of the data, e.g. ".<name>.done") sit in exactly
    these directories by design. pyarrow.dataset (and pandas.read_parquet,
    when this project still used it) silently skips dot-prefixed files on
    a bare directory read, the standard Hadoop/Spark/Parquet convention
    for "not a data file". polars.read_parquet on a bare directory does
    not: confirmed directly, it raises InvalidOperationError on the
    mixed .parquet/.done extensions rather than skipping the marker, a
    clearer failure than pandas/pyarrow's silent tolerance would have
    produced but still a failure. The explicit *.parquet glob here avoids
    the question either way, by construction rather than by relying on
    whichever behavior the current engine happens to have.

    Globbed recursively (rglob), not just the directory's own top level:
    found via a live comprehensive QA pass, crossref --events <dir>
    reported "No parquet files found" against a real, valid, non-empty
    Hive-partitioned historical directory (Year=YYYY/MonthYear=YYYYMM/
    *.parquet), the exact directory shape converter.partitioning writes
    for events/events-reduced and IndexedSampler's own FileIndex already
    walks. A flat, non-nested directory's own files are still found the
    same way as before, one level down being the trivial case of
    "anywhere under."

    Files are read through a per-file union schema with missing_columns=
    "insert", the same reconciliation IndexedSampler's own get_random_
    sample and CalendarSampler/FilteredSampler's shared _scan_dataset
    already apply: a real accumulated directory mixing flat and
    historical files can genuinely disagree on physical schema (a
    Hive-partitioned file converted before a schema fix landed, an
    output_columns setting narrowed at one point and widened again
    later), and a plain per-file pl.read_parquet followed by pl.concat
    has no way to tolerate that, raising a raw "unable to append to a
    DataFrame of width X with a DataFrame of width Y" the moment two
    such files are both pulled into --events.
    """
    p = Path(path)
    if not p.is_dir():
        return pl.read_parquet(p)

    files = sorted(p.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {path}")
    with clearer_dataset_errors(f"{len(files)} parquet file(s) in {path}"):
        schema: dict[str, PolarsDataType] = {}
        for f in files:
            for name, dtype in pl.read_parquet_schema(f).items():
                schema.setdefault(name, dtype)
        return pl.concat(
            pl.scan_parquet(f, schema=schema, missing_columns="insert").collect()
            for f in files
        )


def _fingerprint_value(value: object) -> str:
    if value is None:
        return "None"
    if isinstance(value, (list, tuple, set, frozenset)):
        return ",".join(sorted(value))
    return str(value)


def config_fingerprint(**fields: object) -> str:
    """
    Build a stable, human-readable fingerprint string from the config
    values that determine a pipeline stage's output shape or content (e.g.
    which columns are kept, which are cast to float32).

    Used together with is_marked_done/mark_done below so a resumed run
    treats a source file as done only if it was already processed under
    the exact same configuration this run is about to use, not merely
    that a marker exists. A run started after a relevant setting changed
    (columns_to_check, output_columns, ...) must reprocess every file
    rather than silently serving output produced under the old config.

    Field order is fixed (sorted by name) so the same fields always
    produce the same string regardless of call-site kwarg order. List
    values are sorted too, so reordering a column list without changing
    its membership doesn't look like a change.
    """
    return "\n".join(
        f"{name}={_fingerprint_value(value)}" for name, value in sorted(fields.items())
    )


def _done_marker_path(source_path: str | Path) -> Path:
    # Dot-prefixed: filter's own marker sits next to its source, which is
    # convert's *output* parquet directory, exactly the directory a user
    # or notebook most naturally points a plain pd.read_parquet(directory)/
    # pyarrow.dataset.dataset(directory) at. A bare directory read tries to
    # open every file in it, and a non-dot-prefixed "<name>.parquet.done"
    # (plain text, not parquet) fails with a bare, unhelpful
    # "Parquet magic bytes not found in footer", confirmed against a
    # real 30,000+-marker directory in this repo's own data. Dot-prefixed
    # (hidden) files are the standard Hadoop/Spark/Parquet convention for
    # "not a data file" (the same way _SUCCESS/.hidden are skipped), and
    # both pandas.read_parquet and pyarrow.dataset already skip them
    # automatically, confirmed empirically, not assumed. gdeltforge's
    # own glob("*.parquet") call sites were never affected either way,
    # since ".done" doesn't match that pattern regardless of the leading
    # dot; this only matters for readers outside gdeltforge's own control.
    source_path = Path(source_path)
    return source_path.parent / ("." + source_path.name + ".done")


def _legacy_done_marker_path(source_path: str | Path) -> Path:
    """Pre-hidden-marker naming (not dot-prefixed): checked as a fallback
    only, by is_marked_done, and migrated away (renamed to the current
    scheme) the first time it's found valid, so an existing installation's
    output directory cleans itself up over time without forcing every
    already-processed file to be reprocessed."""
    source_path = Path(source_path)
    return source_path.parent / (source_path.name + ".done")


def is_marked_done(source_path: str | Path, fingerprint: str) -> bool:
    """
    True if source_path has a sibling .done marker whose stored fingerprint
    matches the given one, meaning it was already processed under the
    current configuration. A marker left by a differently-configured run,
    or no marker at all (including a pre-fingerprint empty marker from
    before this existed), returns False so the file gets (re)processed.

    Falls back to the legacy (pre-hidden) marker name if the current one
    isn't there, migrating it (rename, not copy) on a match so the
    directory converges on the hidden naming without any reprocessing.
    """
    marker = _done_marker_path(source_path)
    if marker.exists():
        return marker.read_text() == fingerprint

    legacy = _legacy_done_marker_path(source_path)
    if legacy.exists() and legacy.read_text() == fingerprint:
        try:
            legacy.rename(marker)
        except OSError:
            pass  # best-effort migration; still correctly reports done
        return True

    return False


def mark_done(source_path: str | Path, fingerprint: str) -> None:
    """Record source_path as done under the given config fingerprint."""
    _done_marker_path(source_path).write_text(fingerprint)


def delete_done_marker(source_path: str | Path) -> None:
    """
    Remove source_path's .done marker, if any: both the current
    (hidden) and legacy naming, since either could be present depending
    on whether is_marked_done has run since the migration to hidden
    markers. Meant to be called right after source_path itself is
    deleted (--delete-source): once the source is gone, the marker has
    nothing left to gate, since a deleted zip/parquet can never be found
    by process_all_files'/filter_all_files' own glob again on a later
    run. Left behind, it just accumulates one orphaned marker file per
    deleted source in a directory --delete-source's whole point was to
    shrink. missing_ok=True: a source that was never actually marked
    done (e.g. force=True skipped the check that would have written
    one) isn't an error here.
    """
    _done_marker_path(source_path).unlink(missing_ok=True)
    _legacy_done_marker_path(source_path).unlink(missing_ok=True)


def warn_if_delete_source_drops_recoverable_data(
    logger, stage: str, delete_source: bool, narrowing: list[str]
) -> None:
    """
    Shared by convert.py's run_converter and filter.py's run_filter, both
    of which expose a delete_source knob that removes the input file once
    its output is written successfully, to save the disk a full
    raw-plus-processed archive would otherwise need. Combined with any
    setting that narrows what actually lands in that output relative to
    the input (a column projection, a row filter, a precision cast),
    deleting the input means whatever that setting dropped or changed has
    nothing left to recover it from except redoing an earlier pipeline
    stage: re-scraping for convert, re-converting for filter. Never
    blocks: this is a legitimate storage/completeness tradeoff a caller
    is entitled to make deliberately, just one worth being explicit about
    rather than silent.

    narrowing is the list of setting names actually active for this run
    (e.g. ["output_columns"], ["columns_to_check", "float32_columns"]);
    empty means this run's output is a straight copy of its input, so
    delete_source has no recoverability cost and nothing to warn about.
    """
    if not delete_source or not narrowing:
        return
    settings = ", ".join(narrowing)
    logger.warning(
        f"delete_source is set for {stage} together with {settings}: the input "
        f"file is deleted once its (narrowed) output is written, so whatever "
        f"{settings} dropped or changed has nothing left to recover it from "
        f"except redoing an earlier pipeline stage."
    )


def narrow_to_available_columns(
    logger, label: str, requested: set[str], required: set[str], available: set[str]
) -> list[str]:
    """
    Shared by samplers.py's FilteredSampler and crossref.py's v1/v2 join
    paths, both of which build a column projection that defaults to a
    dataset's full declared schema (columns.<dataset> in config) when
    the caller doesn't pass --columns/--columns v2_columns explicitly.
    That declared schema is not the same thing as what a real file
    actually has on disk: convert.output_columns or filter.output_columns
    can legitimately prune a dataset down to a handful of columns for
    disk/CPU reasons, and neither sampling nor crossref knew to check
    for that before this existed, so the default (or an explicit
    --columns naming a column config still declares but a pruned file no
    longer has) crashed with a raw, unhelpful polars error at the
    eventual .select() ("unable to find column ...") instead of a clear
    one, once convert/filter had already run to completion under that
    pruning.

    required is checked against `available` first and raises a clear
    error if actually missing: this is for a column the caller has no
    usable path forward without (a join key, a --stratify/--date-column
    grouping key, a --filter condition's own column), so silently
    narrowing it away would only trade one confusing failure for a
    quieter, more misleading one (e.g. a join that runs to completion
    but never matches anything). Everything else in `requested` is
    optional, output-only: whichever of it is actually available is
    kept, and whichever isn't is dropped with a warning naming exactly
    what and why, rather than crashing or silently returning less than
    requested with no explanation at all.

    logger is passed in rather than used from this module, matching
    warn_if_delete_source_drops_recoverable_data's own reasoning just
    above, so the warning is attributed to whichever stage actually
    emitted it.
    """
    missing_required = required - available
    if missing_required:
        raise ValueError(
            f"{label}: required column(s) {sorted(missing_required)} not found in the "
            f"scanned data. This dataset's configured output_columns may have pruned "
            f"them away at an earlier stage (convert/filter)."
        )

    missing_output = requested - available - required
    if missing_output:
        logger.warning(
            f"{label}: {sorted(missing_output)} not found in the scanned data, likely "
            f"pruned by an earlier stage's output_columns, and will be excluded from "
            f"the output rather than failing the run."
        )

    return sorted((requested & available) | required)


def unzip_file(zip_filepath: str | Path, extract_to_dir: str | Path | None = None) -> list[Path]:
    """
    Unzips a zip file and returns a list of extracted file paths.
    """
    zip_path = Path(zip_filepath)
    if not zip_path.exists():
        logger.error(f"Zip file not found: {zip_path}")
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    if extract_to_dir is None:
        out_dir = zip_path.parent
    else:
        out_dir = Path(extract_to_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Unzipping: {zip_path} -> {out_dir}")

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            file_names = zip_ref.namelist()
            zip_ref.extractall(out_dir)

    except zipfile.BadZipFile:
        logger.error(f"Bad ZIP file: {zip_path}")
        raise

    except Exception as e:
        logger.error(f"Unexpected error while unzipping {zip_path}: {e}")
        raise

    extracted = [out_dir / name for name in file_names if (out_dir / name).is_file()]
    logger.info(f"Extracted {len(extracted)} files from {zip_path.name}")

    return extracted
