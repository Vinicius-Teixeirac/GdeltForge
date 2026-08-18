import os
import zipfile
from pathlib import Path

import pandas as pd

from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)


def ensure_exists(path: str | Path, description: str) -> Path:
    """Ensure the given folder exists; raise helpful error if not."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{description} does not exist: {p}")
    return p


def write_parquet_atomic(df: pd.DataFrame, out: str | Path, **to_parquet_kwargs) -> None:
    """
    Write a DataFrame to Parquet via a temp file plus an atomic rename, so a
    process killed mid-write leaves either a complete file at the
    destination path or no file at all there, never a corrupt or empty one.

    to_parquet_kwargs are passed straight through to DataFrame.to_parquet
    (e.g. engine, compression), for callers that need more control than
    the pandas default.
    """
    out = Path(out)
    tmp_path = out.with_name(out.name + ".tmp")

    if tmp_path.exists():
        logger.warning(
            f"Found a leftover incomplete file from a previous interrupted "
            f"run: {tmp_path}. It will be overwritten."
        )

    try:
        df.to_parquet(tmp_path, **to_parquet_kwargs)
        os.replace(tmp_path, out)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def read_parquet_path(path: str | Path) -> pd.DataFrame:
    """
    Read a single Parquet file, or every Parquet file directly in a
    directory, concatenated into one DataFrame. A directory is globbed to
    *.parquet explicitly rather than handed to pandas as-is: convert and
    filter's own resumability markers (mark_done above writes <name>.done
    as a real sibling of the data) sit in exactly these directories by
    design, and pandas has no notion of that convention, so handing it a
    directory containing one tries to parse the marker as a Parquet file
    and fails with a confusing "magic bytes not found" error instead of
    just ignoring it.
    """
    p = Path(path)
    if not p.is_dir():
        return pd.read_parquet(p)

    files = sorted(p.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {path}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


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
    the exact same configuration this run is about to use -- not merely
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
    source_path = Path(source_path)
    return source_path.parent / (source_path.name + ".done")


def is_marked_done(source_path: str | Path, fingerprint: str) -> bool:
    """
    True if source_path has a sibling .done marker whose stored fingerprint
    matches the given one, meaning it was already processed under the
    current configuration. A marker left by a differently-configured run,
    or no marker at all (including a pre-fingerprint empty marker from
    before this existed), returns False so the file gets (re)processed.
    """
    marker = _done_marker_path(source_path)
    return marker.exists() and marker.read_text() == fingerprint


def mark_done(source_path: str | Path, fingerprint: str) -> None:
    """Record source_path as done under the given config fingerprint."""
    _done_marker_path(source_path).write_text(fingerprint)


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
