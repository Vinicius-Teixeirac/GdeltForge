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
