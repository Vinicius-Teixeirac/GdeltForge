import zipfile
from pathlib import Path

from utils.logging import get_logger

logger = get_logger(__name__)


def ensure_exists(path: str | Path, description: str) -> Path:
    """Ensure the given folder exists; raise helpful error if not."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{description} does not exist: {p}")
    return p

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
