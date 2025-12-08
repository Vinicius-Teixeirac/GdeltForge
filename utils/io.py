import os
from pathlib import Path
import zipfile

from utils.logging import get_logger

log = get_logger(__name__)


def ensure_exists(path: str | Path, description: str) -> Path:
    """Ensure the given folder exists; raise helpful error if not."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{description} does not exist: {p}")
    return p

def unzip_file(zip_filepath, extract_to_dir=None):
    """
    Unzips a zip file and returns a list of extracted file paths.
    """
    if not os.path.exists(zip_filepath):
        log.error(f"Zip file not found: {zip_filepath}")
        raise FileNotFoundError(f"Zip file not found: {zip_filepath}")

    if extract_to_dir is None:
        extract_to_dir = os.path.dirname(os.path.abspath(zip_filepath))
    else:
        os.makedirs(extract_to_dir, exist_ok=True)

    log.info(f"Unzipping: {zip_filepath} -> {extract_to_dir}")

    try:
        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            file_names = zip_ref.namelist()
            zip_ref.extractall(extract_to_dir)

    except zipfile.BadZipFile:
        log.error(f"Bad ZIP file: {zip_filepath}")
        raise

    except Exception as e:
        log.error(f"Unexpected error while unzipping {zip_filepath}: {e}")
        raise

    extracted = []
    for name in file_names:
        abs_path = os.path.abspath(os.path.join(extract_to_dir, name))
        if os.path.isfile(abs_path):
            extracted.append(abs_path)

    log.info(f"Extracted {len(extracted)} files from {os.path.basename(zip_filepath)}")

    return extracted
