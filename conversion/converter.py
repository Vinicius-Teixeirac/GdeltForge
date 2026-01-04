"""
converter.py

Tools for converting downloaded GDELT data (https://data.gdeltproject.org/events/) 
from CSV inside ZIP archives to Parquet format.

All GDELT event files are distributed as compressed ZIP archives containing CSVs, which 
are less efficient and less memory-friendly than Parquet for large tabular datasets.

This module provides:
    - Converter: class responsible for performing file-by-file conversion
    - run_converter: wrapper that calls the main conversion routine `process_all_files`
"""

import glob
import os
from pathlib import Path
from typing import List, Optional

import pandas as pd
from tqdm import tqdm

from utils.io import unzip_file
from utils.logging import get_logger

log = get_logger(__name__)


class GDELTConverter:
    """
    Pipeline component: Converts GDELT compressed zip files into Parquet.
    Configuration is INJECTED (not loaded internally).
    """

    def __init__(self, config: dict):
        self.config = config

        self.input_folder = Path(config["paths"]["downloaded_data_directory"])
        self.unzip_folder = Path(config["paths"]["unzipped_data_directory"])
        self.parquet_folder = Path(config["paths"]["parquet_data_directory"])
        self.keep_unzipped = config["converter"]["keep_unzipped"]
        self.pattern = config["converter"].get("file_pattern", "*.zip")

        # Column definitions
        self.EVENT_COLUMNS = config["columns"]["gdelt_event"]
        self.NUMERIC_COLUMNS = config["columns_numeric"]

        self._create_folders()

    def _create_folders(self):
        """Create necessary folders if they don't exist."""
        for folder in [self.unzip_folder, self.parquet_folder]:
            folder.mkdir(parents=True, exist_ok=True)
            log.debug(f"Ensured folder exists: {folder}")

    # ------------------------------------------------------------
    # PROCESS ALL ZIP FILES
    # ------------------------------------------------------------
    def process_all_files(self) -> List[str]:
        zip_files = glob.glob(str(self.input_folder / self.pattern))

        if not zip_files:
            log.warning(f"No zip files found in {self.input_folder} with pattern '{pattern}'")
            return []

        log.info(f"Found {len(zip_files)} zip files to convert.")

        all_outputs = []

        for zip_file in tqdm(zip_files, desc="Converting ZIP files", unit="zip"):
            try:
                outputs = self.process_single_file(zip_file)
                all_outputs.extend(outputs)
            except Exception as e:
                log.error(f"Failed to process {os.path.basename(zip_file)}: {e}")

        log.info(f"Conversion complete. Total Parquets created: {len(all_outputs)}")
        self._cleanup_unzipped_folder()

        return all_outputs

    # ------------------------------------------------------------
    # PROCESS SINGLE ZIP FILE
    # ------------------------------------------------------------
    def process_single_file(self, zip_path: str) -> List[str]:
        log.info(f"Processing ZIP: {os.path.basename(zip_path)}")
        created_parquets = []

        extracted_files = unzip_file(zip_path, str(self.unzip_folder))
        if not extracted_files:
            log.warning(f"No extracted files from {zip_path}")
            return created_parquets

        for csv_path in extracted_files:
            csv_file = Path(csv_path)
            if csv_file.suffix.lower() != ".csv":
                log.debug(f"Skipping non-CSV file: {csv_file.name}")
                continue

            try:
                df = self._read_csv(csv_path)
                if df.empty:
                    continue

                parquet_path = self._save_parquet(df, csv_file.stem)
                if parquet_path:
                    created_parquets.append(str(parquet_path))

                if not self.keep_unzipped:
                    os.remove(csv_path)

            except Exception as e:
                log.error(f"Error processing CSV {csv_file.name}: {e}")

        return created_parquets

    # ------------------------------------------------------------
    # READ CSV
    # ------------------------------------------------------------
    def _read_csv(self, csv_path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                csv_path,
                sep="\t",
                header=None,
                dtype=str,
                encoding="utf-8",
                low_memory=False,
                names=self.EVENT_COLUMNS,
                on_bad_lines="warn",
            )

            # Convert numeric columns
            for col in self.NUMERIC_COLUMNS:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            return df

        except Exception as e:
            log.error(f"Error reading CSV {csv_path}: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------
    # SAVE PARQUET
    # ------------------------------------------------------------
    def _save_parquet(self, df: pd.DataFrame, base_name: str) -> Optional[Path]:
        if df.empty:
            log.warning(f"DataFrame empty, skipping parquet: {base_name}")
            return None

        parquet_path = self.parquet_folder / f"{base_name}.parquet"

        try:
            df.to_parquet(
                parquet_path,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )
            return parquet_path

        except Exception as e:
            log.error(f"Error saving parquet {parquet_path}: {e}")
            return None

    # ------------------------------------------------------------
    # CLEAN UP
    # ------------------------------------------------------------
    def _cleanup_unzipped_folder(self) -> None:
        """Remove the unzip folder if empty and keep_unzipped=False"""
        if self.keep_unzipped:
            return

        try:
            self.unzip_folder.rmdir()  # Only works if empty
            log.info(f"Deleted empty unzipped folder: {self.unzip_folder}")
        except OSError:
            log.warning(
                f"Unzipped folder not removed because it is not empty: {self.unzip_folder}"
            )


# ------------------------------------------------------------
# WRAPPER 
# ------------------------------------------------------------
def run_converter(config: dict) -> List[str]:
    """
    Convenience wrapper so main.py can call the converter cleanly.
    """
    converter = GDELTConverter(config)
    return converter.process_all_files()


# ------------------------------------------------------------
# STANDALONE EXECUTION
# ------------------------------------------------------------
if __name__ == "__main__":
    from utils.config import load_config

    log.info("Running GDELT conversion pipeline as standalone script…")
    cfg = load_config()
    outputs = run_converter(cfg)
    log.info(f"Created {len(outputs)} parquet files.")
