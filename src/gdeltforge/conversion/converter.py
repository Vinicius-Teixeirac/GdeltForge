"""
converter.py

Tools for converting downloaded GDELT data (https://data.gdeltproject.org/events/)
from CSV inside ZIP archives to Parquet format.

All GDELT event files are distributed as compressed ZIP archives containing CSVs, which
are less efficient and less memory-friendly than Parquet for large tabular datasets.

Source files come in three granularities, each identified by filename pattern:
  - Yearly  : 1979.zip, 2005.zip          (one file per year, up to ~2005)
  - Monthly : 200601.zip, 201303.zip      (one file per month, 2006-early 2013)
  - Daily   : 20130401.export.CSV.zip     (one file per day, April 2013-present)

When converter.partitioning.enabled is true, yearly and monthly ZIPs are written
as a Hive-partitioned dataset under parquet_historical_directory. Daily ZIPs
always go to parquet_data_directory as flat files (unchanged behavior).

This module provides:
    - GDELTConverter: class responsible for performing file-by-file conversion
    - run_converter: wrapper that calls the main conversion routine `process_all_files`
"""

import glob
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from gdeltforge.utils.config import dataset_path_key
from gdeltforge.utils.io import unzip_file
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

# ------------------------------------------------------------------
# Source file-type patterns
# ------------------------------------------------------------------
_YEARLY_PAT  = re.compile(r'^\d{4}\.zip$',        re.IGNORECASE)
_MONTHLY_PAT = re.compile(r'^\d{6}\.zip$',        re.IGNORECASE)
_DAILY_PAT   = re.compile(r'^\d{8}\..+\.zip$',    re.IGNORECASE)

# Columns that are semantically integers; cast from float64 after pd.to_numeric.
# Applies to both flat and historical writes for a consistent union schema.
_DATE_INT_COLS = ("Year", "MonthYear", "Day")


class GDELTConverter:
    """
    Pipeline component: Converts GDELT compressed zip files into Parquet.
    Configuration is INJECTED (not loaded internally).
    """

    def __init__(self, config: dict, dataset: str = "gdelt_event"):
        self.config = config
        self.dataset = dataset

        def path_for(base_key: str) -> str:
            return config["paths"][dataset_path_key(dataset, base_key)]

        self.input_folder  = Path(path_for("downloaded_data_directory"))
        self.unzip_folder  = Path(path_for("unzipped_data_directory"))
        self.parquet_folder = Path(path_for("parquet_data_directory"))
        self.keep_unzipped = config["converter"]["keep_unzipped"]
        self.pattern       = config["converter"].get("file_pattern", "*.zip")
        # None is a valid value here: ProcessPoolExecutor treats
        # max_workers=None as "use os.cpu_count()" on its own.
        self.max_workers: int | None = config["converter"].get("max_workers")

        self.COLUMN_NAMES    = config["columns"][dataset]
        self.NUMERIC_COLUMNS = config["columns_numeric"][dataset]

        part_cfg = config["converter"].get("partitioning", {})
        self._partitioning_enabled = part_cfg.get("enabled", False)
        self._partition_rules: list[dict] = part_cfg.get("rules", [])

        self.historical_folder: Path | None = None
        if self._partitioning_enabled:
            hist_key = dataset_path_key(dataset, "parquet_historical_directory")
            hist_path = config["paths"].get(hist_key)
            if not hist_path:
                raise ValueError(
                    f"converter.partitioning.enabled is true but "
                    f"paths.{hist_key} is not set."
                )
            self.historical_folder = Path(hist_path)

        self._create_folders()

    def _create_folders(self):
        for folder in [self.unzip_folder, self.parquet_folder]:
            folder.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured folder exists: {folder}")
        if self.historical_folder:
            self.historical_folder.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured historical folder exists: {self.historical_folder}")

    # ------------------------------------------------------------------
    # File-type detection
    # ------------------------------------------------------------------

    def _detect_file_type(self, zip_name: str) -> str:
        if _DAILY_PAT.match(zip_name):
            return "daily"
        if _MONTHLY_PAT.match(zip_name):
            return "monthly"
        if _YEARLY_PAT.match(zip_name):
            return "yearly"
        return "unknown"

    def _partition_rule_for(self, file_type: str) -> list[str] | None:
        """Return the partition column list for this file type, or None (flat)."""
        for rule in self._partition_rules:
            if rule.get("file_type") == file_type:
                return rule.get("by")
        return None

    # ------------------------------------------------------------------
    # .done marker helpers  (historical files only)
    # ------------------------------------------------------------------

    def _done_path(self, zip_path: Path) -> Path:
        return zip_path.parent / (zip_path.name + ".done")

    def _is_done(self, zip_path: Path) -> bool:
        return self._done_path(zip_path).exists()

    def _mark_done(self, zip_path: Path) -> None:
        self._done_path(zip_path).touch()

    # ------------------------------------------------------------
    # PROCESS ALL ZIP FILES
    # ------------------------------------------------------------
    def process_all_files(self) -> tuple[list[str], list[str]]:
        """
        Convert every matching ZIP in input_folder.

        Returns (outputs, failed) where outputs are the created parquet paths
        and failed are the source ZIP filenames that raised during processing.
        """
        zip_files = glob.glob(str(self.input_folder / self.pattern))

        if not zip_files:
            logger.warning(
                f"No zip files found in {self.input_folder} with pattern '{self.pattern}'"
            )
            return [], []

        to_process = []
        for zip_file in zip_files:
            zip_path = Path(zip_file)
            file_type = self._detect_file_type(zip_path.name)

            if self._partitioning_enabled and file_type != "daily" and self._is_done(zip_path):
                logger.info(f"Skipping already converted: {zip_path.name}")
                continue

            to_process.append(zip_file)

        if not to_process:
            logger.info("Nothing to convert; all files already processed.")
            return [], []

        logger.info(
            f"Converting {len(to_process)} zip file(s) using "
            f"{self.max_workers or os.cpu_count() or '?'} worker process(es)..."
        )
        all_outputs: list[str] = []
        failed: list[str] = []

        # Each zip is processed independently (its own extracted CSV names,
        # own output parquet paths), so file-level parallelism across
        # processes is safe -- this is CPU-bound (CSV parsing + parquet
        # writing), so ProcessPoolExecutor beats threads here.
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.process_single_file, zip_file): zip_file
                for zip_file in to_process
            }

            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Converting ZIP files", unit="zip"
            ):
                zip_path = Path(futures[future])
                try:
                    outputs = future.result()
                    all_outputs.extend(outputs)

                    file_type = self._detect_file_type(zip_path.name)
                    if self._partitioning_enabled and file_type != "daily":
                        self._mark_done(zip_path)

                except Exception as e:
                    logger.error(f"Failed to process {zip_path.name}: {e}")
                    failed.append(zip_path.name)

        logger.info(
            f"Conversion complete. Total Parquets created: {len(all_outputs)}, "
            f"{len(failed)} zip file(s) failed."
        )
        self._cleanup_unzipped_folder()
        return all_outputs, failed

    # ------------------------------------------------------------
    # PROCESS SINGLE ZIP FILE
    # ------------------------------------------------------------
    def process_single_file(self, zip_path: str) -> list[str]:
        zip_p = Path(zip_path)
        logger.info(f"Processing ZIP: {zip_p.name}")
        created_parquets = []

        file_type = (
            self._detect_file_type(zip_p.name)
            if self._partitioning_enabled
            else "daily"
        )

        extracted_files = unzip_file(zip_path, self.unzip_folder)
        if not extracted_files:
            logger.warning(f"No extracted files from {zip_p.name}")
            return created_parquets

        for csv_path in extracted_files:
            if csv_path.suffix.lower() != ".csv":
                logger.debug(f"Skipping non-CSV file: {csv_path.name}")
                continue

            try:
                df = self._read_csv(csv_path)
                if df.empty:
                    continue

                if self._partitioning_enabled and file_type != "daily":
                    written = self._save_historical_parquet(df, zip_p, file_type)
                    created_parquets.extend(str(p) for p in written)
                else:
                    parquet_path = self._save_parquet(df, csv_path.stem)
                    if parquet_path:
                        created_parquets.append(str(parquet_path))

                if not self.keep_unzipped:
                    csv_path.unlink()

            except Exception as e:
                logger.error(f"Error processing CSV {csv_path.name}: {e}")

        return created_parquets

    # ------------------------------------------------------------
    # READ CSV
    # ------------------------------------------------------------
    def _read_csv(self, csv_path: str | Path) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                csv_path,
                sep="\t",
                header=None,
                dtype=str,
                encoding="utf-8",
                low_memory=False,
                names=self.COLUMN_NAMES,
                on_bad_lines="warn",
            )

            for col in self.NUMERIC_COLUMNS:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            return df

        except Exception as e:
            logger.error(f"Error reading CSV {csv_path}: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------
    # SAVE PARQUET  (flat daily files)
    # ------------------------------------------------------------
    def _save_parquet(self, df: pd.DataFrame, base_name: str) -> Path | None:
        if df.empty:
            logger.warning(f"DataFrame empty, skipping parquet: {base_name}")
            return None

        parquet_path = self.parquet_folder / f"{base_name}.parquet"

        try:
            # Cast date/ID columns to Int64 so the schema is consistent with
            # historical Hive files, enabling clean union datasets.
            for col in _DATE_INT_COLS:
                if col in df.columns and pd.api.types.is_float_dtype(df[col]):
                    df[col] = df[col].astype("Int64")

            df.to_parquet(
                parquet_path,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )
            return parquet_path

        except Exception as e:
            logger.error(f"Error saving parquet {parquet_path}: {e}")
            return None

    # ------------------------------------------------------------
    # SAVE HISTORICAL PARQUET  (Hive-partitioned)
    # ------------------------------------------------------------
    def _save_historical_parquet(
        self, df: pd.DataFrame, zip_path: Path, file_type: str
    ) -> list[Path]:
        """
        Write df into the historical Hive directory, partitioned by the columns
        defined in settings under converter.partitioning.rules for this file_type.

        Each (partition key combination) becomes one parquet file named after
        the source ZIP stem, e.g.:
            parquet_historical/Year=1979/MonthYear=197901/1979.parquet
        """
        by = self._partition_rule_for(file_type)
        if not by:
            logger.warning(
                f"No partition rule for file_type={file_type!r}; "
                f"falling back to flat write for {zip_path.name}"
            )
            result = self._save_parquet(df, zip_path.stem)
            return [result] if result else []

        df_clean = df.dropna(subset=by).copy()
        if df_clean.empty:
            logger.warning(f"No valid rows after dropping NaN in {by} for {zip_path.name}")
            return []

        # Cast partition columns to int for clean directory names and
        # consistent Arrow schema with the flat daily files.
        for col in by:
            df_clean[col] = df_clean[col].astype("Int64")

        # Only called when partitioning is enabled, which requires
        # historical_folder to be set (see __init__).
        assert self.historical_folder is not None

        created: list[Path] = []

        for key, group in df_clean.groupby(by, sort=True):
            if not isinstance(key, tuple):
                key = (key,)

            dir_parts = "/".join(f"{col}={int(val)}" for col, val in zip(by, key, strict=True))
            out_dir = self.historical_folder / dir_parts
            out_dir.mkdir(parents=True, exist_ok=True)

            out_path = out_dir / f"{zip_path.stem}.parquet"
            table = pa.Table.from_pandas(group, preserve_index=False)
            pq.write_table(table, out_path, compression="snappy")
            created.append(out_path)
            logger.debug(f"Written: {out_path}")

        return created

    # ------------------------------------------------------------
    # CLEAN UP
    # ------------------------------------------------------------
    def _cleanup_unzipped_folder(self) -> None:
        if self.keep_unzipped:
            return
        try:
            self.unzip_folder.rmdir()
            logger.info(f"Deleted empty unzipped folder: {self.unzip_folder}")
        except OSError:
            logger.warning(
                f"Unzipped folder not removed because it is not empty: {self.unzip_folder}"
            )


# ------------------------------------------------------------
# WRAPPER
# ------------------------------------------------------------
def run_converter(config: dict, dataset: str = "gdelt_event") -> tuple[list[str], list[str]]:
    """
    Convenience wrapper so main.py can call the converter cleanly.

    Returns (outputs, failed): see GDELTConverter.process_all_files.
    """
    converter = GDELTConverter(config, dataset=dataset)
    return converter.process_all_files()


# ------------------------------------------------------------
# STANDALONE EXECUTION
# ------------------------------------------------------------
if __name__ == "__main__":
    from gdeltforge.utils.config import load_config

    logger.info("Running GDELT conversion pipeline as standalone script...")
    cfg = load_config()
    outputs, failed = run_converter(cfg)
    logger.info(f"Created {len(outputs)} parquet files, {len(failed)} failed.")
