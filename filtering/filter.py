"""
filter.py

Filtering utilities for GDELT Parquet datasets.

The GDELTFilter class drops rows with NaN values in specified columns. For example,
a user may wish to keep only events where both actors are identified. In that case, 
they can define in `settings.yaml`:

filter:
  columns_to_check:
    - Actor1Code
    - Actor2Code

The GDELTFilter class will then remove all rows where any of the specified columns 
contain NaN.

Provides:
    - GDELTFilter: main filtering class
    - run_filter: wrapper that calls the main method `filter_all_files`
"""

import glob
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Union

import pandas as pd
from tqdm import tqdm

from utils.logging import get_logger

logger = get_logger(__name__)


class GDELTFilter:
    """
    Filters Parquet files by removing rows with NaN in specified columns.
    """

    def __init__(
        self,
        input_folder: str,
        output_folder: str,
        columns_to_check: List[str],
    ):
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.columns_to_check = columns_to_check

        self.output_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f"Filter output folder ensured: {self.output_folder}")

    # ======================================================================
    # PUBLIC API
    # ======================================================================

    def filter_all_files(self, pattern: str = "*.parquet") -> tuple[int, int]:

        """
        Filter all parquet files in input_folder matching pattern.
        """
        parquet_files = glob.glob(str(self.input_folder / pattern))

        if not parquet_files:
            logger.warning(f"No parquet files found in: {self.input_folder}")
            return 0, 0

        logger.info(f"Filtering {len(parquet_files)} parquet file(s).")

        total_rows_before = 0
        total_rows_after = 0
        files_processed = 0
        files_failed = 0

        for idx, parquet_path in tqdm(enumerate(parquet_files, start=1), desc='filtering data'):
            file_name = Path(parquet_path).name

            try:
                rows_before, rows_after = self.filter_single_file(parquet_path)
                total_rows_before += rows_before
                total_rows_after += rows_after
                files_processed += 1

                rate = (rows_after / rows_before * 100) if rows_before else 0
                logger.info(
                    f"[{idx}/{len(parquet_files)}] {file_name}: "
                    f"{rows_before:,} → {rows_after:,} rows ({rate:.1f}% kept)"
                )

            except Exception as e:
                files_failed += 1
                logger.error(f"Failed to filter {file_name}: {e}")

        # Summary
        logger.info("===============================================")
        logger.info("FILTERING SUMMARY")
        logger.info("===============================================")
        logger.info(f"Files processed successfully: {files_processed}")
        logger.info(f"Files failed: {files_failed}")
        logger.info(f"Total rows before: {total_rows_before:,}")
        logger.info(f"Total rows after: {total_rows_after:,}")

        if total_rows_before > 0:
            dropped = total_rows_before - total_rows_after
            retention = total_rows_after / total_rows_before * 100
            logger.info(f"Overall retention rate: {retention:.2f}%")
            logger.info(f"Total rows removed: {dropped:,}")

        return files_processed, files_failed

    # ======================================================================
    # PER-FILE PROCESSING
    # ======================================================================

    def filter_single_file(self, parquet_path: str) -> Tuple[int, int]:
        """
        Filter a single parquet file and return (rows_before, rows_after).
        """
        file_path = Path(parquet_path)
        logger.debug(f"Filtering file: {file_path.name}")

        # 1. Read parquet
        df = pd.read_parquet(file_path)
        rows_before = len(df)

        if rows_before == 0:
            logger.warning(f"Empty parquet file skipped: {file_path.name}")
            return 0, 0

        # 2. Determine which columns exist
        existing_columns = [c for c in self.columns_to_check if c in df.columns]
        missing_columns = [c for c in self.columns_to_check if c not in df.columns]

        if missing_columns:
            logger.warning(
                f"{file_path.name}: Missing {len(missing_columns)} column(s): {missing_columns}"
            )

        if not existing_columns:
            logger.error(f"{file_path.name}: None of the filter columns exist.")
            return rows_before, rows_before

        # 3. Drop NaN rows
        df_clean = df.dropna(subset=existing_columns).reset_index(drop=True)
        rows_after = len(df_clean)

        # 4. Save filtered parquet
        output_filename = f"{file_path.stem}_filtered.parquet"
        output_path = self.output_folder / output_filename

        df_clean.to_parquet(
            output_path,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        logger.debug(f"Saved filtered file -> {output_path}")
        return rows_before, rows_after

    # ======================================================================
    # VALIDATION
    # ======================================================================

    def validate_columns(self, sample_file: Optional[str] = None) -> Dict[str, Union[str, int, List[str]]]:
        """
        Check if required columns exist in a sample parquet file.
        """
        if sample_file is None:
            files = glob.glob(str(self.input_folder / "*.parquet"))
            if not files:
                return {"error": "No parquet files found for validation."}
            sample_file = files[0]

        sample_file = Path(sample_file)
        logger.info(f"Validating column presence in: {sample_file.name}")

        try:
            schema = pd.read_parquet(sample_file, nrows=0)
            existing = [c for c in self.columns_to_check if c in schema.columns]
            missing = [c for c in self.columns_to_check if c not in schema.columns]

            return {
                "sample_file": sample_file.name,
                "total_expected_columns": len(self.columns_to_check),
                "existing_columns": existing,
                "missing_columns": missing,
            }

        except Exception as e:
            logger.error(f"Column validation error: {e}")
            return {"error": str(e)}


# ======================================================================
# RUN WRAPPER (used by main.py)
# ======================================================================

def run_filter(config: dict) -> Tuple[int, int]:
    """
    Convenience wrapper so main.py can call the filter cleanly.
    """
    filterer = GDELTFilter(
        input_folder=config["paths"]["parquet_data_directory"],
        output_folder=config["paths"]["filtered_data_directory"],
        columns_to_check=config["filter"]["columns_to_check"],
    )
    return filterer.filter_all_files()
