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

When converter.partitioning.enabled is true the historical Hive-partitioned dataset
(under parquet_historical_directory) is filtered in addition to the flat daily files.
The Hive directory structure is preserved in filtered_historical_directory.

Provides:
    - GDELTFilter: main filtering class
    - run_filter: wrapper that calls the main method `filter_all_files`
"""

import glob
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from utils.logging import get_logger

logger = get_logger(__name__)


class GDELTFilter:
    """
    Filters Parquet files by removing rows with NaN in specified columns.
    Handles both flat daily files and Hive-partitioned historical files.
    """

    def __init__(
        self,
        input_folder: str,
        output_folder: str,
        columns_to_check: List[str],
        historical_input_folder: Optional[str] = None,
        historical_output_folder: Optional[str] = None,
    ):
        self.input_folder  = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.columns_to_check = columns_to_check

        self.historical_input_folder: Optional[Path] = (
            Path(historical_input_folder) if historical_input_folder else None
        )
        self.historical_output_folder: Optional[Path] = (
            Path(historical_output_folder) if historical_output_folder else None
        )

        self.output_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f"Filter output folder ensured: {self.output_folder}")

        if self.historical_output_folder:
            self.historical_output_folder.mkdir(parents=True, exist_ok=True)
            logger.info(
                f"Historical filter output folder ensured: {self.historical_output_folder}"
            )

    # ======================================================================
    # PUBLIC API
    # ======================================================================

    def filter_all_files(self, pattern: str = "*.parquet") -> Tuple[int, int]:
        """
        Filter all parquet files in input_folder (flat) and, if configured,
        all parquet files under historical_input_folder (Hive tree).
        """
        flat_files = glob.glob(str(self.input_folder / pattern))
        historical_files = (
            list(self.historical_input_folder.rglob("*.parquet"))
            if self.historical_input_folder and self.historical_input_folder.exists()
            else []
        )

        all_files = [(Path(p), False) for p in flat_files] + \
                    [(p, True) for p in historical_files]

        if not all_files:
            logger.warning(
                f"No parquet files found in: {self.input_folder}"
                + (f" or {self.historical_input_folder}" if self.historical_input_folder else "")
            )
            return 0, 0

        logger.info(
            f"Filtering {len(flat_files)} flat file(s) "
            f"and {len(historical_files)} historical file(s)."
        )

        total_rows_before = 0
        total_rows_after  = 0
        files_processed   = 0
        files_failed      = 0
        n_total           = len(all_files)

        for idx, (parquet_path, is_historical) in tqdm(
            enumerate(all_files, start=1),
            total=n_total,
            desc="Filtering parquet files"
        ):
            try:
                output_path = self._output_path_for(parquet_path, is_historical)
                rows_before, rows_after = self.filter_single_file(parquet_path, output_path)

                total_rows_before += rows_before
                total_rows_after  += rows_after
                files_processed   += 1

                rate = (rows_after / rows_before * 100) if rows_before else 0
                logger.info(
                    f"[{idx}/{n_total}] {parquet_path.name}: "
                    f"{rows_before:,} -> {rows_after:,} rows ({rate:.1f}% kept)"
                )

            except Exception as e:
                files_failed += 1
                logger.error(f"Failed to filter {parquet_path.name}: {e}")

        logger.info("===============================================")
        logger.info("FILTERING SUMMARY")
        logger.info("===============================================")
        logger.info(f"Files processed successfully: {files_processed}")
        logger.info(f"Files failed: {files_failed}")
        logger.info(f"Total rows before: {total_rows_before:,}")
        logger.info(f"Total rows after: {total_rows_after:,}")

        if total_rows_before > 0:
            dropped    = total_rows_before - total_rows_after
            retention  = total_rows_after / total_rows_before * 100
            logger.info(f"Overall retention rate: {retention:.2f}%")
            logger.info(f"Total rows removed: {dropped:,}")

        return files_processed, files_failed

    # ======================================================================
    # PER-FILE PROCESSING
    # ======================================================================

    def filter_single_file(
        self,
        parquet_path: str | Path,
        output_path: Optional[Path] = None,
    ) -> Tuple[int, int]:
        """
        Filter a single parquet file and return (rows_before, rows_after).
        Streams the file in batches to keep peak RAM bounded.

        output_path overrides the default flat naming convention; used to
        preserve Hive subdirectory structure for historical files.
        """
        file_path = Path(parquet_path)
        logger.debug(f"Filtering file: {file_path.name}")

        pf = pq.ParquetFile(file_path)

        if pf.metadata.num_rows == 0:
            logger.warning(f"Empty parquet file skipped: {file_path.name}")
            return 0, 0

        schema_cols = pf.schema_arrow.names
        existing_columns = [c for c in self.columns_to_check if c in schema_cols]
        missing_columns  = [c for c in self.columns_to_check if c not in schema_cols]

        if missing_columns:
            logger.warning(
                f"{file_path.name}: Missing {len(missing_columns)} column(s): {missing_columns}"
            )

        if not existing_columns:
            logger.error(f"{file_path.name}: None of the filter columns exist.")
            return pf.metadata.num_rows, pf.metadata.num_rows

        if output_path is None:
            output_path = self.output_folder / f"{file_path.stem}_filtered.parquet"

        output_path.parent.mkdir(parents=True, exist_ok=True)

        rows_before = 0
        rows_after  = 0

        writer = pq.ParquetWriter(output_path, pf.schema_arrow, compression="snappy")
        try:
            for batch in pf.iter_batches(batch_size=64_000):
                df_batch = batch.to_pandas()
                rows_before += len(df_batch)

                df_clean = df_batch.dropna(subset=existing_columns)
                rows_after += len(df_clean)

                if not df_clean.empty:
                    writer.write_table(
                        pa.Table.from_pandas(df_clean, preserve_index=False)
                    )
        finally:
            writer.close()

        logger.debug(f"Saved filtered file -> {output_path}")
        return rows_before, rows_after

    # ======================================================================
    # VALIDATION
    # ======================================================================

    def validate_columns(
        self, sample_file: Optional[str] = None
    ) -> Dict[str, Union[str, int, List[str]]]:
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
            schema_cols = pq.read_schema(sample_file).names
            existing = [c for c in self.columns_to_check if c in schema_cols]
            missing  = [c for c in self.columns_to_check if c not in schema_cols]

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
    # INTERNAL HELPERS
    # ======================================================================

    def _output_path_for(self, parquet_path: Path, is_historical: bool) -> Path:
        """
        Compute the output path for a given input file.

        Flat daily files  -> output_folder/<stem>_filtered.parquet
        Historical files  -> historical_output_folder/<relative_partition_path>/<stem>_filtered.parquet
        """
        if not is_historical:
            return self.output_folder / f"{parquet_path.stem}_filtered.parquet"

        relative = parquet_path.relative_to(self.historical_input_folder)
        return (
            self.historical_output_folder
            / relative.parent
            / f"{parquet_path.stem}_filtered.parquet"
        )


# ======================================================================
# RUN WRAPPER (used by main.py)
# ======================================================================

def run_filter(config: dict) -> Tuple[int, int]:
    """
    Convenience wrapper so main.py can call the filter cleanly.
    """
    part_cfg = config.get("converter", {}).get("partitioning", {})
    historical_input = historical_output = None

    if part_cfg.get("enabled", False):
        historical_input  = config["paths"].get("parquet_historical_directory")
        historical_output = config["paths"].get("filtered_historical_directory")

    filterer = GDELTFilter(
        input_folder=config["paths"]["parquet_data_directory"],
        output_folder=config["paths"]["filtered_data_directory"],
        columns_to_check=config["filter"]["columns_to_check"],
        historical_input_folder=historical_input,
        historical_output_folder=historical_output,
    )
    return filterer.filter_all_files()
