"""
samplers.py

Sampling utilities for GDELT parquet datasets.
Provides:
    - IndexedSampler
    - DailySampler
    - FilteredSampler
"""

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Union, Tuple, Optional, Set, List
from enum import Enum

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from tqdm import tqdm

from .rng import ReproducibleRNG
from .indexer import FileIndex
from utils.config import load_config
from utils.logging import get_logger

logger = get_logger(__name__)

FilterCondition = Union[
    str, int, float, bool,
    List[Any],
    Tuple[Any, Any],
    Dict[str, Any],
]

# ----------------------------------------------------------
# Filter operation types
# ----------------------------------------------------------
class FilterType(Enum):
    EQUALS = "equals"
    IN_LIST = "in_list"
    RANGE = "range"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    BETWEEN = "between"


# ----------------------------------------------------------
# Indexed Sampler
# ----------------------------------------------------------
class IndexedSampler:
    """
    Draws samples from many parquet files without loading the entire dataset.
    Uses FileIndex to resolve "global row index" → (file, row).
    """

    def __init__(self, folder_path: str, random_state: Optional[int] = 42):
        self.folder = Path(folder_path)
        parquet_files = sorted(self.folder.glob("*.parquet"))

        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {self.folder}")

        self.rng = ReproducibleRNG(random_state)
        self.index = FileIndex(parquet_files)

        logger.info(f"IndexedSampler: {len(self.index.files)} files, "
                    f"{self.index.total_rows:,} total rows.")

    def get_random_sample(self, n: int) -> pd.DataFrame:
        """Sample n rows uniformly across all parquet files."""
        if n > self.index.total_rows:
            raise ValueError("Requested sample size > total available rows")

        random_indices = self.rng.choice(self.index.total_rows, n, replace=False)
        logger.info(f"Sampling {n} rows across {len(self.index.files)} files")

        # group sampled indices into: file_path → [indices]
        indices_by_file = self.index.group_indices_by_file(random_indices)
        logger.info(f"Mapped the indices to the correspondent files")


        sampled = []
        for file_path, relative_rows in tqdm(indices_by_file.items(), desc="Loading samples"):
            df = pd.read_parquet(file_path)
            sampled.append(df.iloc[relative_rows])

        return pd.concat(sampled, ignore_index=True)


# ----------------------------------------------------------
# Daily Sampler
# ----------------------------------------------------------
class DailySampler:
    """
    For each day, sample a fixed number of rows per parquet file.
    """

    def __init__(self, folder_path: str, random_state: Optional[int] = 42):
        self.folder = Path(folder_path)
        self.rng = ReproducibleRNG(random_state)

    def get_daily_samples(self, samples_per_day: int = 10) -> pd.DataFrame:
        daily = {}

        for file_path in tqdm(self.folder.glob("*.parquet"), desc="Daily sampling"):
            df = pd.read_parquet(file_path)
            if "Day" not in df.columns:
                continue

            for day, group in df.groupby("Day"):
                size = min(samples_per_day, len(group))
                if size == 0:
                    continue

                idx = self.rng.choice(len(group), size=size, replace=False)
                sample = group.iloc[idx]

                daily.setdefault(day, []).append(sample)

        if not daily:
            return pd.DataFrame()

        # flatten
        all_samples = [pd.concat(chunks) for chunks in daily.values()]
        return pd.concat(all_samples, ignore_index=True)


# ----------------------------------------------------------
# FilteredSampler  
# ----------------------------------------------------------
class FilteredSampler:
    """
    Filter + sample from folder of parquet files using PyArrow dataset scanning + batch streaming.
    """

    def __init__(
        self,
        folder_path: str,
        columns: Optional[Set[str]] = None,
        filter_dict: Optional[Dict[str, Any]] = None,
        random_state: Optional[int] = 42,
    ):
        self.config = load_config()
        self.gdelt_columns = set(self.config["columns"]["gdelt_event"])

        self.folder = Path(folder_path)
        self.columns = columns or self.gdelt_columns
        self.filter_dict = filter_dict or {}
        self.rng = ReproducibleRNG(random_state)

        self._validate_columns()
        self._validate_filter_dict()

    # ---------- validation ----------
    def _validate_columns(self):
        valid = self.gdelt_columns
        invalid = self.columns - valid
        if invalid:
            raise ValueError(f"Invalid columns: {invalid}")

    def _validate_filter_dict(self):
        def validate_block(block):
            if not isinstance(block, dict):
                raise ValueError("filter_dict must be dict/nested dicts")

            for key, val in block.items():
                if key in ("AND", "OR"):
                    if not isinstance(val, dict):
                        raise ValueError(f"{key} must contain a dict")
                    validate_block(val)
                else:
                    if key not in self.gdelt_columns:
                        raise ValueError(f"Invalid filter column: {key}")

        validate_block(self.filter_dict)

    # ---------- convert simple condition -> pyarrow expression ----------
    def _expr_for_condition(self, column: str, cond: Any) -> ds.Expression:
        f = ds.field(column)

        # scalar equality
        if isinstance(cond, (str, int, float, bool)):
            return f == cond

        # list -> isin
        if isinstance(cond, list):
            # use pyarrow compute is_in via dataset expression .isin
            return f.isin(cond)

        # tuple -> range inclusive
        if isinstance(cond, tuple) and len(cond) == 2:
            lo, hi = cond
            return (f >= lo) & (f <= hi)

        # dict with explicit op
        if isinstance(cond, dict):
            op = cond.get("op")
            if op == FilterType.EQUALS.value:
                return f == cond["value"]
            if op == FilterType.IN_LIST.value:
                return f.isin(cond["values"])
            if op == FilterType.GREATER_THAN.value:
                return f > cond["value"]
            if op == FilterType.LESS_THAN.value:
                return f < cond["value"]
            if op in (FilterType.RANGE.value, FilterType.BETWEEN.value):
                return (f >= cond["min"]) & (f <= cond["max"])

        raise ValueError(f"Invalid condition for {column}: {cond}")

    # ---------- recursive builder: filter_dict -> pyarrow.dataset expression ----------
    def _build_expression(self, block: Dict[str, Any]) -> Optional[ds.Expression]:
        """
        Return a pyarrow.dataset Expression or None if block is empty.
        Supports nested AND/OR and base column conditions.
        """
        if not block:
            return None

        expr = None

        for key, val in block.items():
            if key == "AND":
                sub = self._build_expression(val)
                if sub is None:
                    continue
                expr = sub if expr is None else (expr & sub)

            elif key == "OR":
                sub = self._build_expression(val)
                if sub is None:
                    continue
                expr = sub if expr is None else (expr | sub)

            else:
                # base column condition
                sub = self._expr_for_condition(key, val)
                expr = sub if expr is None else (expr & sub)

        return expr

    # ---------- dataset scanner and batch iterator ----------
    def _dataset(self, needed) -> ds.dataset:
        schema = pa.schema([pa.field(c, pa.string(), nullable=True) for c in needed])

        return ds.dataset(
            list(self.folder.glob("*.parquet")),
            schema=schema,
            format="parquet"
        )

    def _batches(self, needed_columns: List[str]):
        """
        Yield pyarrow.RecordBatch objects matching the configured filter,
        using pyarrow.dataset Scanner with filter pushdown.
        """
        dataset = self._dataset(needed_columns)
        expr = self._build_expression(self.filter_dict)

        # Use scanner to stream record batches
        scanner = dataset.scanner(columns=needed_columns, filter=expr, batch_size=64_000)
        for batch in scanner.to_batches():
            yield batch

    # ---------- API ----------
    def filter_dataset(self) -> pd.DataFrame:
        needed = list(self.columns | {
            key for key in self.filter_dict.keys()
            if key not in {"AND", "OR", "NOT"} and not isinstance(key, tuple)
        })

        frames: List[pd.DataFrame] = []
        for batch in tqdm(self._batches(needed), desc="Filtering parquet files"):
            try:
                df_batch = batch.to_pandas()  # only convert each batch
                if not df_batch.empty:
                    frames.append(df_batch[needed])
            except Exception as e:
                logger.warning(f"Skipping batch due to error: {e}")

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ---------- reservoir sampling for random sample ----------
    def get_random_sample(self, n: int) -> pd.DataFrame:
        if n <= 0:
            return pd.DataFrame()

        needed = list(self.columns | {
            key for key in self.filter_dict.keys()
            if key not in {"AND", "OR", "NOT"} and not isinstance(key, tuple)
        })

        reservoir: List[pd.DataFrame] = []  # store selected rows as 1-row DataFrames
        total_seen = 0

        for batch in tqdm(self._batches(needed), desc="Sampling (random)"):
            df_batch = batch.to_pandas()
            if df_batch.empty:
                continue

            # iterate rows in batch but operate on numpy indices for speed
            for i in range(len(df_batch)):
                row = df_batch.iloc[[i]]  # keep as DataFrame
                total_seen += 1
                if len(reservoir) < n:
                    reservoir.append(row)
                else:
                    # replace with decreasing probability
                    j = self.rng.randint(0, total_seen - 1)
                    if j < n:
                        reservoir[j] = row

        if not reservoir:
            return pd.DataFrame()

        # concat reservoir items and reset index
        sample_df = pd.concat(reservoir, ignore_index=True)
        # If we saw fewer than n rows, return all
        if len(sample_df) <= n:
            return sample_df.reset_index(drop=True)
        # Otherwise randomly choose n rows deterministically from reservoir (already uniform)
        idx = self.rng.choice(len(sample_df), size=n, replace=False)

        # Keep only original gdelt_event columns ordering if available
        keep_cols = [c for c in self.config["columns"]["gdelt_event"] if c in sample_df.columns]

        return sample_df.iloc[idx].reset_index(drop=True)[keep_cols]

    # ---------- stratified reservoir sampling ----------
    def get_stratified_sample(self, stratify_col: str, n_per_group: int) -> pd.DataFrame:
        if n_per_group <= 0:
            return pd.DataFrame()

        needed = list(self.columns | {stratify_col} | {
            key for key in self.filter_dict.keys()
            if key not in {"AND", "OR", "NOT"} and not isinstance(key, tuple)
        })

        # map group_value -> list of row-DataFrames (reservoir)
        reservoirs: Dict[Any, List[pd.DataFrame]] = {}
        counts: Dict[Any, int] = {}

        for batch in tqdm(self._batches(needed), desc="Sampling (stratified)"):
            df_batch = batch.to_pandas()
            if df_batch.empty or stratify_col not in df_batch.columns:
                continue

            # iterate rows; could be optimized with vectorized group ops but reservoir needs per-row logic
            for i in range(len(df_batch)):
                row = df_batch.iloc[[i]]
                g = row.iloc[0][stratify_col]
                # normalize NaN group keys
                if pd.isna(g):
                    g = None

                counts[g] = counts.get(g, 0) + 1
                if g not in reservoirs:
                    reservoirs[g] = []

                r = reservoirs[g]
                cnt = counts[g]
                if len(r) < n_per_group:
                    r.append(row)
                else:
                    j = self.rng.randint(0, cnt - 1)
                    if j < n_per_group:
                        r[j] = row

        # flatten reservoirs
        parts: List[pd.DataFrame] = []
        for group, rows in reservoirs.items():
            parts.append(pd.concat(rows, ignore_index=True))

        if not parts:
            return pd.DataFrame()

        sample = pd.concat(parts, ignore_index=True)

        # Keep only original gdelt_event columns ordering if available
        keep_cols = [c for c in self.config["columns"]["gdelt_event"] if c in sample.columns]
        return sample[keep_cols]