from __future__ import annotations

from bisect import bisect_right
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pyarrow.dataset as ds

from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)


class FileIndex:
    """
    Efficient global-row-index -> (file, relative-row) mapping.
    Uses PyArrow Dataset metadata + binary search for fast lookup.
    """

    def __init__(self, folder_or_files: Iterable[Path]):
        # Accept folder or list of paths
        self.folder, self.files = self._resolve_paths(folder_or_files)

        logger.info("Building FileIndex using PyArrow metadata...")
        self._build_arrow_metadata()
        logger.info(
            f"FileIndex: {len(self.files)} files, {self.total_rows:,} total rows."
        )

    # ----------------------------------------------------------
    # Resolve path input
    # ----------------------------------------------------------
    def _resolve_paths(self, folder_or_files):
        if isinstance(folder_or_files, (str, Path)):
            folder = Path(folder_or_files)
            files = sorted(folder.glob("*.parquet"))
        else:
            files = sorted(folder_or_files)
            folder = None

        if not files:
            raise FileNotFoundError("No parquet files found.")

        return folder, files

    # ----------------------------------------------------------
    # Build metadata via PyArrow
    # ----------------------------------------------------------
    def _build_arrow_metadata(self):
        dataset = ds.dataset(self.files, format="parquet")

        self.stops: list[int] = []    # cumulative row end positions (+1)
        self.starts: list[int] = []   # starting global index for each file
        self.counts: list[int] = []   # rows per file

        fragments = list(dataset.get_fragments())
        cumulative = 0
        for fragment in fragments:
            nrows = fragment.metadata.num_rows
            self.starts.append(cumulative)
            cumulative += nrows
            self.stops.append(cumulative)
            self.counts.append(nrows)

        self.total_rows = cumulative
        self.files = [Path(f.path) for f in fragments]

    # ----------------------------------------------------------
    # Fast lookup via binary search
    # ----------------------------------------------------------
    def lookup(self, g: int):
        """
        Given global row index g, return (file_index, relative_row).
        """
        j = bisect_right(self.starts, g) - 1
        rel = g - self.starts[j]
        return j, rel

    # ----------------------------------------------------------
    # Group a batch of global indices by file
    # ----------------------------------------------------------
    def group_indices_by_file(self, global_indices: np.ndarray) -> dict[Path, list[int]]:
        """
        Input: array of global row indices
        Output: dict: file_path -> [relative_indices]
        """
        global_indices = np.sort(global_indices)

        result: dict[Path, list[int]] = {}
        j = 0  # pointer for file index

        for g in global_indices:
            # Move to the correct file
            while g >= self.stops[j]:
                j += 1

            rel = g - self.starts[j]
            file_path = self.files[j]
            result.setdefault(file_path, []).append(rel)

        return result
