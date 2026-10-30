from __future__ import annotations

import hashlib
import os
from bisect import bisect_right
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from gdeltforge.utils.io import _map_files, clearer_dataset_errors
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

# Bumped whenever the saved index's columns or their meaning change: a
# saved index of another version is rebuilt, never read.
_INDEX_VERSION = "1"
_INDEX_METADATA_KEY = "gdeltforge:file_index"
# Not .parquet: a saved index inside a data directory would otherwise be
# globbed and sampled as data (index_path_for also refuses that).
INDEX_SUFFIX = ".index"


@dataclass(frozen=True)
class _Entry:
    """One file's footer facts, and the size and mtime they were read at."""
    size: int
    mtime_ns: int
    row_groups: tuple[int, ...]
    schema: bytes  # Arrow IPC-serialized schema


def index_path_for(index_directory: str | Path, roots: list[Path]) -> Path:
    """
    The saved index for a sample reading `roots` (its source directory,
    and historical directory if any): one file per set of roots, named
    after the first and a hash of all their absolute paths. Refused when
    it would sit inside one of them.
    """
    directory = Path(index_directory)
    _refuse_inside_roots(directory / "x", roots)
    key = "\n".join(os.path.abspath(r) for r in roots)
    digest = hashlib.sha256(key.encode()).hexdigest()[:12]
    first = Path(os.path.abspath(roots[0]))
    return directory / f"{first.parent.name}_{first.name}-{digest}{INDEX_SUFFIX}"


def _refuse_inside_roots(path: Path, roots: list[Path]) -> None:
    target = Path(os.path.abspath(path))
    for root in roots:
        if target.is_relative_to(os.path.abspath(root)):
            raise ValueError(
                f"The file index {path} would sit inside {root}, which the sample reads. "
                f"Point paths.index_directory (or --file-index) outside every data directory."
            )


class FileIndex:
    """
    Efficient global-row-index -> (file, relative-row) mapping, with each
    file's row groups and schema.

    Every file's footer is read once, a few at a time (_map_files). With
    `saved`, the footer facts are kept in that file and reused while a
    file's size and mtime are unchanged, so a later sample over the same
    files reads no footers at all (one stat per file instead).
    """

    def __init__(
        self,
        folder_or_files: Iterable[Path],
        saved: Path | None = None,
        roots: list[Path] | None = None,
        label: str | None = None,
    ):
        # Accept folder or list of paths
        self.folder, self.files = self._resolve_paths(folder_or_files)
        self.saved = saved
        # What a footer-read error names (clearer_dataset_errors).
        self._label = label or f"{len(self.files)} parquet file(s)"
        if saved is not None:
            _refuse_inside_roots(saved, roots or ([self.folder] if self.folder else []))

        logger.info("Building FileIndex from Parquet footers...")
        self._build_metadata()
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
    # Build metadata from the footers (or the saved index)
    # ----------------------------------------------------------
    def _build_metadata(self):
        self.stops: list[int] = []    # cumulative row end positions (+1)
        self.starts: list[int] = []   # starting global index for each file
        self.counts: list[int] = []   # rows per file

        stats = _map_files(os.stat, self.files)
        known = self._load_saved()
        keys = [os.path.abspath(f) for f in self.files]
        entries: list[_Entry | None] = []
        for key, st in zip(keys, stats, strict=True):
            entry = known.get(key)
            fresh = entry is not None and (entry.size, entry.mtime_ns) == (
                st.st_size, st.st_mtime_ns
            )
            entries.append(entry if fresh else None)

        missing = [i for i, e in enumerate(entries) if e is None]
        if missing:
            # A corrupt/non-parquet file raises here, named clearly.
            # Schemas interned as they're read: most files share one.
            interned: dict[bytes, bytes] = {
                e.schema: e.schema for e in known.values()
            }

            def read(f: Path) -> tuple[tuple[int, ...], bytes]:
                row_groups, schema = _read_footer(f)
                return row_groups, interned.setdefault(schema, schema)

            with clearer_dataset_errors(self._label):
                read_entries = _map_files(read, [self.files[i] for i in missing])
            for i, (row_groups, schema) in zip(missing, read_entries, strict=True):
                entries[i] = _Entry(stats[i].st_size, stats[i].st_mtime_ns, row_groups, schema)
        logger.info(
            f"FileIndex: {len(missing):,} footer(s) read, "
            f"{len(self.files) - len(missing):,} from the saved index."
        )

        # One polars schema per distinct Arrow schema: most files share
        # one, and a dict per file over hundreds of thousands of files
        # costs gigabytes.
        converted: dict[bytes, dict[str, pl.DataType]] = {}
        self.schemas: dict[Path, dict[str, pl.DataType]] = {}
        self.row_groups: dict[Path, tuple[int, ...]] = {}
        cumulative = 0
        for f, entry in zip(self.files, entries, strict=True):
            assert entry is not None
            if entry.schema not in converted:
                converted[entry.schema] = _polars_schema(entry.schema)
            self.schemas[f] = converted[entry.schema]
            self.row_groups[f] = entry.row_groups
            nrows = sum(entry.row_groups)
            self.starts.append(cumulative)
            cumulative += nrows
            self.stops.append(cumulative)
            self.counts.append(nrows)

        self.total_rows = cumulative
        # Every column any file has, in first-seen order: IndexedSampler
        # checks --columns against it.
        names: dict[str, None] = {}
        for schema in converted.values():
            names.update(dict.fromkeys(schema))
        self.schema_names: list[str] = list(names)

        if self.saved is not None and missing:
            known.update(zip(keys, (e for e in entries if e is not None), strict=True))
            self._save(known)

    # ----------------------------------------------------------
    # The saved index
    # ----------------------------------------------------------
    def _load_saved(self) -> dict[str, _Entry]:
        """The saved index's entries by absolute path; empty when there's
        none yet, or it can't be read (it's then rebuilt)."""
        if self.saved is None or not self.saved.exists():
            return {}
        try:
            metadata = pq.read_schema(self.saved).metadata or {}
            version = metadata.get(_INDEX_METADATA_KEY.encode(), b"").decode()
            if version != _INDEX_VERSION:
                logger.warning(
                    f"The file index {self.saved} has version {version or 'unknown'!r}, "
                    f"not {_INDEX_VERSION!r}; rebuilding it."
                )
                return {}
            table = pq.read_table(self.saved)
            # Dictionary-encoded: one bytes object per distinct schema,
            # shared by every file holding it.
            encoded = table.column("schema").combine_chunks()
            schemas = [s.as_py() for s in encoded.dictionary]
            schema_ids = encoded.indices.to_pylist()
            columns = table.select(["path", "size", "mtime_ns", "row_groups"]).to_pydict()
        except (OSError, KeyError, AttributeError, pa.ArrowException) as e:
            logger.warning(f"Could not read the file index {self.saved} ({e}); rebuilding it.")
            return {}
        return {
            path: _Entry(size, mtime_ns, tuple(row_groups), schemas[schema_id])
            for path, size, mtime_ns, row_groups, schema_id in zip(
                columns["path"], columns["size"], columns["mtime_ns"],
                columns["row_groups"], schema_ids, strict=True,
            )
        }

    def _save(self, entries: dict[str, _Entry]) -> None:
        """Write the saved index; a failure (read-only directory, full
        disk) only warns, since the index is a cache."""
        assert self.saved is not None
        paths = sorted(entries)
        table = pa.table(
            {
                "path": pa.array(paths, pa.string()),
                "size": pa.array([entries[p].size for p in paths], pa.int64()),
                "mtime_ns": pa.array([entries[p].mtime_ns for p in paths], pa.int64()),
                "row_groups": pa.array(
                    [list(entries[p].row_groups) for p in paths], pa.list_(pa.int64())
                ),
                "schema": pa.array(
                    [entries[p].schema for p in paths], pa.binary()
                ).dictionary_encode(),
            }
        ).replace_schema_metadata({_INDEX_METADATA_KEY: _INDEX_VERSION})
        # Written through a temp file and a rename, as utils.io's atomic
        # writers do: a run stopped mid-write leaves the old index or none.
        tmp = self.saved.with_name(f"{self.saved.name}.{os.getpid()}.tmp")
        try:
            self.saved.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, tmp)
            os.replace(tmp, self.saved)
        except OSError as e:
            tmp.unlink(missing_ok=True)
            logger.warning(f"Could not save the file index {self.saved}: {e}")
        else:
            logger.info(f"Saved the file index ({len(paths):,} files) -> {self.saved}")

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


def _read_footer(file: Path) -> tuple[tuple[int, ...], bytes]:
    """A file's row-group sizes and its Arrow schema, from one footer read."""
    metadata = pq.ParquetFile(file).metadata
    row_groups = tuple(metadata.row_group(i).num_rows for i in range(metadata.num_row_groups))
    # Without the file's metadata (a cleaned file's marker names its own
    # source), so files with the same columns share one schema.
    schema = metadata.schema.to_arrow_schema().remove_metadata()
    return row_groups, schema.serialize().to_pybytes()


def _polars_schema(serialized: bytes) -> dict[str, pl.DataType]:
    """The polars schema of a serialized Arrow schema: the one
    pl.read_parquet_schema reads from the same footer."""
    schema = pa.ipc.read_schema(pa.py_buffer(serialized))
    frame = pl.from_arrow(schema.empty_table())
    assert isinstance(frame, pl.DataFrame)
    return dict(frame.schema.items())
