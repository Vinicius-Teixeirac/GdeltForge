"""
checkpoint.py

Checkpoints for long scans (sample, crossref, the file index), so a run
stopped partway resumes where it left off and produces exactly the output
an uninterrupted run would.

A run's checkpoint is a directory under paths.checkpoint_directory, named
after a fingerprint of everything that decides the run's output: the
command and its parameters, the seed, every file read with its size and
modification time, and the gdeltforge version. A later run with the same
fingerprint resumes from it; a run with any of those changed doesn't see
it. It is deleted once the run has finished.

What a checkpoint holds is the caller's: a JSON-able state and named numpy
arrays, written together in one .npz replaced atomically, and rows to keep
(a full-set filter's, crossref's matches) as numbered Parquet parts,
written before the state that counts them, so a run stopped mid-save
leaves the previous checkpoint whole.

Provides:
    - CheckpointSettings: where checkpoints go and how often (or none)
    - Checkpoint: one run's checkpoint: load, save when due, finish
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from gdeltforge import __version__
from gdeltforge.utils.io import read_parquet_path
from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

# Bumped when what a checkpoint holds changes meaning: one of another
# version is ignored (and removed), never read.
_FORMAT = "1"
_STATE_FILE = "state.npz"
_HEADER = "__header__"


@dataclass(frozen=True)
class CheckpointSettings:
    """paths.checkpoint_directory and io.checkpoint_every_s for one run;
    directory None means no checkpoints."""
    directory: Path | None
    every_s: float = 60.0

    @property
    def enabled(self) -> bool:
        return self.directory is not None


NO_CHECKPOINTS = CheckpointSettings(None)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=str)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    return str(value)


def fingerprint(key: Mapping[str, Any]) -> str:
    """A stable hash of key (sorted JSON) and the gdeltforge version."""
    text = json.dumps(
        {"key": key, "version": __version__, "format": _FORMAT},
        sort_keys=True, default=_json_default,
    )
    return hashlib.sha256(text.encode()).hexdigest()[:20]


def file_facts(files: list[Path]) -> list[tuple[str, int, int]]:
    """Each file's absolute path, size and modification time, for a
    fingerprint: a file changed since the checkpoint makes it another run."""
    facts = []
    for f in files:
        st = os.stat(f)
        facts.append((os.path.abspath(f), st.st_size, st.st_mtime_ns))
    return facts


class Checkpoint:
    """
    One run's checkpoint. `kind` names what it is for (sample-random,
    crossref-gkg1, ...); `key` is everything that decides the run's output.
    With settings disabled every method is a no-op and load returns None.
    """

    def __init__(self, settings: CheckpointSettings, kind: str, key: Mapping[str, Any]):
        self.settings = settings
        self.kind = kind
        self.path: Path | None = None
        if settings.directory is not None:
            self.path = Path(settings.directory) / f"{kind}-{fingerprint(key)}"
        self._last_save = time.monotonic()
        self._parts = 0

    @property
    def enabled(self) -> bool:
        return self.path is not None

    def due(self) -> bool:
        """Whether io.checkpoint_every_s has passed since the last save (or
        since the run began)."""
        return self.enabled and time.monotonic() - self._last_save >= self.settings.every_s

    def load(self) -> tuple[dict, dict[str, np.ndarray], list[pl.DataFrame]] | None:
        """(state, arrays, parts) of the saved checkpoint, or None when there
        is none or it can't be read (it is then removed and the run starts
        over)."""
        if self.path is None or not (self.path / _STATE_FILE).is_file():
            return None
        try:
            with np.load(self.path / _STATE_FILE, allow_pickle=False) as saved:
                header = json.loads(bytes(saved[_HEADER]).decode())
                arrays = {k: saved[k] for k in saved.files if k != _HEADER}
            if header.get("format") != _FORMAT or header.get("kind") != self.kind:
                raise ValueError(f"format {header.get('format')!r}, kind {header.get('kind')!r}")
            # read_parquet_path hands polars the schema: a polars read that
            # works it out itself maps the file, even unmapped
            # (utils.io.read_parquet_schema).
            parts = [
                read_parquet_path(self.path / f"part-{i:06d}.parquet")
                for i in range(header["parts"])
            ]
        except (OSError, ValueError, KeyError, pl.exceptions.PolarsError) as e:
            logger.warning(f"Could not read the checkpoint {self.path} ({e}); starting over.")
            self.discard()
            return None
        self._parts = header["parts"]
        logger.info(f"Resuming from the checkpoint {self.path} (saved {header['saved_at']})")
        return header["state"], arrays, parts

    def save(
        self, state: Mapping[str, Any], arrays: Mapping[str, np.ndarray] | None = None,
        new_parts: list[pl.DataFrame] | None = None,
    ) -> None:
        """Save state and arrays, and append new_parts to the rows kept so
        far. A failure (a full disk) only warns: the run goes on, and a
        stop then resumes from the previous checkpoint."""
        if self.path is None:
            return
        try:
            self.path.mkdir(parents=True, exist_ok=True)
            for part in new_parts or []:
                part.write_parquet(self.path / f"part-{self._parts:06d}.parquet")
                self._parts += 1
            header = {
                "format": _FORMAT, "kind": self.kind, "parts": self._parts,
                "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"), "state": state,
            }
            payload = {
                **(arrays or {}),
                _HEADER: np.frombuffer(
                    json.dumps(header, default=_json_default).encode(), dtype=np.uint8
                ),
            }
            tmp = self.path / f"{_STATE_FILE}.{os.getpid()}.tmp"
            with open(tmp, "wb") as f:
                np.savez(f, **payload)
            os.replace(tmp, self.path / _STATE_FILE)
        except OSError as e:
            logger.warning(f"Could not save the checkpoint {self.path}: {e}")
        else:
            logger.debug(f"Saved the checkpoint {self.path}")
        self._last_save = time.monotonic()

    def discard(self) -> None:
        """Remove the checkpoint: the run finished, or it can't be used."""
        if self.path is not None and self.path.exists():
            shutil.rmtree(self.path, ignore_errors=True)
        self._parts = 0


def rng_state(generator: np.random.Generator) -> dict:
    """A numpy Generator's state, JSON-able."""
    return dict(generator.bit_generator.state)


def restore_rng(generator: np.random.Generator, state: dict) -> None:
    """Put a Generator back in a state rng_state saved."""
    generator.bit_generator.state = state
