"""io.memory_map off means no data file is ever memory-mapped: polars maps a
whole file to read its footer whenever it works out a schema itself, even
with POLARS_FORCE_ASYNC=1, so every read path must hand polars the schema
(gdeltforge.utils.io.read_parquet_schema). Linux only: it watches
/proc/self/maps."""

import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="watches /proc/self/maps"
)

SCRIPT = textwrap.dedent('''
    import os, sys, threading
    from pathlib import Path
    os.environ["POLARS_FORCE_ASYNC"] = "1"
    import numpy as np
    import polars as pl

    folder = Path(sys.argv[1])
    out = Path(sys.argv[2])
    files = []
    for i in range(6):
        f = folder / f"2020010{i + 1}.export.parquet"
        pl.DataFrame({
            "GlobalEventID": np.arange(i * 50_000, (i + 1) * 50_000),
            "QuadClass": np.random.default_rng(i).integers(1, 5, 50_000),
            "V2DOCUMENTIDENTIFIER": [f"https://example.org/{j}" for j in range(50_000)],
        }).write_parquet(f)
        files.append(f)
    names = {f.name for f in files}

    from gdeltforge.cleaning.cleaner import GDELTCleaner
    from gdeltforge.crossref.crossref import _files_with_matches
    from gdeltforge.utils.io import (
        read_file_schemas, read_parquet_path, scan_dataset_reconciled, sink_parquet_atomic,
    )

    seen = set()
    stop = threading.Event()

    def watch():
        while not stop.is_set():
            with open("/proc/self/maps") as maps:
                for line in maps:
                    name = os.path.basename(line.split()[-1]) if line.strip() else ""
                    if name in names:
                        seen.add(name)

    paths = {
        "schemas": lambda: read_file_schemas(files),
        "reconciled scan": lambda: scan_dataset_reconciled(files).collect(),
        "read_parquet_path": lambda: read_parquet_path(files[0]),
        "clean": lambda: GDELTCleaner(str(folder), str(out / "c"), ["QuadClass"])._clean_file(
            files[0], out / "c.parquet"),
        "aggregate sink": lambda: sink_parquet_atomic(
            scan_dataset_reconciled(files), out / "agg.parquet"),
        "crossref first pass": lambda: _files_with_matches(
            files, "V2DOCUMENTIDENTIFIER", {"https://example.org/7"}),
    }
    for name, run in paths.items():
        seen.clear(); stop.clear()
        t = threading.Thread(target=watch, daemon=True); t.start()
        for _ in range(15):
            run()
        stop.set(); t.join()
        print(f"{name}: {len(seen)}")
''')


def test_no_read_path_maps_a_data_file(tmp_path):
    (tmp_path / "in").mkdir()
    (tmp_path / "out").mkdir()
    out = subprocess.run(
        [sys.executable, "-c", SCRIPT, str(tmp_path / "in"), str(tmp_path / "out")],
        capture_output=True, text=True, check=True,
    )
    counts = dict(line.rsplit(": ", 1) for line in out.stdout.strip().splitlines())
    assert counts and all(n == "0" for n in counts.values()), counts
