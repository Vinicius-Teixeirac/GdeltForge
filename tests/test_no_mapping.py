"""io.memory_map off means no data file is ever memory-mapped: polars maps a
whole file to read its footer whenever it works out a schema itself, even
with POLARS_FORCE_ASYNC=1, so every read path must hand polars the schema
(gdeltforge.utils.io.read_parquet_schema). Linux only.

Where strace runs, every mmap call is counted (`strace -f -y` names the
file behind each one), so a mapping made and released at once is seen.
Without it, a thread polls /proc/self/maps, which misses a mapping that
lives between two polls: on one build it caught 3 of 6 read paths that
mapped a footer, where strace caught all 6."""

import os
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="memory maps are watched on Linux only"
)

# Six small Parquet files, written before any read path is watched.
SETUP = textwrap.dedent('''
    import sys
    from pathlib import Path
    import numpy as np
    import polars as pl

    folder = Path(sys.argv[1])
    for i in range(6):
        pl.DataFrame({
            "GlobalEventID": np.arange(i * 50_000, (i + 1) * 50_000),
            "QuadClass": np.random.default_rng(i).integers(1, 5, 50_000),
            "V2DOCUMENTIDENTIFIER": [f"https://example.org/{j}" for j in range(50_000)],
        }).write_parquet(folder / f"2020010{i + 1}.export.parquet")
''')

# One read path, run `repeat` times. With "poll", a thread watches
# /proc/self/maps meanwhile and the script prints how many data files it
# saw mapped; under strace it prints nothing and the trace is counted.
RUN = textwrap.dedent('''
    import os, sys, threading
    from pathlib import Path
    if sys.argv[4] != "mapped":
        os.environ["POLARS_FORCE_ASYNC"] = "1"
    import polars as pl

    folder, out, name, mode, repeat = (
        Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5]))
    files = sorted(folder.glob("*.parquet"))
    names = {f.name for f in files}

    from gdeltforge.cleaning.cleaner import GDELTCleaner
    from gdeltforge.crossref.crossref import _files_with_matches
    from gdeltforge.utils.io import (
        read_file_schemas, read_parquet_path, scan_dataset_reconciled, sink_parquet_atomic,
    )

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
        # polars working out the schema itself, mapping allowed: the
        # counters' own check that a mapping is seen.
        "plain polars read": lambda: pl.read_parquet(files[0]),
    }
    seen = set()
    stop = threading.Event()

    def watch():
        while not stop.is_set():
            with open("/proc/self/maps") as maps:
                for line in maps:
                    base = os.path.basename(line.split()[-1]) if line.strip() else ""
                    if base in names:
                        seen.add(base)

    watcher = threading.Thread(target=watch, daemon=True)
    if mode == "poll":
        watcher.start()
    for _ in range(repeat):
        paths[name]()
    if mode == "poll":
        stop.set(); watcher.join()
        print(len(seen))
''')

READ_PATHS = [
    "schemas", "reconciled scan", "read_parquet_path", "clean", "aggregate sink",
    "crossref first pass",
]


def _strace_works() -> bool:
    """strace is installed and may trace here (a container can refuse ptrace)."""
    if shutil.which("strace") is None:
        return False
    probe = subprocess.run(
        ["strace", "-f", "-e", "trace=none", "-o", os.devnull, sys.executable, "-c", "pass"],
        capture_output=True,
    )
    return probe.returncode == 0


STRACE = sys.platform.startswith("linux") and _strace_works()


@pytest.fixture(scope="module")
def data(tmp_path_factory) -> tuple[Path, Path]:
    root = tmp_path_factory.mktemp("no_mapping")
    folder, out = root / "in", root / "out"
    folder.mkdir()
    out.mkdir()
    subprocess.run([sys.executable, "-c", SETUP, str(folder)], check=True)
    return folder, out


def _mapped(data: tuple[Path, Path], name: str, mode: str) -> int:
    """How many mmap calls (strace) or data files seen mapped (polling)
    the read path `name` made on the data files."""
    folder, out = data
    if STRACE:
        trace = out / f"{name.replace(' ', '_')}-{mode}.strace"
        subprocess.run(
            ["strace", "-f", "-y", "-e", "trace=mmap", "-o", str(trace),
             sys.executable, "-c", RUN, str(folder), str(out), name, mode, "3"],
            check=True, capture_output=True,
        )
        # A file-backed call names its file: mmap(..., 3</path/file>, 0).
        inside = re.compile(r"<" + re.escape(str(folder.resolve())) + r"/[^>]*\.parquet>")
        return sum(1 for line in trace.read_text().splitlines() if inside.search(line))
    result = subprocess.run(
        [sys.executable, "-c", RUN, str(folder), str(out), name, "poll", "15"],
        check=True, capture_output=True, text=True,
    )
    return int(result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize("name", READ_PATHS)
def test_no_read_path_maps_a_data_file(data, name):
    assert _mapped(data, name, "unmapped") == 0


@pytest.mark.skipif(not STRACE, reason="needs strace: polling can miss a short mapping")
def test_the_mmap_count_sees_a_mapping_when_mapping_is_on(data):
    """Without POLARS_FORCE_ASYNC, polars maps the file it reads: the
    count must see it, or a zero above proves nothing."""
    assert _mapped(data, "plain polars read", "mapped") > 0
