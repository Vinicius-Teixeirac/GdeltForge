"""
concurrency.py

Sizes the polars thread pools inside convert/clean/aggregate's worker
processes, and bounds how many files one command reads at once
(io.max_concurrent_reads).

Every worker is a separately spawned interpreter, and polars sizes its
own pools to the whole machine by default in each one of them: compute
threads, async I/O threads, and max_concurrent_scans (files a multi-file
scan reads at once) all equal the core count. max_workers alone
therefore only caps processes. N workers on a C-core machine used to
carry roughly N x 2C threads, and each worker's memory grew with its
own pool size, above all with how many files it read at once. pyarrow,
imported in every worker too, sizes its CPU pool the same way. See
docs/configuration.md's "Worker pools and polars threads" section for
the measurements behind this module.

Provides:
    - WorkerPlan / plan_workers: resolve a stage's effective worker count
      and each worker's polars thread and file-read budget
    - polars_worker_env: apply a WorkerPlan to the worker processes a
      ProcessPoolExecutor spawns inside it
    - polars_scan_limit: bound concurrent file reads for polars work
      running in this process (sample, crossref)
    - exported_scan_limit: the POLARS_MAX_CONCURRENT_SCANS the user
      exported, checked before polars reads anything
"""

from __future__ import annotations

import math
import os
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass

from gdeltforge.utils.logging import get_logger

logger = get_logger(__name__)

POLARS_MAX_THREADS = "POLARS_MAX_THREADS"
POLARS_MAX_CONCURRENT_SCANS = "POLARS_MAX_CONCURRENT_SCANS"
# Read by Arrow, when pyarrow first loads, to size its CPU thread pool;
# unset, that pool has one thread per core in every process.
OMP_NUM_THREADS = "OMP_NUM_THREADS"


@dataclass(frozen=True)
class WorkerPlan:
    """
    workers: processes the pool will actually run at once.
    polars_threads: POLARS_MAX_THREADS for each worker.
    concurrent_scans: POLARS_MAX_CONCURRENT_SCANS for each worker, or
    None to leave polars' own default (which follows polars_threads).
    """

    workers: int
    polars_threads: int
    concurrent_scans: int | None = None

    def describe(self) -> str:
        scans = (
            f", {self.concurrent_scans} file read(s) at once"
            if self.concurrent_scans is not None else ""
        )
        return (
            f"{self.workers} worker process(es), "
            f"{self.polars_threads} polars thread(s) each{scans}"
        )


def plan_workers(
    max_workers: int | None,
    n_tasks: int,
    scans_per_worker: int | None = None,
    max_concurrent_reads: int | None = None,
    cpu_count: int | None = None,
) -> WorkerPlan:
    """
    Resolve a pool's effective shape.

    max_workers None means one worker per core, the same default
    ProcessPoolExecutor applies on its own. The worker count never
    exceeds n_tasks (a pool never runs more processes than it has tasks
    for), nor max_concurrent_reads when that is set: every clean/
    aggregate worker reads its own input file, so capping how many files
    the command reads at once means capping workers.

    Each worker then gets ceil(cores / workers) polars threads, so the
    whole pool lands near one thread per core. Rounding up keeps a few
    workers on a many-core machine from leaving cores idle; it
    oversubscribes by at most one thread per worker.

    scans_per_worker, when given, becomes each worker's concurrent_scans.
    None leaves polars' own default, which follows the worker's thread
    count once POLARS_MAX_THREADS is set.

    A worker given scans_per_worker scans many files at once (aggregate:
    a whole period). POLARS_MAX_CONCURRENT_SCANS bounds its data reads,
    but polars fetches the files' footers on its thread pool. Under
    max_concurrent_reads the workers' threads therefore share the cap
    too, max_concurrent_reads // workers each, so footer reads stay
    within it. A worker that reads one file (clean, convert) fetches one
    footer at a time whatever its thread count.
    """
    cores = cpu_count or os.cpu_count() or 1
    workers = max_workers if max_workers is not None else cores
    workers = min(workers, max(1, n_tasks))
    if max_concurrent_reads is not None:
        workers = min(workers, max_concurrent_reads)
    threads = max(1, math.ceil(cores / workers))
    if max_concurrent_reads is not None and scans_per_worker is not None:
        threads = max(1, min(threads, max_concurrent_reads // workers))
    return WorkerPlan(
        workers=workers,
        polars_threads=threads,
        concurrent_scans=scans_per_worker,
    )


@contextmanager
def _env_overrides(values: dict[str, int | None]) -> Generator[None, None, None]:
    """
    Set each non-None value in os.environ for the duration of the block,
    restoring the previous state afterwards. A variable the user already
    exported is left alone and logged: an explicit POLARS_MAX_THREADS in
    the shell is a deliberate choice this module shouldn't second-guess.
    """
    applied: list[str] = []
    try:
        for name, value in values.items():
            if value is None:
                continue
            if name in os.environ:
                logger.debug(f"{name}={os.environ[name]} already set; keeping it.")
                continue
            os.environ[name] = str(value)
            applied.append(name)
        yield
    finally:
        for name in applied:
            os.environ.pop(name, None)


@contextmanager
def polars_worker_env(plan: WorkerPlan) -> Generator[None, None, None]:
    """
    Wrap a spawn-context ProcessPoolExecutor's whole lifetime in this.

    polars reads POLARS_MAX_THREADS once, when it is first imported, so
    it can't be changed from inside a worker (an executor initializer
    runs too late: unpickling the task function already imported
    polars). A spawned worker inherits its parent's environment at the
    moment it starts, and ProcessPoolExecutor starts workers lazily, on
    submit(), so the variables have to stay set for as long as the pool
    may start one. They are removed from this process again afterwards.

    pyarrow's CPU pool gets the same size through OMP_NUM_THREADS, which
    Arrow reads when pyarrow loads. Every worker imports pyarrow, and
    left alone each one carried a thread per core on top of its polars
    pool. OpenMP libraries in the worker, if any, follow the same value.
    """
    with _env_overrides({
        POLARS_MAX_THREADS: plan.polars_threads,
        POLARS_MAX_CONCURRENT_SCANS: plan.concurrent_scans,
        OMP_NUM_THREADS: plan.polars_threads,
    }):
        yield


# The largest exported POLARS_MAX_CONCURRENT_SCANS accepted. polars takes
# a leading "+" and numbers up to an internal limit (2**61 in polars
# 1.44) and panics above it; 2**32 files read at once is far beyond any
# real use and well inside what every supported polars accepts.
_MAX_SCAN_LIMIT = 2**32


def exported_scan_limit() -> int | None:
    """
    The POLARS_MAX_CONCURRENT_SCANS exported in this environment, or None
    when it isn't. polars accepts only a whole number greater than 0 there,
    and on anything else panics at its first Parquet read, in this process
    or in a worker that inherits the variable, with a raw traceback: a
    pyo3 PanicException, which no `except Exception` catches. Such a value
    fails here instead, naming the variable.
    """
    value = os.environ.get(POLARS_MAX_CONCURRENT_SCANS)
    if value is None:
        return None
    digits = value[1:] if value.startswith("+") else value
    if not (digits.isascii() and digits.isdigit() and 0 < int(digits) <= _MAX_SCAN_LIMIT):
        raise ValueError(
            f"{POLARS_MAX_CONCURRENT_SCANS}={value!r} is exported, but polars stops at the "
            f"first Parquet file it reads on anything other than a whole number greater "
            f"than 0, and on numbers too large for it. Unset it, or export a whole number "
            f"from 1 to {_MAX_SCAN_LIMIT}."
        )
    return int(digits)


@contextmanager
def polars_scan_limit(max_concurrent_reads: int | None) -> Generator[None, None, None]:
    """
    Bound concurrent file reads for polars work running in this process
    (sample, crossref), which reads many files through one multi-file
    scan. polars reads POLARS_MAX_CONCURRENT_SCANS again at every query,
    so, unlike POLARS_MAX_THREADS, setting it here takes effect at once.
    None leaves polars' own default of one file per core.
    """
    with _env_overrides({POLARS_MAX_CONCURRENT_SCANS: max_concurrent_reads}):
        yield
