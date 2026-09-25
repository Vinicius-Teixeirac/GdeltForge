"""
concurrency.py

Sizes the polars thread pools inside convert/filter/aggregate's worker
processes.

Every worker is a separately spawned interpreter, and polars sizes its
own pools to the whole machine by default in each one of them: compute
threads, async I/O threads, and max_concurrent_scans (files a multi-file
scan reads at once) all equal the core count. max_workers alone
therefore only caps processes. N workers on a C-core machine used to
carry roughly N x 2C threads, and each worker's memory grew with its
own pool size. See docs/configuration.md's "Worker pools and polars
threads" section for the measurements behind this module.

Provides:
    - WorkerPlan / plan_workers: resolve a stage's effective worker count
      and each worker's polars thread budget
    - polars_worker_env: apply a WorkerPlan to the worker processes a
      ProcessPoolExecutor spawns inside it
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


@dataclass(frozen=True)
class WorkerPlan:
    """
    workers: processes the pool will actually run at once.
    polars_threads: POLARS_MAX_THREADS for each worker.
    """

    workers: int
    polars_threads: int

    def describe(self) -> str:
        return (
            f"{self.workers} worker process(es), "
            f"{self.polars_threads} polars thread(s) each"
        )


def plan_workers(
    max_workers: int | None,
    n_tasks: int,
    cpu_count: int | None = None,
) -> WorkerPlan:
    """
    Resolve a pool's effective shape.

    max_workers None means one worker per core, the same default
    ProcessPoolExecutor applies on its own. The worker count never
    exceeds n_tasks: a pool never runs more processes than it has tasks
    for.

    Each worker then gets ceil(cores / workers) polars threads, so the
    whole pool lands near one thread per core. Rounding up keeps a few
    workers on a many-core machine from leaving cores idle; it
    oversubscribes by at most one thread per worker.
    """
    cores = cpu_count or os.cpu_count() or 1
    workers = max_workers if max_workers is not None else cores
    workers = min(workers, max(1, n_tasks))
    return WorkerPlan(
        workers=workers,
        polars_threads=max(1, math.ceil(cores / workers)),
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
    submit(), so the variable has to stay set for as long as the pool
    may start one. It is removed from this process again afterwards.
    """
    with _env_overrides({POLARS_MAX_THREADS: plan.polars_threads}):
        yield
