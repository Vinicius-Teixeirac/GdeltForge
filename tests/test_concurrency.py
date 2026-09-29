import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor

import pytest

from gdeltforge.utils.concurrency import (
    POLARS_MAX_CONCURRENT_SCANS,
    POLARS_MAX_THREADS,
    WorkerPlan,
    plan_workers,
    polars_scan_limit,
    polars_worker_env,
)


def _report_polars_threads() -> tuple[str | None, int]:
    # Runs inside a spawned worker: what the worker's own polars actually
    # sized its pool to, not just what the environment claims.
    import polars as pl

    return os.environ.get(POLARS_MAX_THREADS), pl.thread_pool_size()


class TestPlanWorkers:
    def test_none_means_one_worker_per_core(self):
        assert plan_workers(None, 100, cpu_count=32) == WorkerPlan(workers=32, polars_threads=1)

    def test_explicit_workers_split_the_cores(self):
        assert plan_workers(4, 100, cpu_count=32) == WorkerPlan(workers=4, polars_threads=8)

    def test_uneven_split_rounds_threads_up(self):
        # 20 cores over 3 workers: 7 threads each (21 total), not 6 (18),
        # which would leave two cores idle.
        assert plan_workers(3, 100, cpu_count=20).polars_threads == 7

    def test_never_more_workers_than_tasks(self):
        # 2 periods on a 32-core machine: 2 workers with 16 threads each,
        # not 32 workers with one thread each, 30 of them never started.
        assert plan_workers(None, 2, cpu_count=32) == WorkerPlan(workers=2, polars_threads=16)

    def test_zero_tasks_still_plans_one_worker(self):
        assert plan_workers(None, 0, cpu_count=8).workers == 1

    def test_max_concurrent_reads_caps_workers(self):
        # Each filter/aggregate worker reads one file at a time, so a cap
        # on concurrent reads is a cap on workers; the freed cores go to
        # the remaining workers' polars threads.
        assert plan_workers(None, 100, max_concurrent_reads=4, cpu_count=32) == WorkerPlan(
            workers=4, polars_threads=8
        )

    @pytest.mark.parametrize("max_workers, n_tasks, expected", [
        (None, 1, WorkerPlan(workers=1, polars_threads=4, concurrent_scans=1)),
        (None, 10, WorkerPlan(workers=4, polars_threads=1, concurrent_scans=1)),
        (2, 10, WorkerPlan(workers=2, polars_threads=2, concurrent_scans=1)),
    ])
    def test_multi_file_workers_share_the_cap_among_their_threads(
        self, max_workers, n_tasks, expected
    ):
        # An aggregate worker fetches a period's footers on its thread
        # pool, so under a read cap its threads count toward the cap.
        plan = plan_workers(
            max_workers, n_tasks, scans_per_worker=1, max_concurrent_reads=4, cpu_count=32
        )
        assert plan == expected

    def test_multi_file_workers_keep_their_threads_without_a_cap(self):
        assert plan_workers(None, 1, scans_per_worker=1, cpu_count=32).polars_threads == 32

    def test_max_concurrent_reads_above_the_worker_count_changes_nothing(self):
        assert plan_workers(2, 100, max_concurrent_reads=4, cpu_count=32).workers == 2

    def test_scans_per_worker_passes_through(self):
        assert plan_workers(4, 100, scans_per_worker=1, cpu_count=32).concurrent_scans == 1

    def test_scans_default_to_polars_own(self):
        assert plan_workers(4, 100, cpu_count=32).concurrent_scans is None

    def test_describe_names_both_numbers(self):
        assert WorkerPlan(workers=4, polars_threads=8).describe() == (
            "4 worker process(es), 8 polars thread(s) each"
        )

    def test_describe_names_the_scan_limit_when_set(self):
        assert WorkerPlan(workers=4, polars_threads=8, concurrent_scans=1).describe() == (
            "4 worker process(es), 8 polars thread(s) each, 1 file read(s) at once"
        )


class TestPolarsWorkerEnv:
    def test_sets_and_restores(self, monkeypatch):
        monkeypatch.delenv(POLARS_MAX_THREADS, raising=False)
        with polars_worker_env(WorkerPlan(workers=2, polars_threads=3)):
            assert os.environ[POLARS_MAX_THREADS] == "3"
        assert POLARS_MAX_THREADS not in os.environ

    def test_sets_the_scan_limit_only_when_planned(self, monkeypatch):
        monkeypatch.delenv(POLARS_MAX_CONCURRENT_SCANS, raising=False)
        with polars_worker_env(WorkerPlan(workers=2, polars_threads=3)):
            assert POLARS_MAX_CONCURRENT_SCANS not in os.environ
        with polars_worker_env(WorkerPlan(workers=2, polars_threads=3, concurrent_scans=1)):
            assert os.environ[POLARS_MAX_CONCURRENT_SCANS] == "1"
        assert POLARS_MAX_CONCURRENT_SCANS not in os.environ

    def test_restores_after_an_exception(self, monkeypatch):
        monkeypatch.delenv(POLARS_MAX_THREADS, raising=False)
        with pytest.raises(RuntimeError):
            with polars_worker_env(WorkerPlan(workers=2, polars_threads=3)):
                raise RuntimeError("boom")
        assert POLARS_MAX_THREADS not in os.environ

    def test_keeps_a_value_the_user_exported(self, monkeypatch):
        monkeypatch.setenv(POLARS_MAX_THREADS, "5")
        with polars_worker_env(WorkerPlan(workers=2, polars_threads=3)):
            assert os.environ[POLARS_MAX_THREADS] == "5"
        assert os.environ[POLARS_MAX_THREADS] == "5"

    def test_spawned_worker_polars_pool_follows_the_plan(self, monkeypatch):
        # The real contract: polars reads POLARS_MAX_THREADS only at import,
        # so the value has to reach the worker through its inherited
        # environment, before the worker ever imports polars.
        monkeypatch.delenv(POLARS_MAX_THREADS, raising=False)
        plan = WorkerPlan(workers=1, polars_threads=2)
        with polars_worker_env(plan), ProcessPoolExecutor(
            max_workers=plan.workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            env_value, pool_size = executor.submit(_report_polars_threads).result()
        assert env_value == "2"
        assert pool_size == 2


class TestPolarsScanLimit:
    def test_sets_and_restores(self, monkeypatch):
        monkeypatch.delenv(POLARS_MAX_CONCURRENT_SCANS, raising=False)
        with polars_scan_limit(4):
            assert os.environ[POLARS_MAX_CONCURRENT_SCANS] == "4"
        assert POLARS_MAX_CONCURRENT_SCANS not in os.environ

    def test_none_leaves_polars_default(self, monkeypatch):
        monkeypatch.delenv(POLARS_MAX_CONCURRENT_SCANS, raising=False)
        with polars_scan_limit(None):
            assert POLARS_MAX_CONCURRENT_SCANS not in os.environ

    def test_keeps_a_value_the_user_exported(self, monkeypatch):
        monkeypatch.setenv(POLARS_MAX_CONCURRENT_SCANS, "2")
        with polars_scan_limit(4):
            assert os.environ[POLARS_MAX_CONCURRENT_SCANS] == "2"
