"""
Import-time side effects of the top-level gdeltforge package itself,
as opposed to any one stage module.
"""

import tqdm

import gdeltforge  # pyright: ignore[reportUnusedImport]  # noqa: F401

# The import alone is the thing under test: gdeltforge.__init__ sets
# tqdm.tqdm.monitor_interval as an import-time side effect, with nothing
# in this module needing gdeltforge's own public API otherwise.


class TestTqdmMonitorThreadIsDisabled:
    """
    tqdm starts a background monitor thread the first time any instance
    is created anywhere in the process, and it stays alive for the rest
    of that process. Every convert/filter/scrape stage forks a real
    ProcessPoolExecutor worker pool on Linux (the platform default start
    method), and fork() only clones the calling thread: a child forked
    while the monitor thread holds tqdm's own class lock inherits that
    lock already held, with no thread left to ever release it, so any
    tqdm instance created inside the child afterward deadlocks
    permanently. This hung this project's own CI for real, silently, for
    about a week: every push to main after the polars migration timed
    out its test jobs at GitHub's 6-hour maximum, invisible on Windows
    (always spawn, never fork, nothing to inherit mid-lock) and never
    caught locally as a result.

    monitor_interval = 0 is tqdm's own documented switch to never start
    that thread at all, set once at gdeltforge's own import time (before
    any of the five stage modules that import tqdm get a chance to
    create the first instance).
    """

    def test_monitor_interval_is_disabled_on_import(self):
        assert tqdm.tqdm.monitor_interval == 0

    def test_creating_an_instance_does_not_start_a_monitor_thread(self):
        with tqdm.tqdm(total=1, disable=True) as bar:
            bar.update(1)

        assert tqdm.tqdm.monitor is None
