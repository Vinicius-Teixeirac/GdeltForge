"""io.max_read_mb_per_s: reads paced on the bytes the operating system
counts as read (gdeltforge.utils.io.pace_reads), the scans cut into
ranges so polars can't read far ahead, and the rules that come with it."""

import logging
import os
import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest

import gdeltforge.cleaning.cleaner as cleaner_module
import gdeltforge.cli as cli
import gdeltforge.crossref.crossref as crossref_module
import gdeltforge.sampling.indexer as indexer_module
import gdeltforge.sampling.samplers as samplers_module
import gdeltforge.utils.io as io_module
from gdeltforge.cleaning.cleaner import GDELTCleaner
from gdeltforge.launcher import force_async_reads
from gdeltforge.sampling.indexer import FileIndex
from gdeltforge.sampling.samplers import CalendarSampler, FilteredSampler, _scan_ranges
from gdeltforge.utils.config import resolve_max_read_mb_per_s
from gdeltforge.utils.io import (
    configure_read_rate,
    configure_worker_reads,
    pace_reads,
    paced_ranges,
    pacing_reads,
    read_rate,
    read_retries,
    worker_read_rate,
)
from gdeltforge.utils.storage import memory_map_decision, process_read_bytes


@pytest.fixture(autouse=True)
def no_cap_after():
    yield
    configure_read_rate(None)
    io_module.configure_read_retries(3)


class _Clock:
    """time and the read counter for io's pacer: sleeping is instant, and
    `read(n)` stands for the process reading n bytes."""

    def __init__(self):
        self.now = 100.0
        self.counter = 5_000_000
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(round(seconds, 6))
        self.now += seconds

    def read(self, nbytes: int) -> None:
        self.counter += nbytes
        pace_reads()


@pytest.fixture
def clock(monkeypatch):
    c = _Clock()
    monkeypatch.setattr(io_module.time, "monotonic", c.monotonic)
    monkeypatch.setattr(io_module.time, "sleep", c.sleep)
    monkeypatch.setattr(io_module, "process_read_bytes", lambda: c.counter)
    return c


class TestPaceReads:
    def test_reads_ahead_of_the_rate_wait_for_it(self, clock):
        configure_read_rate(2)  # 2 MB/s
        clock.read(1_000_000)  # 0.5 s worth, read at once
        clock.read(3_000_000)  # 2 s worth in all
        assert clock.slept == [0.5, 1.5]
        assert clock.now == pytest.approx(102.0)

    def test_only_what_was_read_since_the_cap_was_set_counts(self, clock):
        clock.counter += 50_000_000  # read before the command's reads began
        configure_read_rate(1)
        clock.read(500_000)
        assert clock.slept == [0.5]

    def test_reads_slower_than_the_rate_never_wait(self, clock):
        configure_read_rate(1)
        clock.read(500_000)
        clock.now += 0.6
        clock.read(100_000)
        assert clock.slept == [0.5]

    def test_at_most_a_second_of_the_rate_is_banked(self, clock):
        configure_read_rate(1)
        clock.read(1)
        clock.now += 60  # a minute without reading
        clock.read(1)  # rebases: a second of credit kept
        clock.read(3_000_000)
        # 3 s worth against 1 s banked: 2 s of waiting, not none.
        assert sum(clock.slept) == pytest.approx(2.0, abs=1e-3)

    def test_without_a_cap_nothing_waits(self, clock):
        configure_read_rate(None)
        clock.read(10**12)
        assert clock.slept == []
        assert not pacing_reads()


class TestSetting:
    @pytest.mark.parametrize("value, expected", [(None, None), (50, 50.0), (0.5, 0.5)])
    def test_a_positive_number_or_null(self, value, expected):
        assert resolve_max_read_mb_per_s({"io": {"max_read_mb_per_s": value}}) == expected

    @pytest.mark.parametrize("value", [0, -5, "50", True, float("inf"), float("nan")])
    def test_anything_else_is_refused(self, value):
        with pytest.raises(ValueError, match="io.max_read_mb_per_s must be a number"):
            resolve_max_read_mb_per_s({"io": {"max_read_mb_per_s": value}})

    def test_workers_share_the_cap(self):
        configure_read_rate(100)
        assert read_rate() == pytest.approx(100)
        assert worker_read_rate(4) == pytest.approx(25)
        configure_read_rate(None)
        assert worker_read_rate(4) is None

    def test_a_workers_initializer_sets_both(self):
        configure_worker_reads(5, 12.5)
        assert read_retries() == 5
        assert read_rate() == pytest.approx(12.5)


@pytest.mark.skipif(
    not (sys.platform.startswith("linux") or sys.platform == "win32"),
    reason="no per-process read count on this platform",
)
class TestProcessReadBytes:
    def test_a_read_moves_the_count_by_its_bytes(self, tmp_path):
        path = tmp_path / "data.bin"
        path.write_bytes(os.urandom(300_000))
        before = process_read_bytes()
        with open(path, "rb", buffering=0) as f:
            assert len(f.read()) == 300_000
        after = process_read_bytes()
        assert before is not None and after is not None
        assert after - before >= 300_000


class TestCapAndMapping:
    def test_a_cap_turns_mapping_off(self):
        config = {"io": {"memory_map": "auto", "max_read_mb_per_s": 10}, "paths": {}}
        assert memory_map_decision(config, {}) == (False, "io.max_read_mb_per_s is set")

    def test_a_cap_with_mapping_forced_on_is_refused(self):
        config = {"io": {"memory_map": True, "max_read_mb_per_s": 10}, "paths": {}}
        with pytest.raises(ValueError, match="can't be used with io.memory_map: true"):
            memory_map_decision(config, {})

    def test_the_launcher_starts_a_capped_command_without_mapping(self, tmp_path):
        path = tmp_path / "settings.yaml"
        path.write_text("io:\n  max_read_mb_per_s: 20\n", encoding="utf-8")
        assert force_async_reads(["--config", str(path), "sample"], {}) is True

    def test_a_process_that_maps_refuses_the_cap(self):
        with pytest.raises(ValueError, match="this process memory-maps"):
            cli._apply_read_rate({"io": {"max_read_mb_per_s": 10}}, this_process_maps=True)
        assert not pacing_reads()

    def test_no_read_count_refuses_the_cap(self, monkeypatch):
        monkeypatch.setattr(cli, "process_read_bytes", lambda: None)
        with pytest.raises(ValueError, match="gives no count of the bytes"):
            cli._apply_read_rate({"io": {"max_read_mb_per_s": 10}}, this_process_maps=False)

    def test_the_cap_is_set_and_logged(self, caplog):
        with caplog.at_level(logging.INFO, logger=cli.logger.name):
            cli._apply_read_rate({"io": {"max_read_mb_per_s": 12.5}}, this_process_maps=False)
        assert read_rate() == pytest.approx(12.5)
        assert "Reading at most 12.5 MB/s on average" in caplog.text


class TestPacedRanges:
    def test_not_paced_one_range_of_every_file(self):
        configure_read_rate(None)
        assert paced_ranges([10, 10, 10]) == [(0, 3)]

    def test_paced_each_range_is_at_most_a_second_of_the_rate(self):
        configure_read_rate(1)  # 1 MB/s: ranges of up to 1 MB on disk
        sizes = [400_000, 400_000, 400_000, 2_000_000, 100_000]
        assert paced_ranges(sizes) == [(0, 2), (2, 3), (3, 4), (4, 5)]


def _write(folder, name, n, offset=0, row_group_size=None):
    folder.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(offset)
    pl.DataFrame({
        "GlobalEventID": np.arange(offset, offset + n),
        # Twenty dates in each file, so calendar periods fill at different
        # points of the scan.
        "Day": rng.choice(np.arange(20200101, 20200121), n),
        "QuadClass": rng.integers(1, 5, n),
        "Actor1CountryCode": rng.choice(["USA", "BRA", "CHN"], n),
        "SOURCEURL": [f"https://example.org/{i}/{'x' * 40}" for i in range(n)],
    }).write_parquet(folder / name, row_group_size=row_group_size)
    return folder / name


def _dataset(tmp_path):
    folder = tmp_path / "events"
    files = [
        _write(folder, f"2020010{i}.parquet", 3000, offset=i * 3000, row_group_size=1000)
        for i in range(1, 4)
    ]
    return folder, files


@pytest.fixture
def paces(monkeypatch):
    """Every pace_reads call, counted, with no waiting. The cap is set by
    each test: 1e-6 MB/s (a byte per second) makes every file a range of
    its own."""
    calls = {"n": 0}

    def pace():
        calls["n"] += 1

    for module in (samplers_module, indexer_module, io_module, crossref_module, cleaner_module):
        monkeypatch.setattr(module, "pace_reads", pace)
    return calls


class TestPacedScansDrawTheSameRows:
    @pytest.mark.parametrize("draw", [
        lambda s: s.get_random_sample(400),
        lambda s: s.get_random_sample(400, replace=True),
        lambda s: s.get_stratified_sample("QuadClass", n_per_group=50),
        lambda s: s.filter_dataset(),
    ], ids=["random", "replace", "stratified", "filter_dataset"])
    def test_filtered(self, tmp_path, paces, draw):
        folder, files = _dataset(tmp_path)

        def run():
            return draw(FilteredSampler(
                str(folder), ["GlobalEventID", "QuadClass", "Actor1CountryCode"],
                filter_dict={"Actor1CountryCode": ["USA", "BRA"]}, random_state=3,
                provenance=True,
            ))

        whole = run()
        paces["n"] = 0
        configure_read_rate(1e-6)
        assert _scan_ranges(files, FileIndex(folder)) == [(0, 1), (1, 2), (2, 3)]
        assert run().equals(whole)
        assert paces["n"] >= 3

    def test_calendar(self, tmp_path, paces):
        folder = tmp_path / "events"
        for i in range(1, 4):
            _write(folder, f"2020010{i}.parquet", 3000, offset=i * 3000, row_group_size=1000)

        def run():
            return CalendarSampler(
                str(folder), random_state=5, provenance=True
            ).get_calendar_samples(samples_per_period=300)

        whole = run()
        paces["n"] = 0
        configure_read_rate(1e-6)
        assert run().equals(whole)
        assert paces["n"] >= 3


class TestReadersPace:
    def test_a_crossref_collect_paces_after_reading(self, tmp_path, paces):
        _, files = _dataset(tmp_path)
        query = pl.scan_parquet(files).filter(pl.col("QuadClass") == 1).select("SOURCEURL")
        out = crossref_module._paced_collect("the files", query)
        assert out.height == query.collect().height
        assert paces["n"] == 1

    def test_clean_paces_once_per_file(self, tmp_path, paces):
        f = _write(tmp_path / "in", "20200101.export.parquet", 2000)
        cleaner = GDELTCleaner(str(tmp_path / "in"), str(tmp_path / "out"), ["QuadClass"])
        cleaner._clean_file(f, tmp_path / "out" / "20200101.parquet")
        assert paces["n"] == 1

    def test_the_index_paces_each_footer(self, tmp_path, paces):
        _dataset(tmp_path)
        FileIndex(tmp_path / "events")
        assert paces["n"] == 3



class TestPacedCrossref:
    def test_a_gkg_v1_pass_in_ranges_joins_the_same_rows(self, tmp_path, paces):
        from test_crossref import GKG_V1_COLUMNS, TestCrossrefEventsGkgV1

        from gdeltforge.crossref.crossref import crossref_events_gkg_v1

        folder = TestCrossrefEventsGkgV1._write_gkg_v1(tmp_path)
        events = TestCrossrefEventsGkgV1._events_df()
        whole = crossref_events_gkg_v1(events, folder, GKG_V1_COLUMNS)
        paces["n"] = 0
        configure_read_rate(1e-6)  # each of the two files a range of its own
        sizes = [os.path.getsize(f) for f in sorted(Path(folder).glob("*.parquet"))]
        assert paced_ranges(sizes) == [(0, 1), (1, 2)]
        assert crossref_events_gkg_v1(events, folder, GKG_V1_COLUMNS).equals(whole)
        assert paces["n"] >= 1


class TestScanBatches:
    def test_ranges_are_handed_out_in_the_batches_of_one_scan(self, tmp_path):
        _, files = _dataset(tmp_path)
        whole = pl.scan_parquet(files).filter(pl.col("QuadClass") != 2)
        ranges = [pl.scan_parquet([f]).filter(pl.col("QuadClass") != 2) for f in files]
        one = list(io_module.scan_batches([whole], "the scan", 1000))
        split = list(io_module.scan_batches(ranges, "the scan", 1000))
        assert [b.height for b in split] == [b.height for b in one]
        assert all(a.equals(b) for a, b in zip(one, split, strict=True))
