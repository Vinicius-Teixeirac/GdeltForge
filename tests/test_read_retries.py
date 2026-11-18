"""io.read_retries: a read that fails on network storage is tried again,
and a batch scan resumes after the rows it already handed out
(gdeltforge.utils.io.retry_read and batches_with_retries)."""

import logging
from typing import cast

import polars as pl
import pytest

import gdeltforge.utils.io as io_module
from gdeltforge.utils.config import peek_config, resolve_read_retries
from gdeltforge.utils.io import (
    batches_with_retries,
    configure_read_retries,
    read_retries,
    retry_read,
)


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    """Record the waits instead of sleeping, and restore the default."""
    waits: list[float] = []
    monkeypatch.setattr(io_module.time, "sleep", waits.append)
    yield waits
    configure_read_retries(3)


def _flaky(failures: int, error: BaseException, result="ok"):
    calls = {"n": 0}

    def read():
        calls["n"] += 1
        if calls["n"] <= failures:
            raise error
        return result

    return read, calls


class TestRetryRead:
    def test_a_read_error_is_tried_again_after_2_4_and_8_seconds(self, no_waiting, caplog):
        read, calls = _flaky(3, OSError(5, "object-store error"))
        with caplog.at_level(logging.WARNING):
            assert retry_read("day.parquet", read) == "ok"
        assert calls["n"] == 4
        assert no_waiting == [2, 4, 8]
        assert "Reading day.parquet failed" in caplog.text
        assert "retry 3 of 3" in caplog.text

    def test_the_last_error_is_raised_once_the_retries_are_spent(self, no_waiting):
        read, calls = _flaky(10, OSError(116, "Stale file handle"))
        with pytest.raises(OSError, match="Stale file handle"):
            retry_read("day.parquet", read)
        assert calls["n"] == 4

    def test_zero_retries_read_once(self, no_waiting):
        configure_read_retries(0)
        read, calls = _flaky(1, OSError("timed out"))
        with pytest.raises(OSError):
            retry_read("day.parquet", read)
        assert calls["n"] == 1
        assert no_waiting == []

    @pytest.mark.parametrize("error", [
        FileNotFoundError("no such file"), PermissionError("denied"),
        IsADirectoryError("a directory"), ValueError("not a read error"),
        pl.exceptions.ComputeError("corrupt file"),
    ])
    def test_an_error_another_try_cant_change_is_raised_at_once(self, no_waiting, error):
        read, calls = _flaky(1, error)
        with pytest.raises(type(error)):
            retry_read("day.parquet", read)
        assert calls["n"] == 1
        assert no_waiting == []

    def test_the_setting_is_per_process(self):
        configure_read_retries(5)
        assert read_retries() == 5


class _FailingScan:
    """A LazyFrame stand-in whose batches fail with an OSError after
    `after` batches, `times` times over, then come through."""

    def __init__(self, lf: pl.LazyFrame, after: int, state: dict):
        self.lf, self.after, self.state = lf, after, state

    def slice(self, offset: int):
        self.state["slices"].append(offset)
        return _FailingScan(self.lf.slice(offset), self.after, self.state)

    def collect_batches(self, chunk_size: int):
        for i, batch in enumerate(self.lf.collect_batches(chunk_size=chunk_size)):
            if i == self.after and self.state["times"] > 0:
                self.state["times"] -= 1
                raise OSError("object-store error")
            yield batch


def _failing(lf: pl.LazyFrame, after: int, state: dict) -> pl.LazyFrame:
    """A _FailingScan, typed as the LazyFrame it stands in for."""
    return cast(pl.LazyFrame, _FailingScan(lf, after, state))


def _scan(tmp_path) -> pl.LazyFrame:
    files = []
    for i in range(4):
        path = tmp_path / f"{i}.parquet"
        pl.DataFrame({"x": range(i * 1000, (i + 1) * 1000)}).write_parquet(
            path, row_group_size=100
        )
        files.append(path)
    return (
        pl.scan_parquet(files, include_file_paths="file")
        .filter(pl.col("x") % 3 != 0)
        .with_row_index("row")
    )


class TestBatchesWithRetries:
    def test_a_resumed_scan_holds_exactly_the_rows_of_an_uninterrupted_one(
        self, tmp_path, no_waiting
    ):
        lf = _scan(tmp_path)
        state = {"times": 2, "slices": []}
        batches = list(batches_with_retries(
            lambda: _failing(lf, after=3, state=state), "the scan", 250
        ))
        assert pl.concat(batches).equals(lf.collect())
        # Each resume skipped exactly the rows already handed out.
        assert state["slices"] == [750, 1500]
        assert no_waiting == [2, 2]

    def test_rows_read_since_a_failure_start_the_count_again(self, tmp_path, no_waiting):
        configure_read_retries(1)
        lf = _scan(tmp_path)
        state = {"times": 3, "slices": []}
        rows = sum(b.height for b in batches_with_retries(
            lambda: _failing(lf, after=1, state=state), "the scan", 250
        ))
        assert rows == lf.collect().height
        assert no_waiting == [2, 2, 2]

    def test_a_scan_that_keeps_failing_without_progress_stops(self, tmp_path, no_waiting):
        lf = _scan(tmp_path)
        state = {"times": 10, "slices": []}
        with pytest.raises(OSError, match="object-store error"):
            list(batches_with_retries(
                lambda: _failing(lf, after=0, state=state), "the scan", 250
            ))
        assert no_waiting == [2, 4, 8]

    def test_without_a_failure_the_batches_are_the_scans_own(self, tmp_path):
        lf = _scan(tmp_path)
        ours = list(batches_with_retries(lambda: lf, "the scan", 250))
        theirs = list(lf.collect_batches(chunk_size=250))
        assert [b.height for b in ours] == [b.height for b in theirs]
        assert pl.concat(ours).equals(pl.concat(theirs))


class TestReadRetriesSetting:
    @pytest.mark.parametrize("value, expected", [(None, 3), (0, 0), (7, 7)])
    def test_whole_numbers_of_zero_or_more(self, value, expected):
        config = {"io": {} if value is None else {"read_retries": value}}
        assert resolve_read_retries(config) == expected

    @pytest.mark.parametrize("value", [-1, 2.5, "3", True])
    def test_anything_else_is_refused(self, value):
        with pytest.raises(ValueError, match="io.read_retries must be a whole number"):
            resolve_read_retries({"io": {"read_retries": value}})

    def test_the_bundled_default_is_3(self, tmp_path):
        config = peek_config(None, {"GDELTFORGE_CONFIG": str(_settings(tmp_path, "{}\n"))})
        assert config is not None
        assert config["io"]["read_retries"] == 3
        assert config["io"]["memory_map"] == "auto"


def _settings(tmp_path, text):
    path = tmp_path / "settings.yaml"
    path.write_text(text, encoding="utf-8")
    return path


class TestPeekConfig:
    def test_reads_the_file_with_the_bundled_defaults_filled_in(self, tmp_path):
        path = _settings(tmp_path, "io:\n  memory_map: false\n")
        config = peek_config(str(path), {})
        assert config is not None
        assert config["io"]["memory_map"] is False
        assert config["io"]["read_retries"] == 3
        assert "parquet_data_directory" in config["paths"]

    @pytest.mark.parametrize("text", ["io: [\n", "- a list\n"])
    def test_an_unreadable_file_gives_none(self, tmp_path, text):
        assert peek_config(str(_settings(tmp_path, text)), {}) is None

    def test_a_missing_explicit_file_gives_none(self, tmp_path):
        assert peek_config(str(tmp_path / "nope.yaml"), {}) is None

    def test_no_file_at_all_gives_the_bundled_default_and_writes_nothing(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        config = peek_config(None, {})
        assert config is not None
        assert config["io"]["memory_map"] == "auto"
        assert not (tmp_path / "config").exists()
