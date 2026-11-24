"""Checkpoints: a long draw stopped partway resumes and gives exactly the
output of an uninterrupted run (gdeltforge.utils.checkpoint)."""

import numpy as np
import polars as pl
import pytest

import gdeltforge.sampling.samplers as samplers_module
from gdeltforge.sampling.samplers import CalendarSampler, FilteredSampler
from gdeltforge.utils.checkpoint import (
    Checkpoint,
    CheckpointSettings,
    fingerprint,
)

COLUMNS = ["GlobalEventID", "Day", "QuadClass", "Actor1CountryCode", "NumArticles"]


def _write(folder, name, n, offset):
    folder.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(offset)
    pl.DataFrame({
        "GlobalEventID": np.arange(offset, offset + n),
        "Day": rng.choice(np.arange(20200101, 20200121), n),
        "QuadClass": rng.integers(1, 5, n),
        "Actor1CountryCode": rng.choice(np.array(["USA", "BRA", "CHN", "RUS"]), n),
        "NumArticles": rng.integers(1, 100, n),
    }).write_parquet(folder / name, row_group_size=2000)


@pytest.fixture
def dataset(tmp_path, monkeypatch):
    """Four files of 6,000 rows, scanned in 1,000-row batches."""
    folder = tmp_path / "events"
    for i in range(4):
        _write(folder, f"2020010{i + 1}.parquet", 6000, i * 6000)
    monkeypatch.setattr(samplers_module, "_BATCH_ROWS", 1000)
    return folder


class _Stop(BaseException):
    """Stands for a stop (Ctrl+C, a killed job) partway through a scan."""


def _stop_after(monkeypatch, calls: int):
    """Make the scan stop after `calls` batches have been handed out."""
    real = samplers_module._saving
    count = {"n": 0}

    def stopping(batches, checkpoint, state):
        for batch in real(batches, checkpoint, state):
            count["n"] += 1
            if count["n"] > calls:
                raise _Stop
            yield batch

    monkeypatch.setattr(samplers_module, "_saving", stopping)
    return lambda: monkeypatch.setattr(samplers_module, "_saving", real)


def _settings(tmp_path):
    return CheckpointSettings(tmp_path / "checkpoints", every_s=0)


DRAWS = {
    "random": lambda s: s.get_random_sample(700),
    "replace": lambda s: s.get_random_sample(700, replace=True),
    "stratified": lambda s: s.get_stratified_sample("QuadClass", n_per_group=150),
    "quotas": lambda s: s.get_stratified_sample("QuadClass", n_per_group=40, quotas={"2": 900}),
    "filter_dataset": lambda s: s.filter_dataset(),
}


@pytest.mark.parametrize("draw", list(DRAWS))
def test_a_stopped_draw_resumes_to_the_uninterrupted_output(
    tmp_path, dataset, monkeypatch, draw
):
    def sampler():
        s = FilteredSampler(
            str(dataset), COLUMNS, filter_dict={"Actor1CountryCode": ["USA", "BRA"]},
            random_state=11, provenance=True,
        )
        s.checkpoints = _settings(tmp_path)
        return s

    whole = DRAWS[draw](FilteredSampler(
        str(dataset), COLUMNS, filter_dict={"Actor1CountryCode": ["USA", "BRA"]},
        random_state=11, provenance=True,
    ))
    restore = _stop_after(monkeypatch, 5)
    with pytest.raises(_Stop):
        DRAWS[draw](sampler())
    saved = list((tmp_path / "checkpoints").iterdir())
    assert len(saved) == 1
    restore()
    resumed = DRAWS[draw](sampler())
    assert resumed.equals(whole)
    # Finished, the checkpoint is gone.
    assert list((tmp_path / "checkpoints").iterdir()) == []


class TestCheckpoint:
    def test_a_saved_state_reads_back(self, tmp_path):
        ckpt = Checkpoint(CheckpointSettings(tmp_path, 0), "sample-x", {"n": 1})
        ckpt.save({"a": 1, "b": [1, 2]}, {"x": np.arange(5)}, [pl.DataFrame({"y": [1, 2]})])
        ckpt.save({"a": 2}, {"x": np.arange(3)}, [pl.DataFrame({"y": [3]})])
        loaded = Checkpoint(CheckpointSettings(tmp_path, 0), "sample-x", {"n": 1}).load()
        assert loaded is not None
        state, arrays, parts = loaded
        assert state == {"a": 2}
        assert arrays["x"].tolist() == [0, 1, 2]
        assert [p["y"].to_list() for p in parts] == [[1, 2], [3]]

    def test_another_key_is_another_run(self, tmp_path):
        Checkpoint(CheckpointSettings(tmp_path, 0), "sample-x", {"n": 1}).save({"a": 1})
        assert Checkpoint(CheckpointSettings(tmp_path, 0), "sample-x", {"n": 2}).load() is None
        assert fingerprint({"n": 1}) != fingerprint({"n": 2})

    def test_an_unreadable_checkpoint_is_removed_and_the_run_starts_over(self, tmp_path):
        ckpt = Checkpoint(CheckpointSettings(tmp_path, 0), "sample-x", {"n": 1})
        ckpt.save({"a": 1})
        assert ckpt.path is not None
        (ckpt.path / "state.npz").write_bytes(b"not an npz")
        assert ckpt.load() is None
        assert not ckpt.path.exists()

    def test_disabled_checkpoints_write_nothing(self, tmp_path):
        ckpt = Checkpoint(CheckpointSettings(None), "sample-x", {"n": 1})
        ckpt.save({"a": 1})
        assert ckpt.load() is None
        assert not ckpt.due()
        assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("stop_after", [1, 7, 20])
def test_a_stopped_calendar_draw_resumes_to_the_uninterrupted_output(
    tmp_path, dataset, monkeypatch, stop_after
):
    def sampler(checkpoints):
        s = CalendarSampler(str(dataset), random_state=4, provenance=True)
        if checkpoints:
            s.checkpoints = _settings(tmp_path)
        return s

    whole = sampler(False).get_calendar_samples(samples_per_period=300)
    restore = _stop_after(monkeypatch, stop_after)
    with pytest.raises(_Stop):
        sampler(True).get_calendar_samples(samples_per_period=300)
    restore()
    assert sampler(True).get_calendar_samples(samples_per_period=300).equals(whole)
    assert list((tmp_path / "checkpoints").iterdir()) == []


def test_a_resumed_draw_reads_only_what_was_left(tmp_path, dataset, monkeypatch):
    """The resumed scan starts after the checkpoint's batch."""
    def sampler():
        s = FilteredSampler(str(dataset), COLUMNS, random_state=2)
        s.checkpoints = _settings(tmp_path)
        return s

    restore = _stop_after(monkeypatch, 15)
    with pytest.raises(_Stop):
        sampler().get_random_sample(500)
    restore()
    seen = {"n": 0}
    real = samplers_module._saving

    def counting(batches, checkpoint, state):
        for batch in real(batches, checkpoint, state):
            seen["n"] += 1
            yield batch

    monkeypatch.setattr(samplers_module, "_saving", counting)
    sampler().get_random_sample(500)
    # 24,000 rows in 1,000-row batches; 15 were read before the stop.
    assert seen["n"] == 24 - 15


def test_a_stopped_index_build_keeps_the_footers_it_read(tmp_path, monkeypatch, caplog):
    import logging

    import gdeltforge.sampling.indexer as indexer_module
    from gdeltforge.sampling.indexer import FileIndex

    folder = tmp_path / "events"
    for i in range(6):
        _write(folder, f"2020010{i + 1}.parquet", 500, i * 500)
    saved = tmp_path / "index" / "events.index"
    real = indexer_module._read_footer
    read = {"n": 0}

    def stopping(file):
        read["n"] += 1
        if read["n"] > 3:
            raise _Stop
        return real(file)

    monkeypatch.setattr(indexer_module, "_read_footer", stopping)
    with pytest.raises(_Stop):
        FileIndex(folder, saved=saved, save_every_s=0)
    monkeypatch.setattr(indexer_module, "_read_footer", real)
    with caplog.at_level(logging.INFO, logger=indexer_module.logger.name):
        index = FileIndex(folder, saved=saved)
    assert index.total_rows == 3000
    assert "3 footer(s) read, 3 from the saved index" in caplog.text


def _stop_after_saves(monkeypatch, module, saves: int):
    """crossref stopped right after its `saves`-th checkpoint."""
    real = module._Stages.save_if_due
    count = {"n": 0}

    def stopping(self):
        real(self)
        count["n"] += 1
        if count["n"] >= saves:
            raise _Stop

    monkeypatch.setattr(module._Stages, "save_if_due", stopping)
    return lambda: monkeypatch.setattr(module._Stages, "save_if_due", real)


@pytest.mark.parametrize("saves", [1, 2, 3])
def test_a_stopped_gkg_v1_crossref_resumes_to_the_uninterrupted_output(
    tmp_path, monkeypatch, saves
):
    from test_crossref import GKG_V1_COLUMNS, TestCrossrefEventsGkgV1

    import gdeltforge.crossref.crossref as crossref_module

    monkeypatch.setattr(crossref_module, "_BATCH_ROWS", 1)
    folder = TestCrossrefEventsGkgV1._write_gkg_v1(tmp_path)
    events = TestCrossrefEventsGkgV1._events_df()
    run = lambda checkpoints=None: crossref_module.crossref_events_gkg_v1(  # noqa: E731
        events, folder, GKG_V1_COLUMNS,
        **({"checkpoints": checkpoints} if checkpoints else {}),
    )
    whole = run()
    restore = _stop_after_saves(monkeypatch, crossref_module, saves)
    with pytest.raises(_Stop):
        run(_settings(tmp_path))
    restore()
    assert run(_settings(tmp_path)).equals(whole)
    assert list((tmp_path / "checkpoints").iterdir()) == []


@pytest.mark.parametrize("saves", [1, 2, 3, 4])
def test_a_stopped_gkg_v2_crossref_resumes_to_the_uninterrupted_output(
    tmp_path, monkeypatch, saves
):
    from test_crossref import GKG_V2_COLUMNS, TestCrossrefEventsGkgV2

    import gdeltforge.crossref.crossref as crossref_module

    # One file per chunk: every pass has a chunk to stop after (a 1-byte
    # budget puts each file in a batch of its own).
    monkeypatch.setattr(crossref_module, "_FIRST_PASS_FILES", 1)
    monkeypatch.setattr(crossref_module, "_ROWS_BATCH_BYTES", 1)
    mentions = TestCrossrefEventsGkgV2._write_mentions(tmp_path)
    gkg = TestCrossrefEventsGkgV2._write_gkg_v2(tmp_path)
    events = TestCrossrefEventsGkgV2._events_df()
    run = lambda checkpoints=None: crossref_module.crossref_events_gkg_v2(  # noqa: E731
        events, mentions, gkg, GKG_V2_COLUMNS,
        **({"checkpoints": checkpoints} if checkpoints else {}),
    )
    whole = run()
    assert whole.height > 0
    restore = _stop_after_saves(monkeypatch, crossref_module, saves)
    with pytest.raises(_Stop):
        run(_settings(tmp_path))
    restore()
    assert run(_settings(tmp_path)).equals(whole)
    assert list((tmp_path / "checkpoints").iterdir()) == []


def test_another_events_table_is_another_crossref_run(tmp_path, monkeypatch):
    from test_crossref import GKG_V1_COLUMNS, TestCrossrefEventsGkgV1

    import gdeltforge.crossref.crossref as crossref_module

    monkeypatch.setattr(crossref_module, "_BATCH_ROWS", 1)
    folder = TestCrossrefEventsGkgV1._write_gkg_v1(tmp_path)
    events = TestCrossrefEventsGkgV1._events_df()
    restore = _stop_after_saves(monkeypatch, crossref_module, 2)
    with pytest.raises(_Stop):
        crossref_module.crossref_events_gkg_v1(
            events, folder, GKG_V1_COLUMNS, checkpoints=_settings(tmp_path)
        )
    restore()
    other = events.with_columns(pl.lit("Zed").alias("Actor1Name"))
    result = crossref_module.crossref_events_gkg_v1(
        other, folder, GKG_V1_COLUMNS, checkpoints=_settings(tmp_path)
    )
    assert set(result["Actor1Name"].to_list()) == {"Zed"}


def test_a_checkpoint_directory_in_any_data_directory_is_refused(tmp_path):
    import argparse

    import gdeltforge.cli as cli

    config = {
        "io": {"checkpoint_every_s": 60},
        "paths": {
            "parquet_data_directory": str(tmp_path / "converted"),
            "cleaned_data_directory": str(tmp_path / "cleaned"),
            "checkpoint_directory": str(tmp_path / "cleaned" / "ck"),
        },
    }
    # A sample reading the converted directory refuses one in the cleaned too.
    with pytest.raises(ValueError, match="checkpoint directory"):
        cli._checkpoint_settings(
            config, argparse.Namespace(no_checkpoint=False),
            [tmp_path / "converted"] + cli._data_directories(config),
        )


def test_a_resumed_scan_keeps_the_filter_in_the_parquet_read(tmp_path, dataset, monkeypatch):
    """A slice in a resumed scan's plan kept the draw's filter out of the
    Parquet read (a draw resumed mid-file peaked at up to 2.6x): the partly
    read file starts at its row through a filter on the row index."""
    captured: list = []
    real = samplers_module.scan_batches

    def capturing(scans, what, chunk_size):
        captured.extend(scans)
        return real(scans, what, chunk_size)

    monkeypatch.setattr(samplers_module, "scan_batches", capturing)
    sampler = FilteredSampler(
        str(dataset), COLUMNS, filter_dict={"Actor1CountryCode": ["USA", "BRA"]},
        random_state=1,
    )
    whole = list(sampler._batches(["Actor1CountryCode"]))
    before = len(captured)
    resumed = list(sampler._batches(["Actor1CountryCode"], (1, 2500)))
    plan = captured[before].explain()  # the first scan: the partly read file
    assert "SLICE" not in plan
    assert "SELECTION" in plan and "Actor1CountryCode" in plan.split("SELECTION")[1]
    rows = lambda batches: pl.concat(batches).select(  # noqa: E731
        samplers_module._ROW_FILE, samplers_module._ROW_POSITION
    )
    file, position = pl.col(samplers_module._ROW_FILE), pl.col(samplers_module._ROW_POSITION)
    starts_at = rows(whole).with_row_index().filter(
        (file > 1) | ((file == 1) & (position >= 2500))
    )
    assert rows(resumed).equals(starts_at.drop("index"))


def test_the_checkpoint_guard_covers_every_paths_directory_and_symlinks(tmp_path):
    import argparse
    import os

    import gdeltforge.cli as cli

    historical = tmp_path / "cleaned_historical"
    historical.mkdir()
    config = {
        "io": {"checkpoint_every_s": 60},
        "paths": {
            "cleaned_historical_directory": str(historical),
            "checkpoint_directory": str(historical / "ck"),
        },
    }
    args = argparse.Namespace(no_checkpoint=False)
    with pytest.raises(ValueError, match="a data directory, where its Parquet parts"):
        cli._checkpoint_settings(config, args, cli._data_directories(config))
    link = tmp_path / "link"
    try:
        os.symlink(historical, link, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks need privileges on this system")
    config["paths"]["checkpoint_directory"] = str(link / "ck")
    with pytest.raises(ValueError, match="checkpoint directory"):
        cli._checkpoint_settings(config, args, cli._data_directories(config))


def test_checkpoint_parts_are_read_with_their_schema(tmp_path, monkeypatch):
    import gdeltforge.utils.checkpoint as checkpoint_module

    ckpt = Checkpoint(CheckpointSettings(tmp_path, 0), "crossref-x", {"n": 1})
    ckpt.save({}, {}, [pl.DataFrame({"y": [1, 2]})])
    read: list = []
    real = checkpoint_module.read_parquet_path
    monkeypatch.setattr(
        checkpoint_module, "read_parquet_path", lambda p: read.append(p) or real(p)
    )
    loaded = Checkpoint(CheckpointSettings(tmp_path, 0), "crossref-x", {"n": 1}).load()
    assert loaded is not None
    assert len(read) == 1 and loaded[2][0]["y"].to_list() == [1, 2]
