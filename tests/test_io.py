import logging
from pathlib import Path

import pandas as pd
import pytest

from gdeltforge.utils.io import write_parquet_atomic


class TestWriteParquetAtomic:
    def test_writes_file_and_leaves_no_tmp_behind(self, tmp_path):
        out = tmp_path / "sample.parquet"
        df = pd.DataFrame({"GlobalEventID": [1, 2, 3]})

        write_parquet_atomic(df, out)

        assert out.exists()
        assert pd.read_parquet(out)["GlobalEventID"].tolist() == [1, 2, 3]
        assert not (tmp_path / "sample.parquet.tmp").exists()

    def test_warns_and_overwrites_leftover_tmp_from_interrupted_run(self, tmp_path, caplog):
        out = tmp_path / "sample.parquet"
        tmp_path_leftover = tmp_path / "sample.parquet.tmp"
        tmp_path_leftover.write_bytes(b"partial garbage from a killed run")

        df = pd.DataFrame({"GlobalEventID": [1, 2, 3]})
        with caplog.at_level(logging.WARNING):
            write_parquet_atomic(df, out)

        assert "leftover incomplete file" in caplog.text
        assert pd.read_parquet(out)["GlobalEventID"].tolist() == [1, 2, 3]
        assert not tmp_path_leftover.exists()

    def test_cleans_up_tmp_and_reraises_on_write_failure(self, tmp_path, monkeypatch):
        out = tmp_path / "sample.parquet"

        def boom(self, path, *args, **kwargs):
            Path(path).write_bytes(b"partial write before failure")
            raise OSError("disk full")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

        with pytest.raises(OSError):
            write_parquet_atomic(pd.DataFrame({"a": [1]}), out)

        assert not out.exists()
        assert not (tmp_path / "sample.parquet.tmp").exists()
