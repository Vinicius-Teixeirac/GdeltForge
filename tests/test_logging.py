import logging
from pathlib import Path

from gdeltforge.utils.logging import get_logger


class TestGetLoggerUnwritableLogDirectory:
    """
    cli.py calls get_logger(__name__, log_to_file=True) unconditionally
    at module import time, before argparse or main()'s own try/except
    ever run. get_logger() used to do Path("logs").mkdir(exist_ok=True)
    with no error handling at all, so any command, including --help,
    crashed with a raw, unhandled PermissionError in a working directory
    the process can't write to (a read-only container, some CI runners,
    an HPC scratch/read-only home setup).

    Real chmod-based permission tests aren't reliable across platforms
    (Windows doesn't enforce a chmod'd directory as unwritable the same
    way POSIX does), so this mocks Path.mkdir to raise directly, the
    exact call get_logger makes and the exact exception class a real
    unwritable directory produces.
    """

    def test_log_to_file_does_not_raise_when_log_dir_cannot_be_created(
        self, monkeypatch
    ):
        def raise_permission_error(_self, *_args, **_kwargs):
            raise PermissionError(13, "Permission denied", "logs")

        monkeypatch.setattr(Path, "mkdir", raise_permission_error)

        logger = get_logger("gdeltforge.test_unwritable_log_dir", log_to_file=True)

        assert logger is not None
        assert not any(isinstance(h, logging.FileHandler) for h in logger.handlers)

    def test_log_to_file_does_not_raise_when_file_handler_cannot_open(
        self, monkeypatch, tmp_path
    ):
        # mkdir succeeds (e.g. the directory already exists) but the
        # FileHandler's own open() call is what actually fails, e.g. an
        # existing "logs" directory that itself lost write permission
        # after being created, or a full disk.
        monkeypatch.chdir(tmp_path)

        def raise_permission_error(_self, *_args, **_kwargs):
            raise PermissionError(13, "Permission denied", "logs/pipeline.log")

        # Patch __init__, not the class itself: replacing logging.FileHandler
        # wholesale would also break the isinstance(h, logging.FileHandler)
        # check get_logger() uses to decide whether a file handler already
        # exists, since the module-level `logging` object is shared process-
        # wide.
        monkeypatch.setattr(logging.FileHandler, "__init__", raise_permission_error)

        logger = get_logger("gdeltforge.test_unwritable_log_file", log_to_file=True)

        assert logger is not None
        assert not any(isinstance(h, logging.FileHandler) for h in logger.handlers)

    def test_still_logs_to_console_after_falling_back(self, monkeypatch, capsys):
        def raise_permission_error(_self, *_args, **_kwargs):
            raise PermissionError(13, "Permission denied", "logs")

        monkeypatch.setattr(Path, "mkdir", raise_permission_error)

        logger = get_logger("gdeltforge.test_console_fallback", log_to_file=True)
        logger.warning("still reaches the console")

        assert "still reaches the console" in capsys.readouterr().err

    def test_log_to_file_still_works_normally_when_writable(self, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)

        logger = get_logger("gdeltforge.test_writable_log_dir", log_to_file=True)

        assert any(isinstance(h, logging.FileHandler) for h in logger.handlers)
        assert (tmp_path / "logs" / "pipeline.log").exists()
