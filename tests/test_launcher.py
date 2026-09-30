import logging
import os
import subprocess
import sys
import textwrap

import pytest

import gdeltforge.cli as cli
from gdeltforge.launcher import scan_thread_cap


def _config(tmp_path, text="io:\n  max_concurrent_reads: 3\n"):
    path = tmp_path / "settings.yaml"
    path.write_text(text, encoding="utf-8")
    return str(path)


class TestScanThreadCap:
    """sample and crossref fetch file footers on polars' thread pool, so with
    io.max_concurrent_reads set they start polars with at most that many
    threads; the launcher has to find the value before polars loads."""

    @pytest.mark.parametrize("command", ["sample", "crossref"])
    def test_scanning_commands_get_the_cap(self, tmp_path, command):
        argv = ["--config", _config(tmp_path), command, "--dataset", "events"]
        assert scan_thread_cap(argv, {}) == 3

    def test_config_given_with_an_equals_sign(self, tmp_path):
        assert scan_thread_cap([f"--config={_config(tmp_path)}", "sample"], {}) == 3

    def test_config_from_the_environment_variable(self, tmp_path):
        assert scan_thread_cap(["sample"], {"GDELTFORGE_CONFIG": _config(tmp_path)}) == 3

    def test_config_from_the_default_path(self, tmp_path, monkeypatch):
        (tmp_path / "config").mkdir()
        (tmp_path / "config" / "settings.yaml").write_text(
            "io:\n  max_concurrent_reads: 2\n", encoding="utf-8"
        )
        monkeypatch.chdir(tmp_path)
        assert scan_thread_cap(["crossref"], {}) == 2

    @pytest.mark.parametrize("command", ["clean", "aggregate", "convert", "scrape", "codes"])
    def test_other_commands_are_left_alone(self, tmp_path, command):
        assert scan_thread_cap(["--config", _config(tmp_path), command], {}) is None

    def test_an_exported_value_wins(self, tmp_path):
        argv = ["--config", _config(tmp_path), "sample"]
        assert scan_thread_cap(argv, {"POLARS_MAX_THREADS": "16"}) is None

    @pytest.mark.parametrize("text", [
        "io:\n  max_concurrent_reads: null\n",
        "io:\n  max_concurrent_reads: '4'\n",
        "io:\n  max_concurrent_reads: true\n",
        "io:\n  max_concurrent_reads: 0\n",
        "clean: {}\n",
        "io: [\n",
    ])
    def test_no_cap_or_an_invalid_one_is_left_to_the_config_validation(self, tmp_path, text):
        assert scan_thread_cap(["--config", _config(tmp_path, text), "sample"], {}) is None

    def test_a_missing_config_file_is_left_alone(self, tmp_path):
        assert scan_thread_cap(["--config", str(tmp_path / "nope.yaml"), "sample"], {}) is None

    def test_no_command(self):
        assert scan_thread_cap(["--version"], {}) is None

    def test_the_polars_pool_follows_the_cap(self, tmp_path):
        # In a fresh interpreter: the launcher runs before polars loads.
        # gdeltforge.cli is replaced by a stub that reports the pool size.
        script = textwrap.dedent(f"""
            import sys, types
            stub = types.ModuleType("gdeltforge.cli")
            def main():
                import polars
                print(polars.thread_pool_size())
            stub.main = main
            sys.modules["gdeltforge.cli"] = stub
            sys.argv = ["gdeltforge", "--config", {_config(tmp_path)!r}, "sample"]
            from gdeltforge.launcher import main as launch
            launch()
        """)
        env = {k: v for k, v in os.environ.items() if k != "POLARS_MAX_THREADS"}
        out = subprocess.run(
            [sys.executable, "-c", script], env=env, capture_output=True, text=True, check=True
        )
        assert out.stdout.strip() == "3"


class TestReportScanThreads:
    def test_warns_when_polars_runs_more_threads_than_the_cap(self, monkeypatch, caplog):
        monkeypatch.setattr(cli.pl, "thread_pool_size", lambda: 16)
        with caplog.at_level(logging.INFO):
            cli._report_scan_threads(4)
        assert any(
            r.levelno == logging.WARNING and "POLARS_MAX_THREADS=4" in r.message
            for r in caplog.records
        )

    @pytest.mark.parametrize("exported, warned", [("8", True), ("x", True), ("4", False)])
    def test_an_exported_scan_limit_above_the_cap_is_warned_about(
        self, monkeypatch, caplog, exported, warned
    ):
        monkeypatch.setattr(cli.pl, "thread_pool_size", lambda: 4)
        monkeypatch.setenv("POLARS_MAX_CONCURRENT_SCANS", exported)
        with caplog.at_level(logging.INFO, logger=cli.logger.name):
            cli._report_scan_threads(4)
        messages = [r.message for r in caplog.records]
        assert any("POLARS_MAX_CONCURRENT_SCANS" in m for m in messages) is warned
        # The "at most N" line only when nothing exceeds the cap.
        assert any("Reading at most 4" in m for m in messages) is not warned

    def test_reports_the_bound_when_the_pool_fits(self, monkeypatch, caplog):
        monkeypatch.setattr(cli.pl, "thread_pool_size", lambda: 4)
        monkeypatch.delenv("POLARS_MAX_CONCURRENT_SCANS", raising=False)
        # On the CLI's own logger: an earlier --quiet run can leave it at
        # WARNING, which the root level caplog sets doesn't override.
        with caplog.at_level(logging.INFO, logger=cli.logger.name):
            cli._report_scan_threads(4)
        assert any("at most 4 file(s) at once, on 4 polars" in r.message for r in caplog.records)

    def test_silent_without_a_cap(self, caplog):
        with caplog.at_level(logging.INFO):
            cli._report_scan_threads(None)
        assert caplog.records == []
