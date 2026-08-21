import logging
from pathlib import Path

import pytest
import yaml

import gdeltforge.utils.config as config_module
from gdeltforge.utils.config import CONFIG_ENV_VAR, dataset_path_key, load_config


class TestDatasetPathKey:
    def test_events_keeps_unprefixed_key(self):
        # Events predates multi-dataset support; its paths.* keys must stay
        # unprefixed so existing settings.yaml files keep working unchanged.
        assert dataset_path_key("gdelt_event", "downloaded_data_directory") == (
            "downloaded_data_directory"
        )

    def test_other_datasets_get_a_prefixed_key(self):
        assert dataset_path_key("gdelt_gkg_v1", "downloaded_data_directory") == (
            "gkg_v1_downloaded_data_directory"
        )
        assert dataset_path_key("gdelt_gkg_v2", "parquet_data_directory") == (
            "gkg_v2_parquet_data_directory"
        )
        assert dataset_path_key("gdelt_mentions", "filtered_data_directory") == (
            "mentions_filtered_data_directory"
        )

    def test_unknown_dataset_raises(self):
        with pytest.raises(ValueError, match="Unknown dataset"):
            dataset_path_key("not_a_real_dataset", "downloaded_data_directory")


class TestModuleLoggerIsProperlyConfigured:
    """Regression test for a real bug found in review: this module used
    to build its logger with a bare logging.getLogger(__name__) instead
    of gdeltforge.utils.logging.get_logger, the helper every other
    module in the codebase goes through. The warnings still reached the
    terminal either way (Python's own logging.lastResort fallback
    catches an unconfigured logger's WARNING+ records), so nothing was
    silently lost, but with zero formatting: no "WARNING" label, no
    timestamp, indistinguishable from ordinary print output and
    inconsistent with every other warning this tool emits. caplog alone
    can't catch this class of bug: it captures log records directly,
    the same regardless of which logger built them, so this checks the
    module's actual handler setup instead."""

    def test_logger_was_built_via_get_logger_not_a_bare_getlogger(self):
        # get_logger() eagerly attaches a formatted StreamHandler at
        # import time; a bare logging.getLogger(__name__) attaches none.
        assert config_module.logger.handlers, (
            "config_module.logger has no handlers, it's probably using "
            "logging.getLogger(__name__) directly instead of "
            "gdeltforge.utils.logging.get_logger(__name__)"
        )


class TestLoadConfig:
    """load_config()'s fallback chain: an explicit --config or
    GDELTFORGE_CONFIG that's missing must still raise clearly (almost
    always a typo), but a bare `gdeltforge <command>` with no config
    anywhere, the exact situation a fresh `pip install gdeltforge` in
    a Colab session hits on every single run, since pip doesn't drop
    config/settings.example.yaml into the working directory the way a
    git clone does, now falls back to the bundled default instead of
    a hard FileNotFoundError."""

    @pytest.fixture(autouse=True)
    def _isolated_cwd(self, tmp_path, monkeypatch):
        # Every test in this class runs from an empty directory with no
        # GDELTFORGE_CONFIG set, so the ambient repo's own real
        # config/settings.yaml (if one happens to exist) can never leak
        # into a test's result.
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv(CONFIG_ENV_VAR, raising=False)
        self.tmp_path = tmp_path

    def test_explicit_config_path_is_used_when_present(self):
        custom = self.tmp_path / "custom.yaml"
        custom.write_text("columns: {gdelt_event: [GlobalEventID]}\n")

        config = load_config(str(custom))

        assert config == {"columns": {"gdelt_event": ["GlobalEventID"]}}

    def test_explicit_config_path_missing_raises_not_falls_back(self):
        # A typo in --config must surface as an error, not silently
        # substitute an unrelated built-in default.
        with pytest.raises(FileNotFoundError, match="not-a-real-file.yaml"):
            load_config(str(self.tmp_path / "not-a-real-file.yaml"))

    def test_env_var_is_used_when_config_path_argument_is_none(self, monkeypatch):
        custom = self.tmp_path / "from_env.yaml"
        custom.write_text("columns: {gdelt_event: [GlobalEventID]}\n")
        monkeypatch.setenv(CONFIG_ENV_VAR, str(custom))

        config = load_config()

        assert config == {"columns": {"gdelt_event": ["GlobalEventID"]}}

    def test_env_var_missing_raises_not_falls_back(self, monkeypatch):
        monkeypatch.setenv(CONFIG_ENV_VAR, str(self.tmp_path / "not-a-real-file.yaml"))

        with pytest.raises(FileNotFoundError, match="not-a-real-file.yaml"):
            load_config()

    def test_default_path_is_used_when_present(self):
        # config/settings.yaml relative to cwd, still takes priority
        # over the bundled default when it's actually there.
        (self.tmp_path / "config").mkdir()
        (self.tmp_path / "config" / "settings.yaml").write_text(
            "columns: {gdelt_event: [GlobalEventID]}\n"
        )

        config = load_config()

        assert config == {"columns": {"gdelt_event": ["GlobalEventID"]}}

    def test_falls_back_to_bundled_default_when_nothing_is_configured(self, caplog):
        with caplog.at_level(logging.WARNING):
            config = load_config()

        assert set(config) == {
            "columns", "columns_numeric", "paths", "scraping", "converter", "filter",
        }
        assert any("built-in default" in r.message for r in caplog.records)

    def test_bundled_default_is_conservative_no_row_or_column_filtering(self):
        # The specific design choice this session settled on: a fresh
        # zero-config run must never silently drop rows or columns.
        config = load_config()

        for columns in config["filter"]["columns_to_check"].values():
            assert columns == []
        assert "output_columns" not in config.get("filter", {})
        assert "output_columns" not in config.get("converter", {})
        assert "float32_columns" not in config.get("filter", {})

    def test_bundled_default_paths_are_real_not_placeholders(self):
        # Unlike settings.example.yaml's "./path_example/..." (never
        # meant to be used as-is), the bundled default's paths must be
        # immediately usable relative to wherever the command runs.
        config = load_config()

        for value in config["paths"].values():
            assert "path_example" not in value
            assert value.startswith("./data/")

    def test_falling_back_materializes_a_real_editable_file(self):
        assert not (self.tmp_path / "config" / "settings.yaml").exists()

        config = load_config()

        written = self.tmp_path / "config" / "settings.yaml"
        assert written.exists()
        assert yaml.safe_load(written.read_text()) == config

    def test_second_call_after_materializing_reads_the_now_real_file(self):
        # Not just "the fallback happens to work twice": after the first
        # call writes config/settings.yaml, a second call must go
        # through the normal "path exists" branch and pick up an
        # in-session edit to it, not silently re-serve the bundled
        # default from memory forever.
        load_config()
        written = self.tmp_path / "config" / "settings.yaml"
        written.write_text("columns: {gdelt_event: [EditedByUser]}\n")

        config = load_config()

        assert config == {"columns": {"gdelt_event": ["EditedByUser"]}}

    def test_empty_top_level_section_is_normalized_to_an_empty_dict(self):
        # A section key present with nothing indented under it (or an
        # explicit `converter: null`) parses to None, not {}. Every
        # downstream config["converter"].get(...) call assumes a dict;
        # left as None this used to crash with a bare "'NoneType' object
        # has no attribute 'get'" the moment that section was touched.
        custom = self.tmp_path / "custom.yaml"
        custom.write_text(
            "columns: {gdelt_event: [GlobalEventID]}\n"
            "converter:\n"
            "filter: null\n"
        )

        config = load_config(str(custom))

        assert config["converter"] == {}
        assert config["filter"] == {}
        # And the .get() chains real call sites use no longer raise:
        assert config["converter"].get("max_workers") is None
        assert config["filter"].get("output_columns", {}).get("gdelt_event") is None

    def test_sections_with_real_content_are_left_untouched(self):
        custom = self.tmp_path / "custom.yaml"
        custom.write_text(
            "columns: {gdelt_event: [GlobalEventID]}\n"
            "converter: {max_workers: 4}\n"
        )

        config = load_config(str(custom))

        assert config["converter"] == {"max_workers": 4}

    def test_empty_config_file_raises_clearly(self):
        empty = self.tmp_path / "empty.yaml"
        empty.write_text("")

        with pytest.raises(ValueError, match="empty"):
            load_config(str(empty))

    def test_write_failure_falls_back_to_in_memory_only(self, monkeypatch, caplog):
        def _boom(*_args, **_kwargs):
            raise OSError("read-only filesystem")

        monkeypatch.setattr(Path, "write_text", _boom)

        with caplog.at_level(logging.WARNING):
            config = load_config()

        assert set(config) == {
            "columns", "columns_numeric", "paths", "scraping", "converter", "filter",
        }
        assert any(
            "in memory only" in r.message and "read-only filesystem" in r.message
            for r in caplog.records
        )
