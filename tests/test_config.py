import logging
from pathlib import Path

import pytest
import yaml

import gdeltforge.utils.config as config_module
from gdeltforge.utils.config import (
    CONFIG_ENV_VAR,
    dataset_path_key,
    get_dict,
    load_config,
    validate_max_workers,
)


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


class TestGetDict:
    """get_dict guards the same None-vs-{} YAML footgun as
    _normalize_top_level_sections below, one level deeper: an optional,
    dict-valued config subsection (converter.output_columns,
    filter.compression, etc.) commonly left blank while a user is still
    filling the config in. Every real call site chains a second .get(...)
    onto the result, which used to crash with "'NoneType' object has no
    attribute 'get'" the instant that happened."""

    def test_missing_key_returns_empty_dict(self):
        assert get_dict({}, "output_columns") == {}

    def test_explicit_null_returns_empty_dict_not_none(self):
        assert get_dict({"output_columns": None}, "output_columns") == {}

    def test_real_content_is_returned_unchanged(self):
        section = {"output_columns": {"gdelt_event": ["GlobalEventID"]}}
        assert get_dict(section, "output_columns") == {"gdelt_event": ["GlobalEventID"]}

    def test_chaining_a_second_get_onto_the_result_no_longer_crashes(self):
        # The exact shape every real call site uses.
        assert get_dict({"compression": None}, "compression").get("gdelt_event", "zstd") == "zstd"


class TestDeepMergeDefaults:
    """_deep_merge_defaults fills in whatever a user's config doesn't
    mention, one level at a time, so a hand-written settings.yaml only
    needs to specify what it actually wants to change. Complements
    _normalize_top_level_sections/get_dict above, which handle a section
    being present-but-null; this handles a section, or a key inside one,
    being absent entirely."""

    def test_missing_top_level_key_is_filled_in(self):
        config = {"columns": {"gdelt_event": ["A"]}}
        defaults = {"columns": {"gdelt_event": ["A"]}, "paths": {"x": "y"}}
        merged = config_module._deep_merge_defaults(config, defaults)
        assert merged["paths"] == {"x": "y"}

    def test_present_top_level_key_is_never_overwritten(self):
        merged = config_module._deep_merge_defaults(
            {"scraping": {"timeout": 60}}, {"scraping": {"timeout": 30, "retries": 3}}
        )
        # timeout keeps the user's value; retries, which the user didn't
        # mention, is filled in from the default alongside it.
        assert merged["scraping"] == {"timeout": 60, "retries": 3}

    def test_missing_nested_key_is_filled_in(self):
        merged = config_module._deep_merge_defaults(
            {"filter": {"max_workers": 4}},
            {"filter": {"max_workers": None, "columns_to_check": {"gdelt_event": []}}},
        )
        assert merged["filter"] == {"max_workers": 4, "columns_to_check": {"gdelt_event": []}}

    def test_a_users_list_value_is_never_merged_element_by_element(self):
        # A user's own (possibly empty) columns_to_check list for a dataset
        # must win outright, not get padded with the default's entries for
        # that same dataset: only dict values recurse, never lists.
        merged = config_module._deep_merge_defaults(
            {"filter": {"columns_to_check": {"gdelt_event": []}}},
            {"filter": {"columns_to_check": {"gdelt_event": ["Actor1Name"]}}},
        )
        assert merged["filter"]["columns_to_check"]["gdelt_event"] == []

    def test_original_dicts_are_not_mutated(self):
        config = {"filter": {"max_workers": 4}}
        defaults = {"filter": {"max_workers": None, "columns_to_check": {}}}

        config_module._deep_merge_defaults(config, defaults)

        assert config == {"filter": {"max_workers": 4}}
        assert defaults == {"filter": {"max_workers": None, "columns_to_check": {}}}


class TestValidateMaxWorkers:
    """converter.max_workers/filter.max_workers: 0 used to reach
    ProcessPoolExecutor unchecked, since 0 is falsy in Python. A
    pre-flight log line's own `config_value or cpu_count()`-style
    fallback silently took the same branch a genuinely unset (None)
    value would, reporting the real CPU count, one line before
    ProcessPoolExecutor's own constructor raised "max_workers must be
    greater than 0" against the original, still-0 value: two
    contradictory statements about the same run. Checked explicitly here
    instead, before either the log line or the executor ever see it."""

    def test_none_is_returned_unchanged(self):
        assert validate_max_workers(None, "converter.max_workers") is None

    def test_a_positive_value_is_returned_unchanged(self):
        assert validate_max_workers(4, "converter.max_workers") == 4

    def test_zero_raises_naming_the_given_label(self):
        with pytest.raises(ValueError, match="converter.max_workers must be greater than 0"):
            validate_max_workers(0, "converter.max_workers")

    def test_negative_raises_the_same_way_as_zero(self):
        with pytest.raises(ValueError, match="filter.max_workers must be greater than 0"):
            validate_max_workers(-1, "filter.max_workers")


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

        # The user's own value for the key they set wins; everything else
        # (columns_numeric, paths, scraping, converter, filter, and every
        # other dataset's columns entry) is filled in from the bundled
        # default rather than left missing, see _deep_merge_defaults.
        expected = config_module._bundled_default_dict()
        expected["columns"]["gdelt_event"] = ["GlobalEventID"]
        assert config == expected

    def test_explicit_config_path_missing_raises_not_falls_back(self):
        # A typo in --config must surface as an error, not silently
        # substitute an unrelated built-in default.
        with pytest.raises(FileNotFoundError, match="not-a-real-file.yaml"):
            load_config(str(self.tmp_path / "not-a-real-file.yaml"))

    def test_explicit_config_path_pointing_at_a_directory_raises_clearly(self):
        # path.exists() is true for a directory too, so this used to
        # reach open(path) unchecked, raising a raw, unformatted
        # "[Errno 21] Is a directory: '...'" straight from the
        # filesystem, unlike a missing file, an empty file, or invalid
        # YAML at that same path, all of which get a crafted message.
        fake_dir = self.tmp_path / "fakedir.yaml"
        fake_dir.mkdir()

        with pytest.raises(IsADirectoryError, match="fakedir.yaml"):
            load_config(str(fake_dir))

    def test_env_var_is_used_when_config_path_argument_is_none(self, monkeypatch):
        custom = self.tmp_path / "from_env.yaml"
        custom.write_text("columns: {gdelt_event: [GlobalEventID]}\n")
        monkeypatch.setenv(CONFIG_ENV_VAR, str(custom))

        config = load_config()

        expected = config_module._bundled_default_dict()
        expected["columns"]["gdelt_event"] = ["GlobalEventID"]
        assert config == expected

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

        expected = config_module._bundled_default_dict()
        expected["columns"]["gdelt_event"] = ["GlobalEventID"]
        assert config == expected

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

        expected = config_module._bundled_default_dict()
        expected["columns"]["gdelt_event"] = ["EditedByUser"]
        assert config == expected

    def test_empty_top_level_section_is_normalized_then_filled_from_defaults(self):
        # A section key present with nothing indented under it (or an
        # explicit `converter: null`) parses to None, not {}. Every
        # downstream config["converter"].get(...) call assumes a dict;
        # left as None this used to crash with a bare "'NoneType' object
        # has no attribute 'get'" the moment that section was touched.
        # Normalizing it to {} and then merging the bundled default's own
        # converter/filter sections on top means an empty section behaves
        # exactly like an absent one: the caller gets the same working
        # defaults either way, not just a crash-free empty dict.
        custom = self.tmp_path / "custom.yaml"
        custom.write_text(
            "columns: {gdelt_event: [GlobalEventID]}\n"
            "converter:\n"
            "filter: null\n"
        )

        config = load_config(str(custom))

        defaults = config_module._bundled_default_dict()
        assert config["converter"] == defaults["converter"]
        assert config["filter"] == defaults["filter"]
        # And the .get() chains real call sites use no longer raise:
        assert config["converter"].get("max_workers") is None
        assert config["filter"].get("output_columns", {}).get("gdelt_event") is None

    def test_sections_with_real_content_keep_the_users_values(self):
        custom = self.tmp_path / "custom.yaml"
        custom.write_text(
            "columns: {gdelt_event: [GlobalEventID]}\n"
            "converter: {max_workers: 4}\n"
        )

        config = load_config(str(custom))

        # max_workers is the user's own value; every other converter key
        # they didn't mention (keep_unzipped, file_pattern, partitioning)
        # is filled in from the bundled default rather than missing
        # entirely, see _deep_merge_defaults.
        expected_converter = dict(config_module._bundled_default_dict()["converter"])
        expected_converter["max_workers"] = 4
        assert config["converter"] == expected_converter

    def test_missing_top_level_section_no_longer_crashes_downstream(self):
        # The gap _deep_merge_defaults actually closes: a section absent
        # from the user's file entirely (not present-and-null, which the
        # test above already covers), the more natural mistake for someone
        # writing a small, targeted config instead of starting from the
        # full settings.example.yaml. Every real call site reads these via
        # a direct config["..."][...] access, so a missing section used to
        # surface deep inside converter.py/filter.py as a bare
        # "Error: 'columns'" or "Error: 'columns_to_check'", naming the
        # missing key with no indication of which config file or section
        # was at fault.
        custom = self.tmp_path / "custom.yaml"
        custom.write_text("scraping: {timeout: 60}\n")

        config = load_config(str(custom))

        defaults = config_module._bundled_default_dict()
        assert config["columns"] == defaults["columns"]
        assert config["columns_numeric"] == defaults["columns_numeric"]
        assert config["filter"]["columns_to_check"] == defaults["filter"]["columns_to_check"]
        assert config["scraping"]["timeout"] == 60
        # Untouched scraping keys still come from the default alongside it.
        assert config["scraping"]["retries"] == defaults["scraping"]["retries"]

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
