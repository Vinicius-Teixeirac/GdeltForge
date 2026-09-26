import logging
import re
from pathlib import Path

import pytest
import yaml

import gdeltforge.utils.config as config_module
from gdeltforge.utils.config import (
    CONFIG_ENV_VAR,
    dataset_is_aggregation_eligible,
    dataset_path_key,
    get_dict,
    load_config,
    resolve_max_concurrent_reads,
    validate_max_workers,
)


class TestResolveMaxConcurrentReads:
    def test_missing_section_means_no_cap(self):
        assert resolve_max_concurrent_reads({}) is None

    def test_null_section_means_no_cap(self):
        assert resolve_max_concurrent_reads({"io": None}) is None

    def test_reads_the_configured_value(self):
        assert resolve_max_concurrent_reads({"io": {"max_concurrent_reads": 4}}) == 4

    def test_rejects_zero(self):
        with pytest.raises(ValueError, match="io.max_concurrent_reads must be greater than 0"):
            resolve_max_concurrent_reads({"io": {"max_concurrent_reads": 0}})

    def test_bundled_default_ships_no_cap(self):
        # SSD-first default: a config that never mentions io still gets the
        # section from the bundled default, with no cap in it.
        assert config_module._bundled_default_dict()["io"] == {"max_concurrent_reads": None}


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
        assert dataset_path_key("gdelt_mentions", "cleaned_data_directory") == (
            "mentions_cleaned_data_directory"
        )

    def test_unknown_dataset_raises(self):
        with pytest.raises(ValueError, match="Unknown dataset"):
            dataset_path_key("not_a_real_dataset", "downloaded_data_directory")


class TestDatasetIsAggregationEligible:
    def test_the_three_15_minute_datasets_are_eligible(self):
        assert dataset_is_aggregation_eligible("gdelt_gkg_v2") is True
        assert dataset_is_aggregation_eligible("gdelt_mentions") is True
        assert dataset_is_aggregation_eligible("gdelt_event_15min") is True

    def test_day_or_coarser_datasets_are_not_eligible(self):
        assert dataset_is_aggregation_eligible("gdelt_event") is False
        assert dataset_is_aggregation_eligible("gdelt_event_reduced") is False
        assert dataset_is_aggregation_eligible("gdelt_gkg_v1") is False
        assert dataset_is_aggregation_eligible("gdelt_gkg_v1_counts") is False


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
            {"clean": {"max_workers": 4}},
            {"clean": {"max_workers": None, "columns_to_check": {"gdelt_event": []}}},
        )
        assert merged["clean"] == {"max_workers": 4, "columns_to_check": {"gdelt_event": []}}

    def test_a_users_list_value_is_never_merged_element_by_element(self):
        # A user's own (possibly empty) columns_to_check list for a dataset
        # must win outright, not get padded with the default's entries for
        # that same dataset: only dict values recurse, never lists.
        merged = config_module._deep_merge_defaults(
            {"clean": {"columns_to_check": {"gdelt_event": []}}},
            {"clean": {"columns_to_check": {"gdelt_event": ["Actor1Name"]}}},
        )
        assert merged["clean"]["columns_to_check"]["gdelt_event"] == []

    def test_original_dicts_are_not_mutated(self):
        config = {"clean": {"max_workers": 4}}
        defaults = {"clean": {"max_workers": None, "columns_to_check": {}}}

        config_module._deep_merge_defaults(config, defaults)

        assert config == {"clean": {"max_workers": 4}}
        assert defaults == {"clean": {"max_workers": None, "columns_to_check": {}}}


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

    def test_invalid_yaml_raises_a_crafted_error_naming_the_config_file(self):
        # yaml.safe_load's own exception used to propagate unwrapped: a
        # raw PyYAML message ("while parsing a flow sequence", "expected
        # ',' or ']', but got '<stream end>'") that never says the
        # problem is in the config file at all, unlike every other
        # malformed-config case here (a missing file, an empty file, a
        # directory instead of a file), which already gets a clear,
        # crafted message naming the actual path.
        bad = self.tmp_path / "bad.yaml"
        bad.write_text("paths:\n  foo: [unclosed\n")

        with pytest.raises(ValueError, match=re.escape(str(bad))):
            load_config(str(bad))

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
            "columns", "columns_numeric", "paths", "scraping", "converter", "clean",
            "aggregation", "io",
        }
        assert any("built-in default" in r.message for r in caplog.records)

    def test_bundled_default_is_conservative_no_row_or_column_filtering(self):
        # The specific design choice this session settled on: a fresh
        # zero-config run must never silently drop rows or columns.
        config = load_config()

        for columns in config["clean"]["columns_to_check"].values():
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
        assert config["clean"] == defaults["clean"]
        # And the .get() chains real call sites use no longer raise:
        assert config["converter"].get("max_workers") is None
        assert config["clean"].get("output_columns", {}).get("gdelt_event") is None

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
        assert config["clean"]["columns_to_check"] == defaults["clean"]["columns_to_check"]
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
            "columns", "columns_numeric", "paths", "scraping", "converter", "clean",
            "aggregation", "io",
        }
        assert any(
            "in memory only" in r.message and "read-only filesystem" in r.message
            for r in caplog.records
        )


class TestDeprecatedCleanNames:
    """The clean stage was called `filter` before 0.12: an existing
    settings.yaml using the old section and path-key names keeps loading,
    translated, with a warning per name."""

    def test_filter_section_and_path_keys_are_translated(self, tmp_path, caplog):
        path = tmp_path / "settings.yaml"
        path.write_text(
            "filter:\n"
            "  columns_to_check:\n"
            "    gdelt_event: [Actor1Code]\n"
            "paths:\n"
            "  filtered_data_directory: ./old/events/filtered\n"
            "  gkg_v2_filtered_historical_directory: ./old/gkg/hist\n",
            encoding="utf-8",
        )
        with caplog.at_level(logging.WARNING):
            config = load_config(str(path))
        assert "filter" not in config
        assert config["clean"]["columns_to_check"]["gdelt_event"] == ["Actor1Code"]
        assert config["paths"]["cleaned_data_directory"] == "./old/events/filtered"
        assert config["paths"]["gkg_v2_cleaned_historical_directory"] == "./old/gkg/hist"
        assert "filtered_data_directory" not in config["paths"]
        messages = " ".join(r.message for r in caplog.records)
        assert "`filter:` section is deprecated" in messages
        assert "paths.filtered_data_directory is deprecated" in messages

    def test_old_and_new_section_together_is_an_error(self, tmp_path):
        path = tmp_path / "settings.yaml"
        path.write_text("filter: {max_workers: 2}\nclean: {max_workers: 4}\n", encoding="utf-8")
        with pytest.raises(ValueError, match="sets both `filter:` and `clean:`"):
            load_config(str(path))

    def test_old_and_new_path_key_together_is_an_error(self, tmp_path):
        path = tmp_path / "settings.yaml"
        path.write_text(
            "paths:\n  filtered_data_directory: a\n  cleaned_data_directory: b\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="paths.filtered_data_directory and"):
            load_config(str(path))


class TestMovedDefaultDirectories:
    """0.12.0 moved the bundled default's clean-stage directories from
    data/<dataset>/filtered to data/<dataset>/cleaned; a config leaving them
    to the default, next to output from an earlier version, gets a warning
    naming both."""

    def _load(self, tmp_path, monkeypatch, caplog, text="clean: {max_workers: 2}\n"):
        monkeypatch.chdir(tmp_path)
        path = tmp_path / "settings.yaml"
        path.write_text(text, encoding="utf-8")
        with caplog.at_level(logging.WARNING):
            load_config(str(path))
        return [r.message for r in caplog.records if "holds clean-stage output" in r.message]

    def test_warns_when_only_the_old_default_directory_exists(
        self, tmp_path, monkeypatch, caplog
    ):
        (tmp_path / "data" / "mentions" / "filtered").mkdir(parents=True)
        (tmp_path / "data" / "events" / "filtered_historical").mkdir(parents=True)
        messages = self._load(tmp_path, monkeypatch, caplog)
        assert len(messages) == 2
        mentions = next(m for m in messages if "mentions" in m)
        assert "paths.mentions_cleaned_data_directory" in mentions
        assert str(Path("data/mentions/filtered")) in mentions
        assert str(Path("data/mentions/cleaned")) in mentions
        assert any("paths.cleaned_historical_directory" in m for m in messages)

    def test_quiet_once_the_new_directory_exists(self, tmp_path, monkeypatch, caplog):
        (tmp_path / "data" / "mentions" / "filtered").mkdir(parents=True)
        (tmp_path / "data" / "mentions" / "cleaned").mkdir(parents=True)
        assert self._load(tmp_path, monkeypatch, caplog) == []

    def test_quiet_when_the_config_sets_the_path(self, tmp_path, monkeypatch, caplog):
        (tmp_path / "data" / "mentions" / "filtered").mkdir(parents=True)
        text = "paths:\n  mentions_filtered_data_directory: ./data/mentions/filtered\n"
        assert self._load(tmp_path, monkeypatch, caplog, text) == []

    def test_the_bundled_default_fallback_warns_too(self, tmp_path, monkeypatch, caplog):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv(CONFIG_ENV_VAR, raising=False)
        (tmp_path / "data" / "gkg_v2" / "filtered").mkdir(parents=True)
        with caplog.at_level(logging.WARNING):
            load_config()
        assert any(
            "paths.gkg_v2_cleaned_data_directory" in r.message for r in caplog.records
        )
