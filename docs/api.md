# Python API and Stability

What gdeltforge supports as its public surface: the names, commands, settings and files
you can build on, and how they change between releases. Anything not listed here is
internal and may change in any release, without notice in the
[CHANGELOG](https://github.com/Vinicius-Teixeirac/GdeltForge/blob/main/CHANGELOG.md).

## How releases change it

gdeltforge follows [Semantic Versioning](https://semver.org/). Before 1.0:

- A **patch** release (`0.20.1`) fixes bugs and changes nothing listed on this page.
- A **minor** release (`0.21.0`) may add to it and change what it lists. Every change
  that can break existing use is marked **Breaking** in the CHANGELOG, with what to
  use instead.
- A name this page covers (a command, flag, value, settings key or Python name) that is
  deprecated keeps working, with a warning naming its replacement, until the next major
  release: 1.0.0 for a name deprecated before it. This holds from 0.20.0, which removed
  the last names deprecated under the earlier practice.
- A change that can't carry a warning, such as which rows a seed draws or a default, is
  made in a minor release and marked **Breaking**.

## Python names

Import each name from the module it is listed under. The stage modules carry no
`__init__` re-exports, so `from gdeltforge.cleaning import GDELTCleaner` does not work.

| Module | Supported names |
|---|---|
| `gdeltforge.scraping.scraper` | `run_scraping_pipeline`, `date_parser_for` |
| `gdeltforge.conversion.converter` | `run_converter`, `GDELTConverter` |
| `gdeltforge.cleaning.cleaner` | `run_cleaner`, `GDELTCleaner` |
| `gdeltforge.aggregation.aggregator` | `run_aggregator`, `GDELTAggregator` |
| `gdeltforge.sampling.samplers` | `IndexedSampler`, `CalendarSampler`, `FilteredSampler` |
| `gdeltforge.crossref.crossref` | `crossref_events_gkg_v1`, `crossref_events_gkg_v2`, `crossref_events_gkg_auto` |
| `gdeltforge.sampling.cameo_codes` | `lookup`, `is_recognized_code`, `code_family_for_column`, `family_name_for_column`, `actor_country_codes`, `geo_country_codes`, `ethnic_codes`, `known_group_codes`, `religion_codes`, `type_codes`, `event_codes` |
| `gdeltforge.utils.config` | `load_config` |
| `gdeltforge.utils.checkpoint` | `CheckpointSettings` |

For each of them:

- **Arguments**: pass everything after the first argument by keyword. Keyword names and
  their meaning are supported; their order is not.
- **Classes**: the constructor and the methods without a leading underscore
  (`GDELTCleaner.clean_all_files`, `IndexedSampler.get_random_sample`,
  `CalendarSampler.get_calendar_samples`, `FilteredSampler.get_random_sample`,
  `FilteredSampler.get_stratified_sample`, ...). Attributes are internal.
- **Datasets**: functions that take `dataset` use the config names: `gdelt_event`,
  `gdelt_event_15min`, `gdelt_event_reduced`, `gdelt_gkg_v1`, `gdelt_gkg_v1_counts`,
  `gdelt_gkg_v2`, `gdelt_mentions`. The CLI's `--dataset` values map onto them
  (`events` is `gdelt_event`; see [`--dataset`](cli-reference.md#-dataset)).
- **Configuration**: the `run_*` functions take the dict `load_config` returns, so a
  script reads the same settings file the CLI does.

A minimal script:

```python
from gdeltforge.sampling.samplers import IndexedSampler
from gdeltforge.scraping.scraper import date_parser_for
from gdeltforge.utils.config import load_config

config = load_config("config/settings.yaml")
sampler = IndexedSampler(
    config["paths"]["cleaned_data_directory"],
    random_state=42,
    columns=["GlobalEventID", "Day", "EventCode"],
    date_parser=date_parser_for("gdelt_event"),
)
sample = sampler.get_random_sample(1_000)
```

The CLI does more around each call (the source checks, the manifest, the log file), so
for a sample to cite, prefer `gdeltforge sample`.

Internal, whatever their names: everything in `gdeltforge.cli` and `gdeltforge.launcher`,
`gdeltforge.utils` apart from the two names above, `gdeltforge.sampling.indexer`,
`gdeltforge.sampling.rng`, `gdeltforge.cleaning.steps` and `gdeltforge.cleaning.places`,
and every name with a leading underscore.

## Command line

Every command, flag and value in the [CLI Reference](cli-reference.md), and the exit
status: `0` on success, `1` for an error, `2` for a command line argparse refuses, `130`
after Ctrl+C and `143` after `SIGTERM`. Log and progress output is for people and may
change.

## Settings

Every key in the bundled `src/gdeltforge/config/default_settings.yaml`, as
[Configuration](configuration.md) documents it, and the order in which a settings file
is found (`--config`, `GDELTFORGE_CONFIG`, `./config/settings.yaml`, the bundled
default). A key not in that file is ignored, so check spelling against it.

## Files gdeltforge writes

Supported, as their pages describe them:

- Each stage's Parquet output: file names, column names and types, and the
  `<stem>_cleaned.parquet` naming of `clean`.
- The Parquet metadata entries `gdeltforge:clean` ([Every cleaned file is
  marked](data-cleaning.md#every-cleaned-file-is-marked)) and `gdeltforge:aggregate`
  ([Row provenance](cli-reference.md#row-provenance)).
- A sample's `<out>.manifest.json` ([Sample manifest](cli-reference.md#sample-manifest))
  and `<out>.strata.json`, and the `SourceFile`/`SourceRow` columns of `--provenance`.
- The clean run audit ([The run audit](data-cleaning.md#the-run-audit)).

Internal, read and written by gdeltforge alone: the saved file index
([Saved file index](cli-reference.md#saved-file-index)), checkpoints
([Checkpoints](cli-reference.md#checkpoints)), and the `.done` markers of older
versions. They are caches: a release may rebuild or ignore them.

## Draws

The same data, seed and parameters give the same sample, row for row. A release that
changes which rows a seed draws is a minor release and marks the change **Breaking**. To
reproduce a sample exactly, draw it with the version its manifest names.
