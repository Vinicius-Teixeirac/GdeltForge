# Data cleaning: what `clean` does and why

`gdeltforge clean` is the pipeline's data-quality stage. This page explains
what it changes, in what order, and the decision behind each behavior, so
you can tell exactly how cleaned data differs from what GDELT published.
[Configuration](configuration.md#clean) lists the settings;
[CLI Reference](cli-reference.md#gdeltforge-clean) lists the flags.

## Quality, not relevance

`clean` decides whether a row is **usable** for any analysis. Whether a row
is **relevant** to a research question is decided at sampling time, by
`sample --mode filtered` and its [`--filter`](filtered-sampling.md) condition.

| | `clean` | `sample --mode filtered` |
|---|---|---|
| Question | Is this row usable at all? | Is this row relevant to my study? |
| Example | Drop rows with no event location, when your work needs one | Keep only QuadClass 3 or 4 events in Brazil |
| Applies to | The whole dataset, once; every later sample benefits | One sample, per run |
| Writes to disk | Yes, `cleaned_data_directory` | Only the sample |

Reasons for keeping the two apart:

- **One place for selection.** The filter language (equality, lists, ranges,
  nested AND/OR, schema validation, unrecognized-code warnings) lives in one
  place, with one syntax.
- **Exploring costs no disk.** Trying ten selections doesn't store ten
  copies of the archive.
- **Sampling keeps what only it can do**: `--stratify` (a fixed number of
  rows per group) and `--replace` (sampling with replacement).

So `clean` never selects rows by their content. When a new capability is
proposed for it, the test is this boundary: if it depends on a research
question, it belongs to sampling.

## Decisions

| Decision | Reason |
|---|---|
| **`convert` stays a faithful format change**; every correction lives in `clean` | The converted directory is always the copy to return to, holding exactly what GDELT published; everything opinionated sits in one documented, switchable place |
| **No statistical imputation** | A missing value in GDELT usually means something: an event with no second actor, a place the geocoder didn't find. Filling it in invents data |
| **Row-level, one file at a time** | Keeps the stage streaming, parallel and resumable. Operations that need the whole archive at once (deduplication across files, global statistics) are out of scope |
| **Known GDELT errors are repaired by default, and the defaults lose nothing** | Nearly everyone needs these repairs and almost nobody knows the errors exist. Each rule is exact, backed by evidence from the full archive, and can be switched off. The defaults keep GDELT's own values beside every repaired one and keep rows they flag, so a default run never discards anything GDELT published |
| **Steps run in one fixed order** | No configuration can produce an order-dependent result by accident |
| **Every step declares whether it's lossy** | A lossy step leaves the cleaned output unable to tell what the converted input held, which matters for deciding whether the converted copy can go |
| **Derived columns only add, and are off by default** | Adding a column never discards anything, so the step is never lossy; storing them costs width in every file, and which ones are useful depends on the analysis |
| **No option to store code columns as categoricals** | Measured on real Events files, it gains nothing and costs read time (see [below](#not-included-categorical-storage)) |
| **Normalization is available but off by default** | Whitespace padding is common in GDELT's 2013 files and breaks code lookups, but fixing it rewrites GDELT's values without keeping them, which the defaults never do |
| **`--delete-source` refuses a lossy step added in 0.12.0** (errata, normalize) unless `allow_lossy_delete_source: true` | Deleting the converted copy removes the only way back to what a lossy step discarded, so that combination has to be chosen deliberately. The original three steps (`columns_to_check`, `output_columns`, `float32_columns`) keep their long-standing warning, since existing setups rely on them to fit disk |
| **Every cleaned file says it's cleaned** | A cleaned file must never pass for GDELT's own data. Each one carries a `gdeltforge:clean` entry in its Parquet metadata; `clean` warns when its input already carries it, and `sample`/`aggregate`/`crossref` warn when a `--source converted` directory does |
| **Every run leaves an audit** | So you can always tell what a run changed, per file, without re-deriving it |
| **Output never goes into an input directory** | Cleaned files beside the converted ones would be cleaned again on the next run, and every reader of the converted directory would count those rows twice. `clean` refuses to start in that configuration |
| **Named `clean`**, `filter` before 0.12.0 | `filter` collided with `sample --filter`, which does the relevance job above. The old names keep working through 0.12.x with a deprecation warning |

## Steps, in order

Each file passes through the configured steps in this order:

| Order | Step | Setting | What it does | Lossy |
|---|---|---|---|---|
| 1 | errata | `errata.<dataset>` | Repairs known GDELT errors ([below](#errata-known-gdelt-errors)) | No, with the default settings |
| 2 | normalize | `normalize.<dataset>` | Trims whitespace, turns blank strings into null ([below](#normalize-whitespace)); off by default | Yes: GDELT's own spelling |
| 3 | require | `columns_to_check` | Drops rows with a null in any listed column | Yes: the rows |
| 4 | derive | `derive.<dataset>` | Adds a real event date and code labels ([below](#derive-added-columns)); off by default | No: only adds columns |
| 5 | project | `output_columns` | Keeps only the listed columns | Yes: the other columns |
| 6 | narrow | `float32_columns` | Stores the listed float columns as float32 | Yes: GDELT floats carry up to 15 significant figures, float32 about 7 |

Errata come first so every later step sees corrected values, and
normalization comes before the null check so a blank value counts as
missing there. Derived columns come after both, so they're built from
repaired values, and before projection, so `output_columns` can keep them.

Then the file is written with the configured `compression` (zstd by default,
lossless) as `<stem>_cleaned.parquet`, through a temporary file and a rename,
so an interrupted run never leaves a half-written file.

Apart from errata, every step is off unless configured, and the errata
defaults lose nothing: a first run with the bundled default configuration
keeps every row and every value GDELT published, with the repairs added.

## Errata: known GDELT errors

Rules for errors in GDELT's own data, set per dataset under
`clean.errata.<dataset>`. Each is on by default for the datasets it applies
to. A rule that finds nothing in a file changes nothing there.

### `date_1920`: events dated 1920 instead of 2020

GDELT wrote the year 1920 for 2020 in every event it dated in 2020 and
added between its 2019-12-31 23:00 UTC and 2020-01-05 15:00 UTC updates.
`Day`, `MonthYear`, `Year` and `FractionDate` all say 1920 (`19200102`,
`192001`, `1920`, `1920.0055`); `DATEADDED` is correct.

- **Evidence**: GDELT's own raw files carry it (the raw
  `20200102.export.CSV` has 120,828 such rows, the same count as the
  converted file), in the daily export (`events`) and the 15-minute feed
  (`events-15min`), not in Mentions. In the daily archive it covers 536,074
  rows, about 98% of the rows added on 2020-01-01 to 01-04 and 57% on
  01-05. No other row in 1979 to 2026 is dated before 1979.
- **Repair**: where `Day` is before 1979 and `DATEADDED` falls on
  2019-12-31 to 2020-01-05, add 100 years to `Day`, `MonthYear`, `Year`
  and `FractionDate`. Applies to `gdelt_event` and `gdelt_event_15min`.
- **`keep_original: true`** (default): GDELT's own values stay in
  `Day_original`, `MonthYear_original`, `Year_original` and
  `FractionDate_original`, filled on repaired rows and null elsewhere. Every
  file of the dataset gets the four columns, null throughout outside the
  window, so a cleaned directory has one schema: polars refuses to read
  files that disagree on columns as one dataset, and pandas and pyarrow
  silently drop columns missing from some files. All-null columns cost next
  to nothing in Parquet. `false` repairs in place and is lossy. With
  `clean.output_columns` set, list the four `*_original` columns there too:
  left out, they are projected away, the run warns once, and the repair
  counts as lossy (so `--delete-source` refuses it).
- **Why repair**: dropping the rows would leave a five-day hole, and
  setting the dates to null would hide the rows from calendar sampling. The
  repair is exact, and `DATEADDED` confirms every repaired row.
- **When it can't run**: the rule reads `Day`, `MonthYear`, `Year`,
  `FractionDate` and `DATEADDED`. A converted file missing one of them
  (pruned by `converter.output_columns`) is cleaned without the repair and
  without the `*_original` columns, so its dates are as GDELT wrote them.
  The run warns once, with the number of such files; the audit counts them
  as `errata.date_1920_skipped_files`, and each such file's marker lists the
  step with `"skipped": true` and the columns it lacks.
- **Without it**: calendar sampling treats 1920-01-01 to 01-06 as six real
  days, each drawing its full quota, while the real 2020-01-01 to 01-05
  come out almost empty. `sample --mode calendar` warns whenever it meets
  dates before 1979 (for example with `--source converted`), naming the
  count and this repair; the rows are still sampled.

### `event_markers`: rows that aren't CAMEO events

`EventCode`/`EventBaseCode` `---` with `EventRootCode` `--` (325 rows in
the 1979 to 2026 archive) is the CAMEO null code: the event coder matched a
verb pattern marked as generating no event, yet the row reached GDELT's
output. `X` in all three columns (9 rows) is undocumented; all 9 are
QuadClass 4 with no Goldstein score.

- **`keep`** (default): the rows stay and the run audit counts them
  (`errata.event_markers_keep`). Not lossy.
- **`drop`**: the rows are removed (`errata.event_markers_drop`). Lossy.
- **Why keep by default**: they are what GDELT published, and removing
  them is an analysis choice. For most event-code analyses, `drop` is the
  sensible setting.

### Settings

```yaml
clean:
  errata:
    gdelt_event:
      date_1920: true        # repair the 1920 dates
      keep_original: true    # keep GDELT's values in *_original
      event_markers: keep    # keep | drop
  allow_lossy_delete_source: false
```

An unknown key or an invalid value fails the run before anything is read.
The settings, and a version number bumped whenever a rule changes, are part
of each file's resumability fingerprint: changing them, or upgrading to a
release with a changed rule, cleans the affected datasets again.

## Every cleaned file is marked

Each cleaned file's Parquet metadata holds a `gdeltforge:clean` entry: the
gdeltforge version, every step with its settings and whether it is lossy,
the configuration fingerprint, and the source file's name. The two errata
steps name their `rule` (`date_1920`, `event_markers`), and a step the
configuration leaves empty (a `columns_to_check` of `[]`) isn't listed. A
step that couldn't run on the file, because the file lacks a column it
reads, carries `"skipped": true` and `"missing_columns"`, and counts as not
lossy there, since it changed nothing.
Read it with:

```python
import json, polars as pl
json.loads(pl.read_parquet_metadata("20200101.export_cleaned.parquet")["gdeltforge:clean"])
```

Three places check for it, reading the first, middle and last file's
metadata (a full scan of an archive's footers would cost minutes for a
warning):

- `clean` warns when its own input directory holds cleaned files, since
  cleaning twice compounds every lossy step;
- `sample --source converted`, `aggregate --source converted` and
  `crossref --source converted` warn when the directory they treat as
  GDELT's own data holds cleaned files.

## The run audit

Each run writes `<runs directory>/<UTC start time>.parquet`, one row per
cleaned file. The runs directory is `paths.clean_runs_directory` for
Events and the prefixed key for every other dataset
(`gkg_v2_clean_runs_directory`, `mentions_clean_runs_directory`, ...), the
same rule every path key follows; unset, it sits next to the cleaned
directory, with `_runs` added to its name
(`data/events/cleaned` gets `data/events/cleaned_runs`):

| Column | Meaning |
|---|---|
| `source`, `output` | The converted file and the cleaned file |
| `rows_in`, `rows_out` | Rows before and after the steps |
| `<step>.<count>` | What each step did: `errata.date_1920` (rows repaired), `errata.date_1920_skipped_files` (files the repair couldn't read), `errata.event_markers_keep`/`_drop` (marker rows kept or removed), `normalize.trimmed`/`normalize.blank_to_null` (values changed), `require.rows_dropped`, `derive.event_date_invalid` |
| `unrecognized.<column>` | Non-null values of a CAMEO-coded column missing from the bundled code tables ([`gdeltforge codes`](cli-reference.md#gdeltforge-codes)), counted on the output. One column per coded column the output has, zero included |

The file's own metadata (`gdeltforge:clean-run`) holds the run's settings,
start and end times, and the files that failed. The end-of-run summary
prints every non-zero count. One audit per run, never one per data
file: an archive has hundreds of thousands of files, and per-file sidecars
would recreate the many-small-files problem.

**Why outside the cleaned directory**: polars reads every subdirectory of a
directory it's given, `_`- and `.`-prefixed ones included, so an audit
inside the cleaned directory would be read as rows by
`pl.read_parquet("data/events/cleaned")`. The stage refuses a runs
directory that is, or sits inside, any Parquet directory in `paths`: its
own inputs and outputs, and every other dataset's too.

## Normalize: whitespace

Optional, per dataset, under `clean.normalize.<dataset>`, off by default:

- **`trim_strings`**: strips leading and trailing whitespace from every
  string column (`" USA"` becomes `"USA"`).
- **`blank_to_null`**: turns whitespace-only values (`" "`) into null.
  Use it with `trim_strings`; trimming alone turns `" "` into an empty
  string.

**Evidence**, from all 869 million rows of the 1979 to 2026 Events
archive, almost entirely in files from 2013:

| Column | Padded values | Whitespace-only values |
|---|---|---|
| `Actor2Code` | 11,304,577 | 3,152,942 |
| `Actor1Name` | 479,498 | 0 |
| `Actor2Name` | 383,705 | 0 |
| `*Geo_FullName` (all three) | 23,897 | 0 |

A padded code such as `" USA"` isn't equal to `"USA"`, so a
`sample --filter` on `"USA"` misses it. `Actor2Code` holds composite actor
codes, not one of the CAMEO-coded columns the run audit checks against the
code tables, so the audit doesn't count its padding; `normalize.trimmed`
does, once normalization is on.

**Why off by default**: it changes GDELT's values without keeping the
originals, and the defaults never lose a value. For analysis on codes or
actor names, turning both switches on is recommended:

```yaml
clean:
  normalize:
    gdelt_event:
      trim_strings: true
      blank_to_null: true
```

The run audit counts `normalize.trimmed` and `normalize.blank_to_null`.
Both switches are lossy, so `--delete-source` refuses them unless
`allow_lossy_delete_source: true`.

## Derive: added columns

Optional, per dataset, under `clean.derive.<dataset>`, off by default.
Derived columns go after the file's own columns, in the order listed
below; nothing is replaced, so the step is never lossy.

- **`event_date: true`** adds `EventDate`, a real date parsed from `Day`
  (`YYYYMMDD`), after errata, so the 1920 repair is in it. The run audit
  counts `derive.event_date_invalid`: `Day` values present but not a real
  date.
- **`labels: [<column>, ...]`** adds `<column>_Label` for each listed
  CAMEO-coded column: the code's name from the bundled code tables (the
  ones [`gdeltforge codes`](cli-reference.md#gdeltforge-codes) shows),
  matched regardless of case. A code the tables don't know gets null, and
  the run audit's `unrecognized.<column>` counts those. Listing a column
  that isn't CAMEO-coded fails the run up front.

```yaml
clean:
  derive:
    gdelt_event:
      event_date: true
      labels: [EventRootCode, ActionGeo_CountryCode]
```

With `output_columns` set, list the derived columns there too
(`EventDate`, `EventRootCode_Label`); projection keeps only what's listed.

Every sampling mode returns the derived and `*_original` columns with the
rest. `sample --mode filtered` checks names against the dataset's declared
columns, and it accepts these as well, in `--columns`, `--filter` and
`--stratify`.

**Why off by default**: they make every cleaned file wider, and which
labels are worth storing depends on the analysis. Labels are also tied to
the code tables of the gdeltforge version that wrote them; the code column
itself stays authoritative.

## Not included: categorical storage

Storing the CAMEO-coded columns as polars `Categorical` was proposed as a
size reduction and measured on four real Events files (22 coded columns,
zstd), before building it:

| File | Rows | String | Categorical | Full read, String | Full read, Categorical |
|---|---|---|---|---|---|
| 2016-03-15 daily | 239,103 | 12.04 MB | 12.04 MB | 31 ms | 36 ms |
| 2020-01-02 daily | 123,425 | 6.09 MB | 6.09 MB | 28 ms | 36 ms |
| 2024-06-10 daily | 126,477 | 6.16 MB | 6.16 MB | 30 ms | 39 ms |
| 2008-06 monthly | 1,209,784 | 43.87 MB | 43.87 MB | 93 ms | 117 ms |

The files are the same size to the byte: Parquet already stores repeated
strings as a dictionary, which is all a categorical adds on disk. Reading
is 15 to 30% slower, since polars rebuilds the category mapping. If
categoricals help an analysis in memory, cast after reading
(`pl.col(...).cast(pl.Categorical)`); the files don't need to change.

## Measuring before cleaning

`gdeltforge clean --dry-run --report` runs every step over every file in
scope without writing anything, then logs the totals: rows in and out, each
step's counts, unrecognized codes, columns in and out, and which configured
steps are lossy. It reads the data, so it takes about as long as a real run
minus the writing. A plain `--dry-run` only counts files.

## Data cleaned before 0.12.0

Data cleaned by the stage when it was called `filter` doesn't have the
errata repairs. Nothing needs doing by hand:

- **Datasets with errata rules** (`events`, `events-15min`): the errata
  settings are part of the resumability fingerprint, so the first `clean`
  run after upgrading cleans every file again, and the cleaned directory
  ends up with one schema. To repair the affected days first, run
  `gdeltforge clean --dataset events --start-date 2019-12-31 --end-date
  2020-01-05`, then the full run. In between, the directory holds 62-column
  `_cleaned` files beside 58-column `_filtered` ones: gdeltforge's own
  `sample`, `aggregate` and `crossref` read it, but polars refuses it as one
  dataset and pandas and pyarrow drop the `*_original` columns. The
  date-limited run warns about it, naming the directory.
- **Other datasets**: their fingerprint is unchanged, so their files aren't
  cleaned again. Existing `<stem>_filtered.parquet` files stay valid and
  are read as before.
- **The bundled default's directories moved** from `data/<dataset>/filtered`
  to `data/<dataset>/cleaned`. A config that leaves these paths to the
  default, next to output from an earlier version, would read an empty
  directory, since files already cleaned aren't written again. Loading the
  config warns for as long as the old directory holds Parquet files: move
  them into the new directory (or rename the old one if the new one doesn't
  exist yet), or set `paths.cleaned_data_directory` (or the dataset's own
  key) to the old one.
- `gdeltforge clean --force` re-cleans files in scope regardless of their
  fingerprint.

## Going back to GDELT's own values

- **Read the converted data directly**: `sample`, `aggregate` and `crossref`
  all accept `--source converted`.
- **Re-clean after changing settings**: files already cleaned under the
  current settings are skipped. Changing a setting that affects the output
  makes the next run reprocess every file; `gdeltforge clean --force`
  reprocesses regardless. A run limited by `--start-date`/`--end-date`
  reprocesses only its own days, so a change that adds or removes columns
  leaves the directory with two schemas until a run covers the rest.
- **Read a repaired value's original**: with `keep_original: true`, the
  `*_original` columns hold GDELT's values on repaired rows.
- **Keep the converted copy** unless disk space forces otherwise:
  `--delete-source` removes it once the cleaned file is written, and after
  that the only way back is re-converting from GDELT's raw ZIPs. It refuses
  to run with a lossy step added in 0.12.0 (errata or normalize) unless you
  set `allow_lossy_delete_source: true`.
