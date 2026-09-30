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
| **Place resolution is available but off by default, from a table built once** | A geo point's `FeatureID` and coordinates often disagree: one place under three codes, two places under one, a country written in the wrong ocean. Telling them apart takes evidence from the whole archive, which a row-level step can't gather, so the decisions are made once, over the full archive, and shipped as a table; the step only looks up each point. It replaces GDELT's values without keeping them, which the defaults never do |
| **Derived columns only add, and are off by default** | Adding a column never discards anything, so the step is never lossy; storing them costs width in every file, and which ones are useful depends on the analysis |
| **No option to store code columns as categoricals** | Measured on real Events files, it gains nothing and costs read time (see [below](#not-included-categorical-storage)) |
| **Normalization is available but off by default** | Whitespace padding is common in GDELT's 2013 files and breaks code lookups, but fixing it rewrites GDELT's values without keeping them, which the defaults never do |
| **`--delete-source` refuses a lossy step added in 0.12.0** (errata, normalize, places) unless `allow_lossy_delete_source: true` | Deleting the converted copy removes the only way back to what a lossy step discarded, so that combination has to be chosen deliberately. The original three steps (`columns_to_check`, `output_columns`, `float32_columns`) keep their long-standing warning, since existing setups rely on them to fit disk |
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
| 3 | places | `places.<dataset>` | Resolves each geo point to one place ([below](#places-one-place-per-geo-point)); off by default | Yes: GDELT's geo values |
| 4 | require | `columns_to_check` | Drops rows with a null in any listed column | Yes: the rows |
| 5 | derive | `derive.<dataset>` | Adds a real event date and code labels ([below](#derive-added-columns)); off by default | No: only adds columns |
| 6 | project | `output_columns` | Keeps only the listed columns | Yes: the other columns |
| 7 | narrow | `float32_columns` | Stores the listed float columns as float32 | Yes: GDELT floats carry up to 15 significant figures, float32 about 7 |

Errata come first so every later step sees corrected values, and
normalization and place resolution come before the null check so a blank
value, or a point without a location, counts as missing there. Derived columns come after both, so they're built from
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
  (pruned by `converter.output_columns`) is cleaned without the repair, and
  the run warns once, with the number of such files; the audit counts them
  as `errata.date_1920_skipped_files`.
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
configuration leaves empty (a `columns_to_check` of `[]`) isn't listed.
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
| `<step>.<count>` | What each step did: `errata.date_1920` (rows repaired), `errata.date_1920_skipped_files` (files the repair couldn't read), `errata.event_markers_keep`/`_drop` (marker rows kept or removed), `normalize.trimmed`/`normalize.blank_to_null` (values changed), `places.resolved`/`places.changed`/`places.cleared`/`places.unresolved` (geo points), `places_skipped_files`, `require.rows_dropped`, `derive.event_date_invalid` |
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

## Places: one place per geo point

Optional, per dataset, under `clean.places.<dataset>`, off by default, for
`gdelt_event` and `gdelt_event_15min`.

Every event has three geo points: where Actor1 is (`Actor1Geo_*`), where
Actor2 is (`Actor2Geo_*`), and where the action happened (`ActionGeo_*`).
Each names its place twice. `FeatureID` names it in a gazetteer: a GNIS ID
for a US place, a GNS ID for the rest of the world, a two-letter FIPS code
for a country or a US state, or `0` when GDELT only knew the country or
state. `Lat` and `Long` give the point on the map. The two should agree,
and often don't. Real combinations from the 1979 to 2026 archive, with the
number of geo values carrying each:

| GDELT wrote (FeatureID, Lat, Long) | What it is | Values |
|---|---|---|
| `US`, 39.828175, -98.5795 | The United States | 18,814,549 |
| `US`, 38.0, -97.0 | The United States again, at GDELT 1.0's round-number centre | 13,790,530 |
| `0`, 38.0, -97.0 | The United States again, as "only knew the country" | 14,219,713 |
| `449676`, 39.1662, -86.5264 | Indiana University, in GNIS | 108,168 |
| `449676`, 34.6767, 69.0073 | Kula, Afghanistan: the same number in GNS | 41 |
| `MP`, 16.0, 146.0 | Mauritius (FIPS `MP`), written at the Northern Mariana Islands (ISO `MP`) | 241,561 |
| `MP`, -20.2833, 57.55 | Mauritius, at its own point | 130,695 |
| `0` or none, 0.0, 0.0 | No location at all | 87,790,602 (3.37% of all geo values) |
| `GA`, 0.0, 0.0 | Georgia, the US state, its point missing (2003 to 2016) | 1,422,389 |

So one place gets three codes (the United States), two places get one
code (Indiana University and Kula), a country sits in the wrong ocean two
times out of three, and a point with no location passes a null check,
since `0.0` isn't null.

### What the step does

A table built once from the full archive assigns every (FeatureID, Lat,
Long) combination GDELT wrote to one place. It ships with gdeltforge
(`gdeltforge/data/places.parquet`, 22 MB), so the step needs nothing but
the point itself. Every geo point comes out either as a place, under one
ID no other place shares, or as no location, the way GDELT writes a point
it couldn't locate (`FeatureID`, `Lat`, `Long` and `FullName` null, `Type`
0):

- **A point with a location** is looked up by its exact `FeatureID`, `Lat`
  and `Long`, at full float64 precision. Found, it gets the place's ID in
  `FeatureID`, its canonical point in `Lat`/`Long`, its canonical name in
  `FullName` and its type in `Type`.
- **A point without a location**, with no coordinates or at (0, 0), which
  is the same thing, is looked up by its `FeatureID` alone. It gets the
  place when the archive shows that ID, written without a location,
  always names one place ([rule 8](#how-the-table-decides)), and is no
  location otherwise. The null check runs after this step, so a
  `columns_to_check` that requires a geo column drops those events.
- **A point with a location the table doesn't know**, a combination GDELT
  first wrote after the table was built, gets the place its `FeatureID`
  names in the gazetteer its `Type` says, when the table has one: a known
  place written at a new point. Otherwise it's a place the table doesn't
  have, and its point, name and type stay as GDELT wrote them. Its
  `FeatureID` follows rule 2: an ID the table gives another gazetteer's
  place takes its own form (`gns:<id>` or `US<code>`, or `gnis:<id>` for a
  GNIS ID the table gives a GNS place), and `0`, or an untyped ID more than
  one place uses, becomes null. In the archive itself, 6,818 combinations
  (12,707 values) resolve under no rule below, nearly all of them named
  points GDELT wrote without an ID; they keep their point and name.

`Type` is read only for that last case, to tell which gazetteer a new
point's ID is from. `CountryCode`, `ADM1Code` and `ADM2Code` are never
touched, nor is any other column.

For a made-up event built from three of the cases above:

| Point | GDELT wrote | After the step | `FullName` after |
|---|---|---|---|
| `Actor1Geo` | `0`, 38.0, -97.0 | `US`, 39.828175, -98.5795 | United States |
| `Actor2Geo` | `MP`, 16.0, 146.0 | `MP`, -20.2833, 57.55 | Mauritius |
| `ActionGeo` | `449676`, 34.6767, 69.0073 | `gns:449676`, 34.6767, 69.0073 | Kula, Kabol, Afghanistan |

### How the table decides

The table covers 1,193,681 distinct combinations of `FeatureID`, `Type`,
`FullName`, `Lat` and `Long`, 2.61 billion geo values from 1979 to
2026-07-31, and resolves them to 965,477 places. `Type` and `FullName` are
used there, as witnesses; the lookup key is the point alone, and no
(FeatureID, Lat, Long) combination resolves to two places.

1. **A place is a location.** A feature (a FeatureID of a given type) is
   written most often at one point, its main point. It also owns every
   other point where it carries at least half of the values written there.
   Features that own the same point are one place, joined largest first,
   except that two different countries or US states never become one place
   through a shared point.
2. **A place's ID is GDELT's own `FeatureID`, one reading per ID.** A
   numeric ID is a GNIS ID for a US place (Type 3) and a GNS ID otherwise;
   a two-letter code is a FIPS country for Type 1 and a US state for Type
   2. When one ID names a place in each reading, the first reading in a
   fixed order (country, US state, GNIS, GNS) keeps it, and the other takes
   a form of its own. A US state takes its ADM1 code, the one GDELT writes
   in `ADM1Code`: `CA` stays Canada and California becomes `USCA`, so a
   country's ID is still its `CountryCode`. A GNS place takes the `gns:`
   prefix: `449676` stays Indiana University and Kula becomes
   `gns:449676`. This concerns 27 US states and 3,739 GNS places; every
   other ID stays as GDELT wrote it. The order is fixed, not the larger
   reading first, so a rebuild never swaps two IDs: 843 of the 3,739
   numeric pairs are within a factor of two of each other.
3. **`0`, and a point written without an ID,** resolve to the place that
   owns their point: `0` at (38.0, -97.0) is the United States.
4. **GDELT 1.0's untyped features** (Type 0) resolve to the typed feature
   with the same ID at the same point, when exactly one has it, and by
   their point otherwise.
5. **A feature written at a point it doesn't own:** when the name written
   with it is the point owner's name and not the feature's own, the ID is
   the error and the point's place wins; otherwise the point is the error
   and the feature's own place wins.
6. **The canonical point** of a city or landmark is its main point. A
   country's or US state's is, of the points it owns, the one nearest the
   median of its own features (its cities for a country, its GNIS features
   for a state), since the most frequent point is sometimes another
   country's: Mauritius' most frequent point is the Northern Mariana
   Islands, the ISO reading of its FIPS code. When every point it owns is
   more than 5,000 km from its features, GDELT has no right point for it,
   and the median is used.
7. **The canonical name** is the name written most for the place in the
   latest era it was written in (2015 on, or its own last years if it
   ends earlier), leaving out names with `?` or the Unicode replacement
   character, which mark a broken encoding.
8. **A point without a location** names a place when its `FeatureID` does:
   read in the gazetteer its `Type` says (or, untyped, in the only one that
   has the ID), it is a feature with a real point, and every point of that
   ID without a location agrees. `GA` at (0, 0) is always Georgia, the US
   state, and resolves to it (1,422,389 values); `GG` without coordinates
   is Georgia, the country (240,758). `0`, a missing ID, and IDs with no
   place of their own (`RB`, `YI`, and `MH` written as the Marshall
   Islands, while `MH` with a location is Montserrat; 412,271 values) are
   no location.

Of all 2.61 billion geo values, 85.7% resolve to a place; 12.8% get a
different `FeatureID` (mostly `0` becoming its country or state); 3.8% of
all values (4.5% of those resolved) move more than 1 km. A point GDELT got
right stays where it is.

### What it never does

- Invent a place: a point is given a place only when GDELT's own ID or
  point names it.
- Move a point GDELT got right, or discard one the table doesn't know.
- Read `QuadClass`, or any column but the geo points.
- Change the converted or the raw files.

### Settings

```yaml
clean:
  places:
    gdelt_event:
      resolve: true
```

The run audit counts, per geo point: `places.resolved` (found in the
table), `places.changed` (of those, the ones whose ID or point changed),
`places.cleared` (turned into no location: at (0, 0), or an ID written
without coordinates that names no place) and `places.unresolved` (with a
location, but no place in the table: kept as written). A file with no geo point left to
resolve, pruned by `converter.output_columns`, counts under
`places_skipped_files`, and the run warns with the number of such files.

**Why off by default**: it replaces GDELT's values without keeping them,
so it's lossy, and `--delete-source` refuses it unless
`allow_lossy_delete_source: true`. It also costs each worker about 80 MB
of memory for the table. The table's identity (its rule version and the
archive it was built from) is part of the resumability fingerprint, so a
gdeltforge release with a rebuilt table resolves every file again.

The table is rebuilt from a converted Events archive with
`tools/build_place_table.py` in the repository, a maintainer tool.

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
  run after upgrading cleans every file again. To repair the affected days
  first, run `gdeltforge clean --dataset events --start-date 2019-12-31
  --end-date 2020-01-05`; the rest follows on the next full run.
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
  reprocesses regardless.
- **Read a repaired value's original**: with `keep_original: true`, the
  `*_original` columns hold GDELT's values on repaired rows.
- **Keep the converted copy** unless disk space forces otherwise:
  `--delete-source` removes it once the cleaned file is written, and after
  that the only way back is re-converting from GDELT's raw ZIPs. It refuses
  to run with a lossy step added in 0.12.0 (errata, normalize or places) unless you
  set `allow_lossy_delete_source: true`.
