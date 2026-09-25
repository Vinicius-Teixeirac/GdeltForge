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
| **Steps run in one fixed order** | No configuration can produce an order-dependent result by accident |
| **Every step declares whether it's lossy** | A lossy step leaves the cleaned output unable to tell what the converted input held, which matters for deciding whether the converted copy can go |
| **Output never goes into an input directory** | Cleaned files beside the converted ones would be cleaned again on the next run, and every reader of the converted directory would count those rows twice. `clean` refuses to start in that configuration |
| **Named `clean`**, `filter` before 0.12.0 | `filter` collided with `sample --filter`, which does the relevance job above. The old names keep working through 0.12.x with a deprecation warning |

## Steps, in order

Each file passes through the configured steps in this order:

| Order | Step | Setting | What it does | Lossy |
|---|---|---|---|---|
| 1 | require | `columns_to_check` | Drops rows with a null in any listed column | Yes: the rows |
| 2 | project | `output_columns` | Keeps only the listed columns | Yes: the other columns |
| 3 | narrow | `float32_columns` | Stores the listed float columns as float32 | Yes: GDELT floats carry up to 15 significant figures, float32 about 7 |

Then the file is written with the configured `compression` (zstd by default,
lossless) as `<stem>_cleaned.parquet`, through a temporary file and a rename,
so an interrupted run never leaves a half-written file.

Every step is off unless configured: the bundled default configuration
cleans nothing away, so a first run's output equals its input.

## Going back to GDELT's own values

- **Read the converted data directly**: `sample`, `aggregate` and `crossref`
  all accept `--source converted`.
- **Re-clean after changing settings**: files already cleaned under the
  current settings are skipped. Changing a setting that affects the output
  makes the next run reprocess every file; `gdeltforge clean --force`
  reprocesses regardless.
- **Keep the converted copy** unless disk space forces otherwise:
  `--delete-source` removes it once the cleaned file is written, and after
  that the only way back is re-converting from GDELT's raw ZIPs.
