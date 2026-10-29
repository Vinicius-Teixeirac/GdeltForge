# Known Absent Files

`scrape`/`convert` failing on a specific GDELT file usually means exactly
what it looks like: a bug, a network blip, a disk problem. But a small,
genuinely real set of files simply don't exist on GDELT's own servers, or
exist as an intentional empty placeholder. This page is the record of
which ones, so a failure on one of them can be told apart from a real
problem without re-investigating from scratch every time.

## Scope

Audited: every file `events`, `gkg_v2`, and `mentions` could contain across
their full historical range, from each dataset's own start (events: yearly
from 1979, monthly from 2006, daily from April 2013; gkg_v2/mentions:
15-minute files from February 2015) **through 2025-12-31**. Nothing after
that date is covered here; a failure on a more recent file isn't ruled out
by this page either way.

For each dataset, GDELT's own remote master file list was diffed against
what a full archive scrape actually produced, and every resulting gap was
individually re-fetched live and classified by what the server actually
returned, not inferred from a log message:

- **`404`** — GDELT's server returns Not Found for that exact file.
- **`empty (0 bytes)`** — the file downloads, the zip is valid, but the CSV
  member inside it is confirmed 0 bytes by directly inspecting the zip's
  contents.

Both classifications are re-checkable at any time: `curl -sI <url>` for a
404, or download-and-inspect-the-zip-member for an empty file. Spot-checks
of files in both categories, redone independently, have confirmed the
classification exactly, including the precise minute-level boundary of
each gap window.

Starting from the lists leaves out what GDELT never listed: whole days
of the daily archive and thousands of 15-minute updates. Those are in
[Never listed by GDELT](#never-listed-by-gdelt), below.

## Summary

| Dataset | In-scope total | Present | Absent | — 404 (never published) | — empty (published, 0 bytes) |
|---|---:|---:|---:|---:|---:|
| events | 4,750 | 4,748 | 2 | 2 | 0 |
| gkg_v2 | 373,743 | 373,605 | 138 | 127 | 11 |
| mentions | 373,738 | 373,558 | 180 | 94 | 86 |
| **Total** | **752,231** | **751,911** | **320** | **223** | **97** |

*(events' 4,750 in-scope total spans three formats: 27 yearly 1979–2005, 87
monthly 2006–early 2013, 4,636 daily since; the yearly and monthly tiers
are both 100% complete, every absence is in the daily tier.)*

## This isn't random, it's real outages

The clearest evidence that the 223 "never published" files are genuine
provider gaps, not an artifact of any one dataset's own scraping/parsing
code, is that **the same windows are missing across independent datasets
scraped, parsed, and converted by entirely separate code paths**:

| Window | events | gkg_v2 | mentions |
|---|---|---|---|
| **2022-11-10 22:00 → 2022-11-11 18:30** | 1 file missing (2022-11-10) | 83 files missing | 83 files missing |
| **2023-03-23 13:00 → 14:30** | 1 file missing (2023-03-23) | 7 files missing | 7 files missing |
| **2015-02-19 07:45 → 08:00** | — | 2 files missing | 2 files missing |
| **2020-12-14 12:15** | — | 1 file missing | 1 file missing |

If this were an artifact of scraping or conversion here, there'd be no
reason for a daily-cadence dataset (events) and two independent 15-minute-
cadence datasets (gkg_v2, mentions) to all go dark on the exact same
minutes. The likeliest explanation is that GDELT's own pipeline had real
outages around **2022-11-10/11** (~20.5 hours) and **2023-03-23**
(~1.5 hours of listed files, part of a ~34-hour outage whose earlier
updates GDELT never listed; see [Never listed by GDELT](#never-listed-by-gdelt)),
plus others too brief or dataset-specific to leave this same
cross-dataset signature.

The largest single-dataset cluster is **2015-05-29 00:00–06:45 in gkg_v2
only** (28 files, absent from gkg_v2 but not from mentions): GKG 2.1 was
less than four months old at that point, consistent with early-feed
immaturity rather than a shared outage.

`mentions`' 86 "empty" files are almost entirely isolated singles
scattered through 2015–2017, each a distinct 15-minute interval where
GDELT evidently had zero new mentions to report, plausible for a feed's
early, lower-volume years.

## Never listed by GDELT

The audit above diffs GDELT's lists against what was downloaded, so a file
missing from the lists themselves never enters it. Laid against the
calendar, from each dataset's start through 2025-12-31, the lists lack
these days and 15-minute updates:

| Dataset | Never listed |
|---|---:|
| events (daily) | 22 days |
| events-15min | 7,290 updates |
| mentions | 7,290 updates |
| gkg_v2 | 7,285 updates |

GDELT doesn't serve them either: all 22 days and 300 updates per 15-minute
dataset, drawn at random, answer 404. `scrape` downloads what the lists
hold, so it neither fetches these nor reports them as failures; a day
without a file reads as a day without events. The 15-minute counts
reconcile with the summary above: the 381,024 updates from 2015-02-19
through 2025-12-31, plus the 4 listed on 2015-02-18, less the unlisted
ones, are exactly the in-scope totals for `gkg_v2` (373,743) and
`mentions` (373,738).

events-15min and mentions lack the same 7,290 updates. gkg_v2 lacks 5
fewer: it holds 2016-04-25 15:30, 2017-05-10 07:45, 2017-07-21 09:00 and
10:45, and 2017-08-20 21:00, which the other two don't.

**The daily archive** has no file for:

| Days | Count |
|---|---:|
| 2014-01-23 to 2014-01-25 | 3 |
| 2014-03-19 | 1 |
| 2025-06-14 to 2025-07-01 | 18 |

**The 15-minute datasets**, by year (mentions; gkg_v2 differs only by the
five updates above):

| Year | Updates never listed |
|---|---:|
| 2015 | 200 |
| 2016 | 45 |
| 2017 | 994 |
| 2018 | 992 |
| 2019 | 11 |
| 2020 | 2,585 |
| 2021 | 610 |
| 2022 | 5 |
| 2023 | 165 |
| 2024 | 2 |
| 2025 | 1,681 |

Most of the 2,247 gaps are single updates (1,724). The rest fall into a
few patterns:

- **Three long outages**: 2015-10-21 04:15 to 2015-10-22 21:45 (167
  updates); 2023-03-22 04:45 to 2023-03-23 12:45 (129); and 2025-06-14
  18:00 to 2025-07-02 02:00 (1,665, about 17 days), the same outage as the
  daily archive's 18 missing days. The 2023 one runs straight into the
  listed-but-404 window of 2023-03-23 13:00 to 14:30 above, so that outage
  lasted about 34 hours.
- **Nightly gaps for weeks**: 2020-09-24 to 2020-11-18 lose 2,579 updates
  in 309 gaps, 69% of them between 21:00 and 08:00 UTC; 2021-07-30 to
  2021-08-19 lose 598 in 109 gaps, 94% at those hours.
- **2017 and 2018**: about 990 updates a year, in runs of one to four,
  scattered across most months (one run of 19 on 2018-07-15).

A window that crosses one of these gaps holds fewer events than its
length suggests. For counts per day or per update, check the window
against this list before reading a dip as real.

## Full itemized list

??? note "events — not hosted by GDELT (2)"
    | Timestamp | File |
    |---|---|
    | 2022-11-10 | `20221110.export.CSV.zip` |
    | 2023-03-23 | `20230323.export.CSV.zip` |

??? note "gkg_v2 — not hosted by GDELT (127, in 9 clusters)"
    | Window | Files |
    |---|---:|
    | 2015-02-19 07:45 – 08:00 | 2 |
    | 2015-03-18 20:45 – 21:00 | 2 |
    | 2015-05-12 16:45 | 1 |
    | 2015-05-29 00:00 – 06:45 | 28 |
    | 2016-05-25 22:00 – 22:15 | 2 |
    | 2017-07-07 00:00 | 1 |
    | 2020-12-14 12:15 | 1 |
    | 2022-11-10 22:00 – 2022-11-11 18:30 | 83 |
    | 2023-03-23 13:00 – 14:30 | 7 |

??? note "gkg_v2 — hosted but 0 bytes (11, in 6 clusters)"
    | Window | Files |
    |---|---:|
    | 2015-02-19 09:45 | 1 |
    | 2016-05-08 14:15 | 1 |
    | 2016-05-16 14:00 – 14:15 | 2 |
    | 2016-05-16 20:30 – 21:30 | 5 |
    | 2017-05-10 07:45 | 1 |
    | 2017-08-04 21:30 | 1 |

??? note "mentions — not hosted by GDELT (94, in 5 clusters)"
    | Window | Files |
    |---|---:|
    | 2015-02-19 07:45 – 08:00 | 2 |
    | 2015-04-30 00:00 | 1 |
    | 2020-12-14 12:15 | 1 |
    | 2022-11-10 22:00 – 2022-11-11 18:30 | 83 |
    | 2023-03-23 13:00 – 14:30 | 7 |

??? note "mentions — hosted but 0 bytes (86)"
    Almost entirely isolated single 15-minute intervals; two short
    back-to-back pairs noted.

    | Window | Files |
    |---|---:|
    | 2015-03-08 06:15 | 1 |
    | 2015-12-03 02:15 | 1 |
    | 2016-05-16 14:00 – 14:15 | 2 |
    | 2016-05-16 20:30 – 21:30 | 5 |
    | 2016-05-31 23:30 | 1 |
    | 2016-06-11 01:45 | 1 |
    | 2016-07-18 19:00 | 1 |
    | 2016-08-17 21:30 | 1 |
    | 2016-12-22 21:00 | 1 |
    | 2017-03-20 21:00 | 1 |
    | 2017-03-25 22:15 | 1 |
    | 2017-03-26 05:00 | 1 |
    | 2017-03-26 05:45 | 1 |
    | 2017-04-06 03:15 | 1 |
    | 2017-04-08 04:00 | 1 |
    | 2017-04-08 23:00 | 1 |
    | 2017-04-10 05:45 | 1 |
    | 2017-04-20 19:30 | 1 |
    | 2017-04-23 20:30 | 1 |
    | 2017-04-27 19:30 | 1 |
    | 2017-05-04 19:15 | 1 |
    | 2017-05-04 19:45 | 1 |
    | 2017-05-10 22:00 | 1 |
    | 2017-05-11 22:45 | 1 |
    | 2017-05-11 23:15 | 1 |
    | 2017-05-13 20:30 | 1 |
    | 2017-05-17 03:30 | 1 |
    | 2017-05-23 21:45 | 1 |
    | 2017-05-26 19:15 | 1 |
    | 2017-06-01 21:15 | 1 |
    | 2017-06-10 17:00 | 1 |
    | 2017-06-10 19:00 | 1 |
    | 2017-06-18 23:30 | 1 |
    | 2017-06-20 23:30 | 1 |
    | 2017-06-23 21:45 | 1 |
    | 2017-06-25 10:45 | 1 |
    | 2017-06-25 21:15 | 1 |
    | 2017-06-28 17:30 | 1 |
    | 2017-06-28 20:45 | 1 |
    | 2017-06-28 22:15 | 1 |
    | 2017-07-01 10:45 | 1 |
    | 2017-07-01 15:15 | 1 |
    | 2017-07-02 22:00 | 1 |
    | 2017-07-03 21:00 | 1 |
    | 2017-07-07 21:00 | 1 |
    | 2017-07-10 16:30 | 1 |
    | 2017-07-10 21:15 | 1 |
    | 2017-07-11 21:00 | 1 |
    | 2017-07-15 14:30 | 1 |
    | 2017-07-16 22:30 | 1 |
    | 2017-07-20 16:00 | 1 |
    | 2017-07-25 21:45 | 1 |
    | 2017-07-29 19:15 | 1 |
    | 2017-07-31 22:00 | 1 |
    | 2017-08-01 10:45 | 1 |
    | 2017-08-01 17:15 | 1 |
    | 2017-08-05 16:45 | 1 |
    | 2017-08-08 18:15 | 1 |
    | 2017-08-09 15:45 | 1 |
    | 2017-08-11 19:45 | 1 |
    | 2017-08-12 22:00 | 1 |
    | 2017-08-24 22:30 | 1 |
    | 2017-08-29 20:45 | 1 |
    | 2017-08-31 06:00 | 1 |
    | 2017-09-03 04:45 | 1 |
    | 2017-09-03 07:30 | 1 |
    | 2017-09-03 08:45 | 1 |
    | 2017-09-04 21:45 | 1 |
    | 2017-09-12 22:00 | 1 |
    | 2017-09-16 19:00 | 1 |
    | 2017-09-19 20:45 | 1 |
    | 2017-09-20 19:45 | 1 |
    | 2017-09-25 04:00 | 1 |
    | 2017-09-27 16:00 | 1 |
    | 2017-09-27 18:30 | 1 |
    | 2017-10-04 03:45 | 1 |
    | 2017-10-12 20:30 | 1 |
    | 2017-10-18 20:00 | 1 |
    | 2017-11-08 20:45 | 1 |
    | 2017-11-10 22:00 | 1 |
    | 2023-03-21 23:00 | 1 |

## Two lessons from this audit worth keeping

Not every early gap-detection pass gets this right the first time:

- **A `.tmp` leftover can read as "already converted."** An atomic write
  (write-to-`.tmp`-then-rename) interrupted mid-run (a shared server's own
  reboot, mid-write) can leave a stale `<name>.parquet.tmp` sitting where
  the finished file should be. Because the `.tmp` filename still starts
  with a valid timestamp, a naive presence check can mistake it for a
  completed file and skip it, exactly the opposite of what should happen.
  Check for `.tmp` leftovers explicitly, don't rely on filename presence
  alone.
- **An "unclear" wrapper error isn't a classification.** A caught
  exception that only says a file "failed" without saying why should be
  resolved to an actual 404 or an actual empty-zip check, not counted as
  data loss (or as fine) on the strength of a generic error message.

## Using this page

If `scrape` or `convert` reports a failure on a specific timestamp, check
this page (or re-run the classification above directly against that exact
file) before assuming the pipeline itself is at fault. A file listed here
is a confirmed GDELT-side gap, not something `--force`, a retry, or a
different `gdeltforge` version will fix.
