# Storage concurrency benchmark

This page records the measurements behind [`io.max_concurrent_reads`](configuration.md#io), the per-worker polars thread split, and `aggregate`'s one-file-at-a-time reads (see [Worker pools and polars threads](configuration.md#worker-pools-and-polars-threads)). It was prompted by a real incident: a full-archive run on a shared lab server slowed its NFS storage to about 4 MB/s for every user, including the run itself. The storage's administrator traced it to gdeltforge starting many processes that kept dozens of reads and writes in flight at once.

Everything below was measured on gdeltforge 0.11.0, before any of the changes this page motivated. The "Mapping to current behavior" section at the end says which of the measured configurations the current defaults and settings produce.

## Setup

| | |
|---|---|
| Client | Linux server, 32 cores, Python 3.12, polars 1.44.1 |
| Network | 1 GbE (about 117 MB/s ceiling) |
| Storage | NFS 4.2 over TCP, `rsize`/`wsize` 1 MiB, HDD-backed, shared with other users |
| Idle read latency | 11.5-14.7 ms per cold 64 KiB read, 27 ms per cold 1 MiB read |
| Data | real GKG 2.1: 15-minute parquet files (2.5-5 MB each), daily aggregates (550-900 MB), fresh GKG 2.1 zips |

Method:

- **Cold inputs only.** Every run read files no earlier run had touched, and a `mincore` check refused any run whose inputs were more than 5% cached (actual: 0-1.5%). Comparable runs got date windows of similar size; runs are compared by MB/s since window sizes still differed.
- **Shuffled order**, fixed seed, so slow drift in the storage's background load couldn't line up with one setting.
- **Latency from the kernel's own counters**: `nfsiostat 5` alongside every run (first block dropped) plus a 1-second `/proc/self/mountstats` sampler. The per-read round-trip time (RTT) is what every other user of the storage feels.
- **Safety limits**, since the storage was shared: each run capped at 170 s, and killed as soon as the 5-second average RTT went above 100 ms, followed by a pause and a fresh idle-latency probe before continuing. Seven runs were killed this way, after 8-21 s each; they're reported as such below, with throughput estimated from what they read before the kill.
- Outputs went to a scratch directory and were deleted afterwards. Both idle baselines showed no read traffic from other users.

## Raw storage scaling

`dd iflag=direct bs=1M`, N parallel readers, each on its own set of files, about 1.26 GB per run.

| Files | N | MB/s | Read RTT avg (ms) | Status |
|---|---|---|---|---|
| large (daily aggregates) | 1 | 65.2 | 15 | ok |
| large | 2 | 82.7 | 25 | ok |
| large | 4 | **88.5** | 46 | ok |
| large | 8 | 88.4 | 90 | ok |
| large | 16 | 58.5 (est.) | 185 | killed |
| small (15-minute parquet) | 1 | **56.7** | 13 | ok |
| small | 2 | 42.8 | 31 | ok |
| small | 4 | 44.5 | 61 | ok |
| small | 8 | 30.6 (est.) | 122 | killed |
| small | 16 | 33.2 (est.) | 258 | killed |

On large files, throughput stops growing at 2-4 readers, about 75% of the link. On small files, a single reader was already fastest in MB/s, and files/s was flat from 1 to 4 readers (11.9-13.4). In both series the RTT grows roughly in proportion to N: the storage queues extra readers, it doesn't serve them in parallel. gdeltforge's own reads are smaller than `dd`'s 1 MiB (70-110 KB for `aggregate`/`filter`, 200-430 KB for `convert`), so they sit at least as close to the seek-bound, small-file end.

## `aggregate`

`--period day --source converted`, 8 days per run, so at most 8 worker processes. "Reads in flight" is workers x files each worker reads at once. `SCANS` is `POLARS_MAX_CONCURRENT_SCANS`, `THREADS` is `POLARS_MAX_THREADS`; "default" is 0.11.0 with no environment variables. Second figures are replicates on a fresh window.

| max_workers | Environment | Reads in flight | Input MB/s | Read RTT avg / worst 5 s (ms) | Status |
|---|---|---|---|---|---|
| null | default | 8 x 32 | **8.2** (est.) | **121 / 132** | killed |
| 1 | THREADS=32 | 32 | 20.6 (est.) | 68 / 129 | killed |
| 8 | SCANS=4 | 32 | 13.9 (est.) | 72 / 100 | killed |
| 8 | THREADS=4 | up to 32 | 33.6 | 58 / 95 | ok |
| 4 | THREADS=8 | up to 32 | 33.8 | 50 / 98 | ok |
| 2 | THREADS=16 | up to 32 | 30.9 | 39 / 73 | ok |
| 8 | SCANS=2 | 16 | 36.1 | 43 / 66 | ok |
| 4 | SCANS=4 | 16 | 37.4; 28.0 | 35 / 59; 22 / 40 | ok |
| 8 | SCANS=1 | 8 | 29.9; 37.1 | 25 / 65; 16 / 51 | ok |
| 4 | SCANS=2 | 8 | 25.6 | 24 / 64 | ok |
| 2 | SCANS=4 | 8 | 28.2 | 22 / 53 | ok |
| 2 | SCANS=2 | 4 | 33.5; 30.8 | 13.5 / 28; 13.3 / 24 | ok |
| **4** | **SCANS=1** | **4** | **33.2; 26.7 (est.)** | **9.7 / 30; 11.2 / 32** | ok; replicate hit the 170 s cap |
| 1 | SCANS=4 | 4 | 27.4 | 12.6 / 24 | ok |
| 1 | SCANS=2 | 2 | 27.9 | 6.8 / 16 | ok |
| **2** | **SCANS=1** | **2** | **19.0** | **10.0 / 31** | ok |
| 1 | SCANS=1 | 1 | 17.7 (est.) | 4.8 / 19 | hit the 170 s cap |

- **The incident reproduced**: the default read at 8 MB/s while every other user's reads waited 121 ms on average (337 ms in the worst `nfsiostat` interval), eight to ten times the idle figure.
- **Even one worker overloads the storage** with polars' default of one concurrent file scan per core. The worker count alone can't make `aggregate` safe on this storage.
- **Throughput levels off at about 4 reads in flight**, at 27-37 MB/s. Beyond that, only latency grows.
- **Splitting threads is not a read cap.** `POLARS_MAX_THREADS = 32 / max_workers` also lowers polars' concurrent scans, but keeps workers x scans at 32; all three such runs had worst-case RTTs of 73-98 ms.
- `aggregate` used under one CPU core in every configuration, and wrote about 70% of its input volume back to the same storage.

## `convert`

128 distinct zips per run. "CSVs on" is where `unzipped_data_directory` pointed: the NFS share next to the zips, or local `/tmp`. Throughput is measured on zip bytes.

| max_workers | CSVs on | polars threads per worker | Zip MB/s | NFS write MB/s | Read RTT avg / worst 5 s (ms) |
|---|---|---|---|---|---|
| 4 | NFS | 32 | 19.3 | 75 | 13 / 16 |
| 8 | NFS | 32 | 27.0 | 104 | 29 / 36 |
| 16 | NFS | 32 | 27.7 | **108** | 31 / 36 |
| 32 | NFS | 32 | 25.0 | 97 | 27 / 34 |
| 4 | local | 32 | 54.8 | 43 | 23 / 27 |
| 4 | local | 8 | 52.0 | 41 | 21 / 27 |
| **8** | **local** | 32 | **68.1** | 53 | 29 / 34 |
| **8** | **local** | 4 | **71.6** | 56 | **25 / 30** |
| 16 | local | 32 | 66.5 | 52 | 33 / 47 |
| 16 | local | 2 | 64.9 | 50 | 26 / 35 |
| 32 | local | 32 | 56.5 | 43 | 25 / 32 |
| 32 | local | 1 | 63.9 | 50 | 43 / 60 |

- **Extracting CSVs to local disk is 2.5-3x faster at every worker count.** With CSVs on the share, every CSV crosses the network twice (written, then read back), which alone pushed 75-108 MB/s of writes onto a 117 MB/s link.
- **8 workers is the best point** with local extraction; more workers only add queueing.
- **The per-worker thread split** made no consistent throughput difference here (within the ±10% replicate noise), while cutting thread counts by 50-68% (4,359 to 1,383 threads at 32 workers).
- No `convert` setting reaches idle-level RTT, since it streams 40-56 MB/s of Parquet writes back to the share. The gentlest, 4 workers with local extraction, averaged 21-23 ms at 52-55 MB/s.

## `filter`

`filter` is the stage named `clean` since 0.12.0. The rename also gave it steps that can read a few more columns of an Events file; its worker pool is the one measured here.

| max_workers | Input MB/s (2-day; 4-day runs) | Read RTT avg / worst 5 s (ms), 4-day runs | Peak threads |
|---|---|---|---|
| **4** | 82.5; 82.4 | **47 / 59** | 583 |
| 8 | 64.1; 55.1 | 73 / 89 | 1,127 |
| 16 | 88.1; 87.3 | 71 / 81 | 2,215 |
| 32 | 71.0; 94.0 | 76 / 86 | 4,391 |

`filter` reads only the columns it checks and keeps, about a sixth of each file's bytes. Throughput is flat from 4 to 32 workers, within run-to-run noise, while RTT rises from 31-47 ms at 4 workers to 70-90 ms at 16-32. The 2-day runs lasted only 9-13 s, so the 4-day runs are the reliable ones. Fewer than 4 workers wasn't measured.

## Mapping to current behavior

| Stage | Current default | With `io.max_concurrent_reads: 4` | Corresponding measurement |
|---|---|---|---|
| `aggregate` | 1 file per worker, one worker per period up to one per core | 4 workers x 1 file | `4 / SCANS=1`: 27-33 MB/s at 10-11 ms average RTT |
| `clean` (measured as `filter`) | one worker per core | 4 workers | `max_workers 4`: same throughput as 32, a third lower RTT |
| `sample`, `crossref` | polars default, one scan per core | 4 concurrent scans | not measured directly; same multi-file scan mechanism as `aggregate` |
| `convert` | one worker per core, uncapped | uncapped: set `max_workers` and `unzipped_data_directory` | `8 / local / 4 threads`: 72 MB/s at 25 ms |

The recommended settings for this kind of storage are in [Network storage](configuration.md#network-storage-nfs-smb-shared-hdd-arrays).

## Limitations

- **One storage system**, measured while otherwise idle. With other users' load on it, the right numbers are lower, not higher.
- **Mostly single runs.** Four `aggregate` and four `filter` settings were replicated; replicates differed by up to ±15%. Differences smaller than that aren't findings.
- **Wall time is accurate to about 1 s**, which is weak for the shortest runs (the 2-day `filter` runs, 9-13 s).
- **Symlinked input directories**: listing the real directory of about 373,000 GKG 2.1 files once at startup, a real cost for a full-archive run, is not included.
- **Two gaps in the kill rule**: two `filter` runs (16 and 32 workers) went above 100 ms on read RTT alone without being killed, because the rule in force then averaged reads and writes together. They ran for 12-13 s.
