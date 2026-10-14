# M3.1: dataset manifest and chronological split

First step of [milestone M3](PROJECT_PLAN.md#m3--ml-comparison-and-reproducible-evaluation). Before any model is fitted, we fix *exactly which data* trains it, calibrates its threshold, and tests it, and write that down in a file that can be rebuilt and verified.

## Why

Two ways evaluations quietly go wrong: testing on data the model has already seen, and not being able to recreate the split later. A **chronological** split (earlier data trains, later data tests) mirrors real use, where a model scores tomorrow's traffic. A **manifest** pins the split to file checksums, so a changed file is detected instead of silently altering results.

## Run it

```powershell
.\.venv\Scripts\python.exe -m blockpulse build-dataset
```

It reads every run folder under `data/screened/`, writes `data/datasets/dataset-<id>.json`, and prints the splits. Running it again on unchanged data reports `"created": false`; it never overwrites a manifest with different content. Use `--min-rows` to change the minimum session size (default 100).

## How the split is made

- **Unit: whole sessions.** One capture is never divided, so rows that arrived together (including bursts and related transactions) can't straddle train and test.
- **Order: by first received time.** Cut points are chosen so the row shares come closest to 60% train, 20% calibration, 20% test, with each split non-empty. The rule is deterministic.
- **Exclusions are recorded with reasons:** not live data (synthetic or demo runs), a feature version other than `transaction-v2`, mixed rule versions, or too few rows.
- **Duplicates:** a txid already seen in an earlier session is dropped from later ones when loading, and the manifest counts how many such repeats each session has. So far there are none.
- **Provenance:** each session records its screened-file SHA-256, the capture's SHA-256, and the capture's quality (duration, stop reason, connection errors, recorded gap seconds), found by following the summaries back from screening to the raw capture.
- `manifest_id` is a hash of the content. Paths are relative to the screened folder, so rebuilding elsewhere gives the same ID.

`load_split(manifest, "train", root)` in `src/blockpulse/dataset.py` returns a split's rows and refuses to read a file whose checksum no longer matches. M3.2 will use it.

## Result on the data collected so far (2026-10-14)

Manifest `acc0c2dc3985`, 15,515 rows from 5 sessions:

| Split | Sessions | Rows | Flagged by rules | Time span (UTC) |
| --- | --- | ---: | ---: | --- |
| train | Oct 9, Oct 12 04:21, Oct 12 17:58 | 8,186 | 205 | Oct 9 02:39 to Oct 12 18:08 |
| calibration | Oct 13 06:21 | 5,021 | 59 | Oct 13 06:21 to 06:31 |
| test | Oct 14 06:49 | 2,308 | 92 | Oct 14 06:49 to 06:59 |

Excluded: two 22-row captures from early Oct 8 (below the 100-row minimum).

The shares come out at 53% / 32% / 15%, not 60/20/20, because whole sessions are the smallest unit and the sessions are uneven in size.

## Limits to keep in mind

- The calibration and test splits are each a single 10-minute window, so any result on them is a pipeline check, not a finding. More sessions will change the manifest; re-run `build-dataset` and keep the old file.
- The sessions have reconnect gaps (2 to 12 seconds recorded each, a lower bound), and one training session stopped early at the retry limit after 4 connection errors.
- Flag counts come from uncalibrated rules, not labels. They show how rows are distributed, not how good any detector is.
- Captured sessions are all in early-morning or evening UTC.
