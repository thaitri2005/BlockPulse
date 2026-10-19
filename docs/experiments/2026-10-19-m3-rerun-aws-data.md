# M3 rerun on 89 hours of AWS-collected data

Date: 2026-10-19. Same code, seed (7), alert budget (25 per 1,000) and promotion criteria as the [first evaluation](2026-10-14-m3-first-evaluation.md); only the data changed.

## Data

The AWS collector (E3a) ran 89 hourly sessions without a failure, from 2026-10-15 19:45 to 2026-10-19 12:45 UTC, covering a full weekend. All 89 sessions stopped on `duration` (none truncated). Downloaded 17.9 GB (raw 10.99 GB, processed 4.18, screened 2.34, lake 0.59); file counts and byte totals matched S3 exactly.

Manifest `dcce553c9bae` (94 sessions including the 5 earlier laptop sessions):

| Split | Sessions | Rows | Rule-flagged | Window (UTC) |
| --- | ---: | ---: | ---: | --- |
| train | 52 | 1,594,680 | 22,933 | Oct 9 02:39 to Oct 17 17:45 |
| calibration | 20 | 530,323 | 8,911 | Oct 17 17:45 to Oct 18 13:45 (Sat-Sun) |
| test | 23 | 526,726 | 11,196 | Oct 18 13:45 to Oct 19 12:45 (Sun-Mon) |

Run times on the laptop: manifest 12 min, train 12 min, evaluate 8 min, sweep 2 min, package 2.5 min. The time is spent re-reading and checksumming 2.3 GB of screened JSON at each step; the fit itself took 14 s on the default 50,000-row training subsample.

## Results, against the first run

| Measure | First run (15,515 rows) | Rerun (2.65 M rows) |
| --- | ---: | ---: |
| Test model alerts per 1,000 (budget 25) | 65.4 | **28.7** |
| Test rate / budget | 2.62x | **1.15x** |
| Test rule alerts per 1,000 | 39.9 | 21.3 |
| Model-rule agreement on test (Jaccard) | 0.52 | 0.54 |
| Benign controls flagged by the model | 43% | **5.6%** |
| Worst benign family (large payments) | 97.5% | **12.5%** (only the 1,000 BTC size) |
| Rule-scope synthetic patterns flagged by the model | 82.6% | 82.4% |
| Fee-rate outliers (up to 2,000 sat/vB) flagged | none | none |

Promotion gate for detector `d095dc59ced7`: **not promoted**. It passes determinism and alert-rate stability (1.15x, limit 2x), and fails worst benign family (0.125, limit 0.10) and rule-scope coverage (0.824, limit 0.90). The rules remain champion.

## What this supports

- The threshold now holds its alert rate across a day boundary and a weekend: 25 per 1,000 on calibration, 28.7 on test. The earlier 2.6x jump was mostly a small-data effect, as suspected.
- With representative training data the model stopped treating large but ordinary payments as unusual (97.5% to 12.5%).
- The model still misses small fan-outs the rules are defined to catch (0% at 10 outputs, 70% at 20) and is blind to fee-rate outliers, so it is a complement to the rules, not a replacement.
- The gate criteria were set before this rerun and were not changed to let the model pass.

As before there are no labels: nothing here measures accuracy, only alert volume, stability, agreement and behaviour on synthetic patterns.

## Follow-ups

- Loading splits re-reads every screened JSON file; reading the Parquet lake instead would cut the chain from about 35 minutes to a few (fits DE3/DE4).
- A fee-rate feature relative to the hour's median would address the fee blind spot; that is a feature-set change (new `FEATURE_SET_VERSION`).
