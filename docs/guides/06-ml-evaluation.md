# 06 · ML evaluation: rules versus Isolation Forest

Can an unsupervised model on the shared structural features add value over the transparent rule baseline, and is it good enough to replace it? This guide covers the whole chain (dataset, model, threshold, synthetic sweep, registry) and its current answer.

## Current answer (2026-10-19, 2.65 M transactions)

**The rules stay champion; the Isolation Forest is a registered challenger.** On four days of continuous data the model became much better behaved, but it still fails two of the four promotion criteria.

| Measure | First run, 15,515 rows (Oct 14) | Rerun, 2.65 M rows (Oct 19) |
| --- | ---: | ---: |
| Test alert rate against a 25-per-1,000 budget | 65.4 per 1,000 (2.62x) | **28.7 per 1,000 (1.15x)** |
| Model-rule agreement on test (Jaccard) | 0.52 | 0.54 |
| Benign synthetic controls flagged by the model | 43% | **5.6%** |
| Worst benign family (large payments) | 97.5% | 12.5% |
| Synthetic patterns the rules target, flagged by the model | 82.6% | 82.4% |
| Fee-rate outliers up to 2,000 sat/vB flagged | none | none |
| Promotion gate | failed 3 of 4 | **failed 2 of 4** (benign family 0.125 > 0.10; rule coverage 0.82 < 0.90) |

What this supports:

- A threshold calibrated on one day now holds its alert rate on the next (1.15x). The earlier 2.6x jump was a small-data effect.
- With representative training data the model stopped treating large ordinary payments as unusual.
- The model misses small fan-outs the rules catch (0% at 10 outputs) and is blind to fee-rate outliers beyond its training range. It is a complement to the rules, not a replacement.
- Benign and coverage figures move by several points with the 50,000-row training sample alone, so a decision near a gate boundary needs checking across seeds.

What it does **not** support: any accuracy, precision, recall or false-positive claim (there are no labels), or that a flag means anything beyond unusual structure.

Evidence: [first evaluation](../experiments/2026-10-14-m3-first-evaluation.md), [synthetic sweep](../experiments/2026-10-14-m3-synthetic-sweep.md), [rerun on AWS data](../experiments/2026-10-19-m3-rerun-aws-data.md).

## Run the whole chain

```powershell
pip install -e ".[ml,analytics,dev]"
$py = ".\.venv\Scripts\python.exe"
& $py -m blockpulse build-dataset                                        # -> data/datasets/dataset-<id>.json
& $py -m blockpulse train-iforest data/datasets/dataset-<id>.json        # -> data/models/iforest-<id>-seed7/
& $py -m blockpulse evaluate-iforest data/datasets/dataset-<id>.json data/models/iforest-<id>-seed7
& $py -m blockpulse sweep data/datasets/dataset-<id>.json data/models/iforest-<id>-seed7 data/evaluations/iforest-<id>-seed7-budget25
& $py -m blockpulse detectors package-model <manifest> <model> <evaluation> <sweep>
& $py -m blockpulse detectors promote <detector id> --reason "..."       # exit code 4 if the gate rejects it
powershell -File scripts/reproduce_m3.ps1 -Manifest data/datasets/dataset-<id>.json   # rebuild and compare, 6 checks
```

On 2.65 M rows the chain takes about 6 minutes, because splits are read from the Parquet lake.

## 1. Dataset manifest and chronological split

Two ways evaluations quietly go wrong: testing on data the model has seen, and not being able to recreate the split. `build-dataset` fixes both.

- **Chronological, by whole session.** Earlier sessions train, later ones calibrate and test, the way a model meets tomorrow's traffic. A capture is never divided, so rows that arrived together can't straddle two splits. Cut points get closest to 60/20/20 by row count.
- **Recorded exclusions:** synthetic or demo runs, other feature versions, mixed rule versions, sessions under 100 rows.
- **Duplicates:** a txid seen in an earlier session is dropped from later ones.
- **Pinned and verifiable:** every session's screened-file SHA-256, the capture's quality (duration, stop reason, connection errors, gap seconds), and a content-hash `manifest_id`. `blockpulse verify-dataset <manifest>` checks the ID still matches the content and every file is unchanged.
- **Row order is declared** (`selection.row_order`: session, received_at, txid), because the model samples rows by position. Manifests from before 2026-10-19 keep their original file order and read from JSONL, so their results reproduce bit for bit.

`load_split` reads each session from the [lake](05-lake-and-athena.md) when its quality report matches the manifest (only the needed columns; only `txid` outside the requested split) and from JSONL otherwise. On the 527k-row test split: 31 s from the lake, 105 s from JSONL, identical rows.

Current manifest: train 1,594,680 rows (52 sessions, Oct 9 to Oct 17), calibration 530,323 (20 sessions, Sat to Sun), test 526,726 (23 sessions, Sun to Mon).

## 2. The model: Isolation Forest

Many random trees each split on a random feature at a random point until a row is alone; unusual rows are isolated in fewer splits. It needs no labels, which is why it fits, but it only learns "different from the training data", not "bad".

- **Inputs:** the nine shared `transaction-v2` features. Counts and amounts get `log1p` (output value reaches 2.5e11 and split points are chosen uniformly between a column's min and max); `rbf_signaled` becomes 0/1.
- **Missing values** exclude a row and are counted, never filled.
- **Score:** the negated `score_samples`, so larger means more unusual. Not a probability.
- **Reproducible:** seed 7, 100 trees, 256 samples per tree, training capped at 50,000 rows sampled with the seed. Each fit is repeated and must give identical scores. The model file's SHA-256, the manifest ID, parameters and library versions are recorded; loading refuses a mismatched checksum or feature schema.
- **Cost:** 14 s to fit on the 50,000-row sample.

Limits: it can't extrapolate (a value ten times beyond the training range lands where the training maximum does), and it learns whatever the training windows contain.

## 3. Threshold and comparison with the rules

1. **Calibrate:** score the calibration split; the threshold flags the top `floor(budget/1000 × rows)` rows. Default budget 25 per 1,000. Without labels there is no accuracy to maximize, so the threshold is a statement about how many alerts someone is willing to review. On current traffic, 25 per 1,000 is roughly 750 alerts an hour, which is a placeholder, not a decision.
2. **Score the test split once** with that fixed threshold.
3. **Compare with the rules** on the same rows (both, model only, rules only, neither, Jaccard). Agreement is not accuracy.
4. **Describe, don't explain:** the 25 highest-scoring test rows list up to three features that sit furthest into the training distribution's tails.
5. **Name the detector:** `detector_id` hashes the model checksum, threshold, budget and manifest ID, because a new threshold is a new detector.

## 4. Synthetic sweep

`sweep` generates 780 internally consistent transactions (inputs equal outputs plus fee) with known shapes: fan-out and fan-in from 3 to 300, equal-value outputs from 2 to 50, fee rates from 1 to 2,000 sat/vB, and benign controls (ordinary, round-amount, high-fee, small batch, small consolidation, large payments). Nothing synthetic goes into training or calibration. The rules match their labels on all 780. The sweep measures detector behaviour on patterns this project chose; the "benign" controls are not verified real traffic.

## 5. Detector bundles, registry, and promotion gate

- **Bundle:** a self-describing, checksummed detector (schema, threshold, training reference percentiles, manifest ID, checksums of its evaluation and sweep evidence). The rules get one too.
- **Registry:** `data/detectors/registry.jsonl`, an append-only event log (registered, promoted, promotion_rejected, rolled_back). The champion and rollback history are computed by replaying it.
- **Gate** (`PROMOTION_CRITERIA` in `src/blockpulse/registry.py`):

| Criterion | Requirement | Why |
| --- | --- | --- |
| Deterministic refit | Same seed and data give identical scores | Reproducibility |
| Test alert rate | At most 2.0x the calibration budget | A threshold that doesn't hold its volume is hard to operate |
| Worst benign-control family | Flagged at most 10% of the time | Don't flag ordinary behaviour |
| Rule-scope coverage | At least 90% of the patterns the rules target | A replacement shouldn't lose what the baseline does |

These criteria were written on 2026-10-14 after the first results, so they record a decision rather than a pre-registered test. They were not changed for the rerun.

```powershell
& $py -m blockpulse detectors status      # champion, history
& $py -m blockpulse detectors rollback --reason "..."
```

| Detector | Data | Outcome |
| --- | --- | --- |
| Rules `01afceac9eea` | n/a | **Champion** |
| Isolation Forest `53a956583b6f` | 15,515 rows | Rejected (3 of 4 criteria failed) |
| Isolation Forest `d095dc59ced7` | 2.65 M rows | Rejected (2 of 4 failed) |

MLflow was not adopted: the local registry covers register, gated promote and rollback without another service.

## What would change the decision

- A deliberate alert budget.
- A fee feature relative to the hour's median, so extreme fee rates become visible (a new `FEATURE_SET_VERSION`).
- Independent labels, even a small hand-reviewed sample, so agreement can become a measurement (see the [review rubric](02-structural-rules.md#review-rubric)).
- Gate decisions checked across several seeds.
