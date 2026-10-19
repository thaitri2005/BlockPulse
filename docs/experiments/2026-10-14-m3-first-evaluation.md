# M3.3: first threshold calibration and rules comparison

Date: 2026-10-14. Dataset manifest `acc0c2dc3985`; model `iforest-acc0c2dc3985-seed7` (see [M3 model](../guides/06-ml-evaluation.md#2-the-model-isolation-forest)); detector ID `53a956583b6f`. Reproduce with `evaluate-iforest` (see [M3 evaluation](../guides/06-ml-evaluation.md#3-threshold-and-comparison-with-the-rules)). Local output: `data/evaluations/iforest-acc0c2dc3985-seed7-budget25/`.

**Read this first.** There are no independent labels, and calibration and test are each one 10-minute window. Everything below describes alert volume and agreement between two detectors. It is a pipeline check, not evidence that either detector is good, and it will change as sessions are added.

## Setup

- Threshold: the calibration session (Oct 13, 5,021 scored rows) was ranked by model score, and the threshold was set so the top 25 per 1,000 rows are flagged (125 rows). Threshold = 0.5811.
- Test: the Oct 14 session (2,308 rows) was scored once with that threshold. It played no part in training or calibration.
- Rules: the existing `structural-rules-v1` flags, taken from the stored screening output.

## Results

| | Calibration (Oct 13) | Test (Oct 14) |
| --- | ---: | ---: |
| Rows scored | 5,021 | 2,308 |
| Model flagged | 125 (24.9 per 1,000) | 151 (65.4 per 1,000) |
| Rules flagged | 59 (11.8 per 1,000) | 92 (39.9 per 1,000) |
| Flagged by both | 51 | 83 |
| Model only | 74 | 68 |
| Rules only | 8 | 9 |
| Neither | 4,888 | 2,148 |
| Jaccard overlap (both / either) | 0.38 | 0.52 |

## What this does and doesn't show

**The flag rate did not carry over.** The threshold gives exactly 25 per 1,000 on the data it was calibrated on, by construction. On the next window it gives 65 per 1,000, 2.6 times higher. The rules moved the same way, from 11.8 to 39.9 per 1,000, 3.4 times higher. Both detectors seeing more unusual rows in the later window suggests the window itself differs (traffic mix, or a few large batches), not that the model malfunctioned. One window per split can't separate "this day was different" from "the model drifts".

**The training windows differ from the test window.** Fee rate in the test session has a 90th percentile of 7.1 sat/vB against 18.6 in the train split, so fee conditions differed between days. A model trained on a few windows can't tell ordinary market change from unusual transactions.

**The alert volume is not reviewable.** Traffic was about 30,000 added transactions per hour in the calibration session. At 25 per 1,000, the model flags roughly 750 per hour on calibration and 905 per hour on test. The plan's review budget of about 10 alerts per hour would mean about 0.33 flags per 1,000, which on a 5,021-row window is a single row. The right budget is a decision for you; a very tight one needs far more calibration data to be stable. Treat the alerts-per-hour figures as measurements of this threshold, not a target.

**Agreement with the rules.** In calibration the model flagged 51 of the 59 rule-flagged rows; in test, 83 of 92. The rule-only rows are fan-out and fan-in shapes whose scores sit just below the threshold (0.56 to 0.58 against 0.581). That overlap is partly because the model flags more rows overall; it says nothing about which flags are right.

**Some model-only flags look ordinary.** Of the 68 model-only test flags, the most unusual feature (by position in the training distribution) was `round_amount_fraction` for 36, then `n_out` for 16. Examples include a 1-input, 2-output payment with one round-valued output, which is common behavior; it is rare only within these training windows. The rules-flagged rows that both detectors catch are mostly driven by `n_in` and `vsize`. So the model's extra flags are not obviously more interesting than the rules' flags, and some are likely noise from a small training sample. The "most unusual features" labels are descriptive context, not an explanation of the forest.

## Limits

- No labels, so no accuracy, precision or recall.
- One calibration window and one test window; results will move with more sessions.
- Training covers about 30 minutes across three days.
- Capture gaps (a few seconds per session, recorded as a lower bound) mean some transactions were never seen.

## Next

M3.4 adds labeled synthetic structural patterns and benign counterexamples, kept out of training, so the two detectors can be compared on cases where the expected answer is defined. That tests detector behavior on known shapes, not real-world fraud detection.
