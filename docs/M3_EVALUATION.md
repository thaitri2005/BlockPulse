# M3.3: threshold calibration and comparison with the rules

Third step of [milestone M3](PROJECT_PLAN.md#m3--ml-comparison-and-reproducible-evaluation). It turns the model's scores into flags, then measures alert volume and agreement with the rule baseline. The first results are in the [2026-10-14 experiment report](experiments/2026-10-14-m3-first-evaluation.md).

## Run it

```powershell
.\.venv\Scripts\python.exe -m blockpulse evaluate-iforest data/datasets/dataset-<id>.json data/models/iforest-<id>-seed7
```

Option `--alerts-per-1000` sets the alert budget (default 25). It writes `data/evaluations/<model name>-budget<N>/` containing `summary.json` and `test_top_scores.csv`, and refuses to overwrite. The model must have been trained on the same manifest, or the command stops.

## Method

1. **Calibrate.** Score the calibration split. The threshold is the score ranked just below the budget, so `score > threshold` flags the top `floor(budget/1000 * rows)` rows. Ties at the boundary can flag fewer, and the actual count is reported.
2. **Score the test split once** with that fixed threshold.
3. **Compare with the rules** on the same rows: both, model only, rules only, neither, and Jaccard overlap.
4. **Describe, don't explain.** For the 25 highest-scoring test rows, `test_top_scores.csv` lists up to three features that sit furthest into a tail of the *training* distribution. This is context, not a formal feature attribution.
5. **Name the detector.** `detector_id` hashes the model checksum, threshold, budget, and manifest ID, because changing the threshold changes the detector's behavior even if the model file is unchanged.

## Why a budget instead of a "best" threshold

Without labels there is no accuracy to maximize, so the threshold is a statement about how many alerts a person is willing to look at. That is a policy choice. Note that the budget fixes the flag rate only on the calibration data; later traffic can produce a different rate, as the first run showed (25 per 1,000 became 65).

## Limits

No labels (agreement is not accuracy); one calibration window and one test window; thresholds are not stable until more sessions exist. Reference percentiles are recomputed from the train split at evaluation time and are not yet stored inside the model artifact; M3.5 will package them with it.
