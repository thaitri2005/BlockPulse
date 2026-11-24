# Exercise 14: choosing a threshold without labels

The goal is to understand how a score becomes a flag when nobody can say which rows are "right". A **threshold** is the score above which a row is flagged. With labels you would pick the threshold that makes the fewest mistakes. Without labels there are no mistakes to count, so BlockPulse uses an **alert budget**: a statement of how many flags per 1,000 transactions someone is willing to review. The threshold is whatever score produces that many flags on the calibration split, and the test split then shows whether the rate holds on later data.

This continues from [Exercise 13](13_ISOLATION_FOREST.md) with the same practice data. The numbers illustrate the mechanics only; the recorded results are in the [first evaluation](../experiments/2026-10-14-m3-first-evaluation.md) and [guide 06](../guides/06-ml-evaluation.md#3-threshold-and-comparison-with-the-rules).

## Run it

Needs the folder `data/learning/ml` with the manifest and model from Exercises 12 and 13.

```powershell
$py = ".\.venv\Scripts\python.exe"
$w = "data/learning/ml"
& $py -m blockpulse evaluate-iforest $w/manifest.json $w/model --root $w/screened --output $w/eval
```

It scores the calibration split, sets the threshold at the default budget of 25 per 1,000, scores the test split once with that fixed threshold, and compares the model's flags with the rule flags stored on the same rows.

## Open the output

Open `data/learning/ml/eval/summary.json`. From my run (some fields left out):

```text
"alert_budget_per_1000": 25.0,
"threshold": { "threshold": 0.5958768860408181, "target_flags": 7, "actual_flags": 7,
               "calibrated_on": ["s3"] },
"calibration": { "rows": 300, "model_flagged": 7, "model_alerts_per_1000": 23.33, ... },
"test":        { "rows": 300, "model_flagged": 8, "model_alerts_per_1000": 26.67,
                 "rule_flagged": 0,
                 "agreement": { "both": 0, "model_only": 8, "rules_only": 0, "neither": 292, "jaccard": 0.0 } },
```

Read it in this order:

- **Calibration is true by construction.** Seven flags among 300 rows is what the budget asked for. That number cannot surprise you, so it is not evidence of anything.
- **Test is the measurement.** The same threshold flagged 8 of the 300 later rows, 26.67 per 1,000 against a budget of 25. On the first recorded run this ratio was 2.6 (65.4 per 1,000); on the 2.65 M-row rerun it was 1.15 (28.7 per 1,000). Guide 06 explains why the rate moved.
- **Agreement here is empty, and that is a property of the practice data.** The helper's rows carry no rule flags, so `rule_flagged` is 0 and every model flag is `model_only`. **Jaccard** overlap is rows flagged by both detectors divided by rows flagged by either. On the recorded test splits it was 0.52 and then 0.54.

Agreement is not accuracy. It counts how often two detectors point at the same rows. Both could be pointing at ordinary transactions, and a model that only copied the rules would score 1.0 while adding nothing.

`test_top_scores.csv` lists the 25 highest-scoring test rows with up to three features that sit furthest into the tails of the training data. Those are descriptions, not explanations of the forest.

## Read one function

Open `choose_threshold()` in [evaluate.py](../../src/blockpulse/evaluate.py):

```python
ordered = np.sort(np.asarray(scores))[::-1]
k = int(alerts_per_1000 / 1000 * len(ordered))
if k < 1:
    raise ValueError(f"A budget of {alerts_per_1000} per 1,000 flags nothing among {len(ordered)} calibration rows")
threshold = float(ordered[k])
return {"threshold": threshold, "target_flags": k, "actual_flags": int((ordered > threshold).sum())}
```

Trace the practice run. There are 300 calibration scores, sorted from highest to lowest. `k = int(25 / 1000 * 300) = int(7.5) = 7`: the budget is rounded down. `ordered[7]` is the eighth-highest score, 0.5958…, and the rule is `score > threshold`, strictly greater. Exactly the seven rows above it are flagged. That is why the calibration rate reads 23.33 and not 25: seven is the largest whole number of rows inside the budget.

Tied scores can make the count fall short, which is why `actual_flags` is reported next to `target_flags`. See it on ten made-up scores:

```powershell
& $py -c "from blockpulse.evaluate import choose_threshold; print(choose_threshold([0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0], 300)); print(choose_threshold([0.9, 0.8, 0.7, 0.7, 0.7, 0.4, 0.3, 0.2, 0.1, 0.0], 300))"
```

```text
{'threshold': 0.6, 'target_flags': 3, 'actual_flags': 3}
{'threshold': 0.7, 'target_flags': 3, 'actual_flags': 2}
```

In the second list the third and fourth scores are equal, so the threshold lands on the tie and only two rows are strictly above it.

In `evaluate_detector()`, find where `detector_id` is built: it hashes the model checksum, the threshold, the budget and the manifest ID. A new threshold is a new detector, with its own ID.

## Change one small thing

Double the budget, into a new output folder:

```powershell
& $py -m blockpulse evaluate-iforest $w/manifest.json $w/model --root $w/screened --output $w/eval-budget50 --alerts-per-1000 50
```

In my run the threshold dropped from 0.5959 to 0.5777, the target became 15 rows (50.0 per 1,000 on calibration), the test split had 17 flags (56.67 per 1,000), and the `detector_id` changed. The model file is the same; only the willingness to review moved.

Now ask for a budget that is too tight for the data:

```powershell
& $py -m blockpulse evaluate-iforest $w/manifest.json $w/model --root $w/screened --output $w/eval-budget3 --alerts-per-1000 3
```

```text
BlockPulse: A budget of 3.0 per 1,000 flags nothing among 300 calibration rows
```

Three per 1,000 of 300 rows is 0.9 of a row. The first evaluation report makes the same point with real figures: a review budget of about ten alerts an hour was a single row of its calibration window. A tight budget needs a lot of calibration data before the threshold is stable.

Then rerun the tests:

```powershell
& $py -m pytest -q tests/test_evaluate.py
```

## Question to carry forward

The test rate tells you whether the alert volume carried over. It cannot tell you whether the flagged rows deserved a look. What is the smallest set of independent labels that would turn one of these numbers into a measurement? See the [review rubric](../guides/02-structural-rules.md#review-rubric). Until then, Exercise 15 tries a different route: shapes whose expected answer you define yourself.

## What was run for this page

- Ran (Linux, 2026-11-24, outputs under `/tmp`): `evaluate-iforest` at budgets 25, 50 and 3, the `choose_threshold` one-liner, and `tests/test_evaluate.py` (3 passed). Outputs are copied from those runs. Detector IDs are left out because they depend on file checksums that differ on Windows (see Exercise 12).
- Not run: anything on the recorded dataset, which is not in the repository. The recorded figures quoted above come from guide 06 and the first evaluation report.
