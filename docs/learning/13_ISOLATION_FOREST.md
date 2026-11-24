# Exercise 13: what an Isolation Forest can and can't see

The goal is to understand what the model's score means and where it goes blind. An **Isolation Forest** builds many random trees; each tree keeps splitting the data on a random feature at a random point until a row is alone, and rows that end up alone after few splits are scored as unusual. It is **unsupervised**: it needs no labels, so it learns "different from the training rows" and nothing about whether a transaction is good or bad. An **anomaly score** is that measure of difference. It is not a probability.

This exercise continues from [Exercise 12](12_SPLITS_AND_MANIFESTS.md) and uses the same practice data (random feature values from the test helper). The numbers below illustrate the mechanics only. The recorded results are in [guide 06 §2](../guides/06-ml-evaluation.md#2-the-model-isolation-forest).

## Run it

Needs `pip install -e ".[ml,dev]"` and the folder `data/learning/ml` from Exercise 12.

```powershell
$py = ".\.venv\Scripts\python.exe"
$w = "data/learning/ml"
& $py -m blockpulse train-iforest $w/manifest.json --root $w/screened --output $w/model
```

It fits on the train split only (sessions `s1` and `s2`) and writes `model.joblib` and `metadata.json`.

## Open the output

Open `data/learning/ml/model/metadata.json`. From my run:

```text
"training": { "rows_in_split": 600, "rows_skipped_missing_values": 0, "rows_used": 600,
              "subsampled_to_max_train_rows": false },
"train_score_percentiles": { "p1": 0.4493, "p50": 0.511125, "p90": 0.564768,
                             "p95": 0.5792, "p99": 0.607681, "p100": 0.679943 },
"deterministic_refit_identical": true,
"model_sha256": "56e630f8d18567fe9e35cdc32b2ea9c1174ef94122d934220744987a4e4f329a",
```

Read four things:

- `train_score_percentiles` are **in-sample** scores: the model scoring the rows it was fitted on. They describe the spread of scores (half the rows below 0.51, the most unusual at 0.68) and say nothing about detection quality. The file's own `limits` list says so.
- `deterministic_refit_identical: true` means the code fitted a second forest with the same seed and data and got identical scores. Without that, no later number could be reproduced.
- `dataset_manifest_id` ties the model to one manifest, and `model_sha256` lets `load_model()` refuse a file that changed.
- There is no threshold here. A score becomes a flag only in Exercise 14.

## Read one function

Open `feature_matrix()` in [model.py](../../src/blockpulse/model.py). It turns feature dictionaries into the table of numbers the forest reads:

```python
for index, row in enumerate(rows):
    features = row["features"]
    if any(features.get(name) is None for name in MODEL_COLUMNS):
        continue
    vector = [np.log1p(float(features[name])) for name in LOG1P_COLUMNS]
    vector += [float(features[name]) for name in RAW_COLUMNS]
    vector += [1.0 if features[name] else 0.0 for name in FLAG_COLUMNS]
```

Trace `value_out_sat` for one row. This command builds two rows with the test helper, the second with `rbf_signaled` unknown:

```powershell
& $py -c "from blockpulse.model import feature_matrix; from tests.test_model import row; m, kept, skipped = feature_matrix([row(), row(rbf_signaled=None)]); print(m.round(4).tolist(), kept, skipped)"
```

```text
[[0.6931, 1.0986, 10.8198, 5.7071, 4.9488, 1.1314, 0.6931, 0.0, 1.0]] [0] 1
```

The helper's `value_out_sat` is 50,000. The third number is 10.8198, which is `log1p(50000)`: the natural logarithm of one plus the value. A **log transform** squeezes a range that spans many orders of magnitude into a short one. It matters here because a tree picks each split point uniformly between a column's minimum and maximum. Output values on real traffic reach 2.5e11 satoshis; on the raw scale nearly every split would land above almost all rows and separate nothing. The last number, 1.0, is `rbf_signaled: true` as a number.

The second row is missing from the output: `kept` is `[0]` and `skipped` is 1. A row with an unknown feature is left out and counted. It is never filled with zero, the same rule as everywhere else in the project.

Then read `train_isolation_forest()` from `load_split(manifest, "train", root)` down to `fit`. Only the train split is ever loaded, which is the leakage rule from Exercise 12 enforced in code. `unusualness()` negates scikit-learn's score so that larger means more unusual.

## Change one small thing

Probe the model with one feature pushed further and further out. The practice training rows have output values between 1,000 and 10,000,000 satoshis:

```powershell
& $py -c "from pathlib import Path; from blockpulse.model import feature_matrix, load_model, unusualness; from tests.test_model import row; model, meta = load_model(Path('data/learning/ml/model')); values = (10**4, 10**6, 10**7, 10**8, 10**9, 10**11); m, kept, skipped = feature_matrix([row(value_out_sat=v) for v in values]); [print(v, round(float(s), 6)) for v, s in zip(values, unusualness(model, m))]"
```

```text
10000 0.663325
1000000 0.561434
10000000 0.553779
100000000 0.553779
1000000000 0.553779
100000000000 0.553779
```

From the training maximum upward the score stops moving: ten million satoshis and a hundred billion get the same score to six decimals. A tree has no split point beyond the largest value it saw in training, so everything past it falls into the same leaf. This is **no extrapolation**: the model cannot tell "a bit beyond anything seen" from "ten thousand times beyond". On the recorded data the same effect makes fee rates up to 2,000 sat/vB invisible (guide 06). Note also that the highest score in the list belongs to the smallest value: 10,000 satoshis sits in the thin low end of these training rows. "Unusual" follows the training rows, not size.

Second experiment: train again with another seed, into a new folder (the command refuses to overwrite an existing one):

```powershell
& $py -m blockpulse train-iforest $w/manifest.json --root $w/screened --output $w/model-seed8 --seed 8
```

In my run `p99` moved from 0.607681 to 0.611399 and `model_sha256` changed. Same data, different random trees, slightly different scores. Guide 06 reports that figures near a gate boundary need checking across seeds for this reason.

Then rerun the tests:

```powershell
& $py -m pytest -q tests/test_model.py
```

## Question to carry forward

The model scores a row by how easily it is separated from the training rows. If Tuesday's traffic is ordinary but different from the training days (lower fees, say), what happens to Tuesday's scores? Whose job is it to notice: the model's, or the threshold's? Exercise 14 starts there.

## What was run for this page

- Ran (Linux, 2026-11-24, outputs under `/tmp`, Python 3.12.3, scikit-learn 1.5.0): `train-iforest` with seeds 7 and 8, both one-liners, a second `train-iforest` into the existing folder (refused with `File exists`), and `tests/test_model.py` (3 passed). Outputs are copied from those runs; on Windows expect small differences at most, and different IDs (see Exercise 12).
- Not run: anything on the recorded dataset, which is not in the repository. The recorded figures are those in guide 06.
