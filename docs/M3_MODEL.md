# M3.2: Isolation Forest on the train split

Second step of [milestone M3](PROJECT_PLAN.md#m3--ml-comparison-and-reproducible-evaluation). It fits an unsupervised anomaly model on the **train split only** of a [dataset manifest](M3_DATASET.md) and saves it with the metadata needed to reproduce it. No threshold exists yet, and nothing here evaluates quality; those are M3.3 and later.

## What an Isolation Forest is

It builds many random decision trees. Each tree repeatedly picks a random feature and a random split point until a row is alone. Rows that are unusual get isolated in fewer splits. A row's *score* is based on its average path length across the trees. It needs no labels, which is why it fits here, but it also means the model only knows "different from the training data", not "bad".

## Run it

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[ml,dev]"
.\.venv\Scripts\python.exe -m blockpulse train-iforest data/datasets/dataset-<id>.json
```

It writes `data/models/iforest-<manifest id>-seed<seed>/` containing `model.joblib` and `metadata.json`, and refuses to overwrite an existing directory. Options: `--seed` (default 7), `--trees` (default 100), `--max-train-rows` (default 50,000; a larger split is sampled with the seed).

## Design choices

- **Inputs.** The nine shared `transaction-v2` features. Counts and amounts are transformed with `log1p`, because the data is heavy-tailed (output value reaches 2.5e11) and the forest picks split points uniformly between a column's minimum and maximum. `round_amount_fraction` stays as is, and `rbf_signaled` becomes 0 or 1.
- **Missing values.** A row with any null feature is excluded and counted, never filled with a default. None are missing in the current data.
- **Score direction.** scikit-learn's `score_samples` is larger-means-more-normal; we publish its negation, so a larger score always means more unusual. It is not a probability.
- **Reproducibility.** Fixed seed, deterministic sampling, and every fit is repeated once and checked for identical scores (`deterministic_refit_identical`). Metadata records the manifest ID, parameters, library versions, and the model file's SHA-256. `load_model` refuses a model whose checksum or feature schema doesn't match.
- **Bounded cost.** Parameters are modest (100 trees, 256 samples per tree) and training rows are capped.

## Result on manifest `acc0c2dc3985` (2026-10-14)

| Measure | Value |
| --- | --- |
| Training rows | 8,186 (from 3 sessions); 0 skipped |
| Fit time | 1.5 s |
| Peak Python/NumPy allocation during fit | 0.8 MB |
| Peak process memory (whole Python process) | 143 MB |
| Refit with same seed gives identical scores | yes |
| Training score percentiles (p50 / p95 / p99 / max) | 0.410 / 0.568 / 0.683 / 0.812 |

These are in-sample numbers about the model's own training data. They don't measure detection quality.

## Limits worth knowing

- **It can't extrapolate.** A split point is always chosen inside the range seen in training, so a value far *beyond* that range lands in the same place as the largest training value. A transaction ten times bigger than anything in the training windows won't look ten times stranger. The rule baseline has no such limit for its fan-in and fan-out shapes. This is one reason the plan compares both instead of replacing the rules.
- **It learns whatever the training windows contain.** The train split is about 30 minutes across three sessions. Traffic patterns not in those windows will look "unusual" for that reason alone.
- **No labels.** M3.3 can compare which rows the model and the rules flag, but cannot say which is correct.
- **Environment.** The project environment reuses system packages. Importing scikit-learn prints a SciPy/NumPy version warning (NumPy 2.5.3 with SciPy 1.13.1), and loading a saved model emits a NumPy deprecation warning from joblib. Both work today. A clean virtual environment with pinned versions would remove the risk that a future library change breaks loading; the model metadata records the versions used.
