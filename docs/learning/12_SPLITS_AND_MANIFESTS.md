# Exercise 12: honest evaluation: splits, leakage, manifests

The goal is to understand how BlockPulse decides which rows a model may learn from and which are held back, and how it proves later that the choice was not quietly changed. A **split** is a named share of the data: train (the model learns from it), calibration (a threshold is set on it), test (looked at once, at the end). **Leakage** is information from the held-back data reaching the model, which makes results look better than they are. A **manifest** is a file that records exactly which sessions went into which split, with a **checksum** (a short fingerprint of a file's bytes, here SHA-256) for each one.

Your recorded dataset lives in `data/` and its results are in [guide 06 §1](../guides/06-ml-evaluation.md#1-dataset-manifest-and-chronological-split). This exercise uses practice data, so it shows the mechanics only.

## Run it

`build-dataset` accepts only live sessions, so it cannot split generated captures. To have something it accepts, use the helper the tests use: it writes four sessions of 300 rows with random feature values and made-up txids like `s1-0000`. The helper labels them `ws` so the builder takes them; they are not observations. Keep them under `data/learning/` and never copy them into `data/screened/`.

```powershell
$py = ".\.venv\Scripts\python.exe"
$w = "data/learning/ml"
& $py -c "from pathlib import Path; from tests.test_model import make_manifest; make_manifest(Path('data/learning/ml'))"
```

Add one generated session next to them, to watch it being refused:

```powershell
& $py -m blockpulse synth-capture --output $w/raw-synthetic-1 --messages 2000 --seed 1
& $py -m blockpulse process $w/raw-synthetic-1/messages.jsonl --output $w/processed-synthetic-1
& $py -m blockpulse detect $w/processed-synthetic-1/features.jsonl --output $w/screened/synthetic-1
```

Build and verify:

```powershell
& $py -m blockpulse build-dataset --root $w/screened --output $w/manifest.json
& $py -m blockpulse verify-dataset $w/manifest.json --root $w/screened
```

Run the helper once only; it refuses to overwrite its folders.

## Open the output

`build-dataset` printed (dates trimmed):

```text
"splits": {
  "train":       { "sessions": ["s1", "s2"], "rows": 600, ... },
  "calibration": { "sessions": ["s3"],       "rows": 300, ... },
  "test":        { "sessions": ["s4"],       "rows": 300, ... }
},
"excluded": [
  { "run_id": "synthetic-1", "reason": "not live data (source modes: ['synthetic'])" }
]
```

and `verify-dataset`:

```text
"content_matches_id": true,
"sessions": 4,
"changed_or_missing": [],
"ok": true
```

Three things to notice. The 8,000-row generated session was excluded with its reason written down, not silently skipped. Each session sits wholly in one split. And the sessions are in time order: the earliest train, the latest is the test.

Open `data/learning/ml/manifest.json` and find one entry under `sessions`: it holds `screened_sha256`, the row count, first and last times, and how many of its txids were already seen in an earlier session. Your `manifest_id` will differ from one made on Linux, because the helper writes files with the platform's line endings and every checksum depends on the exact bytes.

## Read one function

Open `choose_cuts()` in [dataset.py](../../src/blockpulse/dataset.py). It receives the row count of each session, in time order, and returns where to cut:

```python
for i in range(1, len(row_counts) - 1):
    for j in range(i + 1, len(row_counts)):
        shares = (sum(row_counts[:i]) / total, sum(row_counts[i:j]) / total, sum(row_counts[j:]) / total)
        cost = sum(abs(share - target) for share, target in zip(shares, fractions))
        if best is None or cost < best[0] - 1e-12:
            best = (cost, i, j)
```

Trace the practice data, `[300, 300, 300, 300]`, against the targets 0.6 / 0.2 / 0.2. There are three ways to cut four sessions into three non-empty groups. Cutting at `(2, 3)` gives shares 0.50 / 0.25 / 0.25, a cost of 0.10 + 0.05 + 0.05 = 0.20. The other two give 0.70. So `(2, 3)` wins: two sessions train, one calibrates, one tests. The function never divides a session. That is the price of the rule "rows captured together stay together", and it is why the split is 50/25/25 here and not 60/20/20.

Why whole sessions and time order? A random row-level split would put transactions from the same minute on both sides. The model would be tested on traffic conditions it had already seen, which is not how it meets tomorrow's data.

Then read the loop at the end of `load_split()`. Sessions outside the requested split are still read, but only for their txids, so a txid already seen in an earlier session is dropped from later ones. Before any row is used, `_read_jsonl()` compares the file's SHA-256 with the manifest and refuses a file that changed.

## Change one small thing

First, ask the function directly:

```powershell
& $py -c "from blockpulse.dataset import choose_cuts; print(choose_cuts([300, 300, 300, 300])); print(choose_cuts([300, 300, 300, 300, 300])); print(choose_cuts([1000, 100, 100, 100]))"
```

```text
(2, 3)
(3, 4)
(1, 2)
```

Five equal sessions reach 60/20/20 exactly. In the third case one large session takes the whole train split: check the shares by hand.

Second, change one byte of data in a copy and verify the manifest against it:

```powershell
Copy-Item -Recurse $w/screened $w/screened-copy
Add-Content $w/screened-copy/s4/screened.jsonl ""
& $py -m blockpulse verify-dataset $w/manifest.json --root $w/screened-copy; $LASTEXITCODE
```

It reports `"changed_or_missing": ["s4"]`, `"ok": false`, and exit code 4. One added line break in the test session is enough. Training against the copy is refused too (`... has changed since the manifest was built`), even though `s4` is not in the train split, because every session is read for the duplicate policy.

Then rerun the tests for this module:

```powershell
& $py -m pytest -q tests/test_dataset.py
```

## Question to carry forward

A chronological split by session stops a model being tested on rows it trained on. It does not make the test split representative: guide 06 notes the first test split was one ten-minute window. What can a split with one short test window prove, and what can it not? Bring that question to Exercise 14, where the threshold moves between calibration and test.

## What was run for this page

- Ran (Linux, 2026-11-24, outputs under `/tmp`): every command above, with `cp -r` and a one-byte append in place of `Copy-Item` and `Add-Content`. Outputs are copied from those runs. `tests/test_dataset.py`: 3 passed. Also confirmed that `build-dataset` on generated sessions alone stops with `At least 3 usable sessions are needed for train/calibration/test`.
- Not run: the PowerShell forms of the copy and append, and anything on the recorded dataset, which is not in the repository.
