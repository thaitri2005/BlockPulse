# Exercise 3: from a file to a feature table

The goal is to follow `process`, the command that turns a saved capture into events and a feature table without any network access. Four ideas appear. **Normalization** means rewriting many message shapes into one record shape. **Deduplication** means keeping one row when the same thing arrives twice. An **error report** is a file listing every input the code could not use, with the reason. **Determinism** means the same input and the same code give the same output, byte for byte.

## Run it

Use the generated capture from [Exercise 2](02_STREAM_TO_FILE.md). If `data/learning/synth-200` does not exist yet:

```powershell
.\.venv\Scripts\python.exe -m blockpulse synth-capture --output data/learning/synth-200 --messages 200
```

Process it twice, into two new folders:

```powershell
.\.venv\Scripts\python.exe -m blockpulse process data/learning/synth-200/messages.jsonl --output data/learning/ex03-a
.\.venv\Scripts\python.exe -m blockpulse process data/learning/synth-200/messages.jsonl --output data/learning/ex03-b
```

Each run prints its summary. The part to read:

```text
  "lines_read": 200,
  "duplicate_messages": 0,
  "event_counts": { "added": 800, "removed": 0, "mined": 0, "replaced": 0 },
  "features_written": 800,
  "incomplete_added_transactions": 0,
  "issue_count": 0,
  "input_sha256": "c62b5f50d2ffe03bcffd8b6bc92a650682ae6329aea18c58728165ffae8f8010",
```

Now compare the two runs. A **checksum** is a short fingerprint of a file's bytes; equal checksums mean equal files.

```powershell
Get-FileHash data/learning/ex03-a/* | Select-Object Hash, Path
Get-FileHash data/learning/ex03-b/* | Select-Object Hash, Path
```

In my run all five files matched between the two folders: `events.jsonl`, `features.jsonl`, `features.csv`, `errors.jsonl` and `summary.json`. The summary stores the absolute path of the input, so it only matches when both runs read the same path.

## Open the output

Open `data/learning/ex03-a/features.csv` in an editor or with `Import-Csv`. The header and first row:

```text
txid,event_id,received_at,source_mode,feature_set_version,n_in,n_out,value_out_sat,fee_sat,vsize,fee_rate_sat_vb,max_equal_output_count,round_amount_fraction,rbf_signaled
9aa3f488b851d5ab63ade9c4a5d5addb5a5b7d6f6e475415d16a07fde054e1ec,4481bb75...,2026-01-01T00:00:00.250+00:00,synthetic,transaction-v2,1,2,234793,1162,140,8.3,1,0.0,False
```

Find the same `txid` in the first line of `events.jsonl`. That event has `"event_type":"added"` and a `payload` with two outputs, `231171` and `3622` satoshis, `"weight":560` and `"fee":1162`. Check the row by hand: 231171 + 3622 = 234793 (`value_out_sat`); 560 / 4 = 140 (`vsize`); 1162 / 140 = 8.3 (`fee_rate_sat_vb`). The event keeps the whole payload; the feature row keeps nine numbers.

The generated capture is clean, so `errors.jsonl` is empty. The bundled fixture is deliberately messy. Process it too:

```powershell
.\.venv\Scripts\python.exe -m blockpulse process examples/fixtures/mempool_messages.jsonl --output data/learning/ex03-fixture
Get-Content data/learning/ex03-fixture/errors.jsonl
```

Its summary showed 6 lines read, 4 `added` events, 2 feature rows, 1 duplicate added transaction, 1 incomplete added transaction and 2 issues. The two error lines (shortened):

```text
{"line":5,"kind":"missing_or_invalid_feature_input","detail":"Transaction must be an object","event_id":"daacb27c...","txid":"cccc...cccc"}
{"line":6,"kind":"schema","detail":"Message is not valid JSON text"}
```

Line 5 carried only a transaction ID, with no inputs, outputs, weight or fee. It still appears in `events.jsonl`, because we did observe it. It gets no feature row.

## Read one function

Open `src/blockpulse/process.py` and read `process_capture()`. Trace `txid` through the last third of the loop:

```python
previous = seen_features.get(event["txid"])
if previous is not None:
    if previous == features:
        summary["duplicate_added_transactions"] += 1
    else:
        summary["conflicting_feature_rows"] += 1
        issue("feature_conflict", "Different features for one txid; first valid row kept", txid=event["txid"])
    continue
seen_features[event["txid"]] = features
```

`seen_features` is a dictionary from `txid` to the features already written. A second `added` event for the same `txid` is counted and skipped if its features are equal, and reported as a conflict if they differ. Either way the first row stays. This is why `events.jsonl` can have more `added` lines than `features.csv` has rows.

Notice where the entry is stored: after `extract_features(payload)` succeeded. An incomplete observation raises `ValueError` earlier, is written to `errors.jsonl`, and is not remembered. A later complete observation of the same `txid` can therefore still produce a row.

## Change one small thing

Deliver one message twice. Copy the capture and append its first line again:

```powershell
New-Item -ItemType Directory data/learning/ex03-dup | Out-Null
Copy-Item data/learning/synth-200/messages.jsonl data/learning/ex03-dup/
Get-Content data/learning/synth-200/messages.jsonl -TotalCount 1 | Add-Content data/learning/ex03-dup/messages.jsonl
.\.venv\Scripts\python.exe -m blockpulse process data/learning/ex03-dup/messages.jsonl --output data/learning/ex03-dup-out
```

My run printed `"lines_read": 201`, `"duplicate_messages": 1`, `"features_written": 800` and `"issue_count": 0`. The repeated line was recognised by its `message_id` and identical content before any event was written, so the output has the same 800 rows. Find the `seen_messages` block in `process_capture()` that does this, and note that it sits above the event loop.

Then rerun the tests for this area:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_process.py tests/test_features.py
```

## Question to carry forward

Why does `process` record an incomplete transaction as an error instead of using zero? Think about what a row with `fee_sat` 0 and `n_out` 0 would look like to the rules in [Exercise 4](04_RULES_AS_DETECTOR.md), and whether anyone reading the table later could tell it from a real value.

## What was run

Verified on Linux on 2026-11-24: `synth-capture`, both `process` runs (all five output files had equal SHA-256 checksums across the two runs), the fixture run, the duplicated-line experiment, and `pytest tests/test_process.py` and `tests/test_features.py` (3 and 11 passed). The PowerShell commands `Get-FileHash`, `Copy-Item` and `Add-Content` were not run here; I used `sha256sum`, `cp` and shell append instead.
