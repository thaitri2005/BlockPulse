# Exercise 4: rules as a first detector

The goal is to understand the three structural rules, how one number decides whether a row is flagged, and what a flag cannot tell you. A **threshold** is a fixed cut-off a value is compared with. **Explainability** here means every flag names its rule and the counts that crossed the threshold. A **label** is an independent statement of what a transaction really was; we have none. A flag is not a label. It says the transaction has an unusual shape, and nothing about why.

## Run it

First the rule lab, which runs eight hand-made cases through the rules:

```powershell
.\.venv\Scripts\python.exe -m blockpulse lab --output data/learning/ex04-lab
```

Then screen the feature table from [Exercise 3](03_FILE_TO_FEATURES.md). If `data/learning/ex03-a` does not exist, create it first:

```powershell
.\.venv\Scripts\python.exe -m blockpulse synth-capture --output data/learning/synth-200 --messages 200
.\.venv\Scripts\python.exe -m blockpulse process data/learning/synth-200/messages.jsonl --output data/learning/ex03-a
```

```powershell
.\.venv\Scripts\python.exe -m blockpulse detect data/learning/ex03-a/features.jsonl --output data/learning/ex04-default
```

The summary of the `detect` run:

```text
  "thresholds": { "many_outputs_min": 10, "many_outputs_max_inputs": 2, "many_inputs_min": 10,
                  "many_inputs_max_outputs": 2, "repeated_value_min": 3, "repeated_value_min_inputs": 3 },
  "rows_read": 800,
  "rows_screened": 800,
  "flagged_rows": 119,
  "signal_counts": { "many_outputs": 83, "many_inputs": 36 },
```

These counts describe generated data. The generator makes batch payments and consolidations on purpose, so the share of flagged rows says nothing about the Bitcoin network.

## Open the output

Open `data/learning/ex04-lab/cases.csv`. Three of its rows, with the constant feature columns left out:

```text
case_name,case_kind,description,n_in,n_out,...,expected_rule_ids,actual_rule_ids,matches_expected_rules
batch_payout_shape,target_shape,Few inputs and many outputs.,1,12,...,many_outputs,many_outputs,true
benign_batch_counterexample,benign_counterexample,A routine batch can have the same shape as a flagged fan-out.,1,12,...,many_outputs,many_outputs,true
fanin_just_below_default,boundary_control,Nine inputs is just below the default fan-in threshold.,9,1,...,,,true
```

The second row is the one to find. Its features are identical to the first row's, so it gets the same flag. "Expected" means the rule the case was built to exercise; it checks the code, not the world.

Now open `data/learning/ex04-default/alerts.jsonl`, which holds only flagged rows. The first one (shortened):

```text
{"txid":"104e728b...","source_mode":"synthetic","features":{"n_in":1,"n_out":34,...},
 "ruleset_version":"structural-rules-v1","signal_count":1,"is_flagged":true,
 "signals":[{"rule_id":"many_outputs","explanation":"At most 2 input(s) fund 34 outputs; the rule threshold is 10 outputs."}],
 "interpretation":"Unusual transaction structure is not evidence of wrongdoing."}
```

`screened.jsonl` has all 800 rows, flagged or not. `screened.csv` is the same content flattened for reading.

## Read one function

Open `src/blockpulse/detector.py` and read `screen_transaction()`. Trace `n_out`:

```python
n_in, n_out = features["n_in"], features["n_out"]
repeated = features["max_equal_output_count"]
if n_in <= config.many_outputs_max_inputs and n_out >= config.many_outputs_min:
    signals.append({
        "rule_id": "many_outputs",
        "explanation": f"At most {config.many_outputs_max_inputs} input(s) fund {n_out} outputs; the rule threshold is {config.many_outputs_min} outputs.",
    })
```

`n_out` started as `len(transaction["vout"])` in `features.py` (Exercise 1), was written to `features.jsonl` (Exercise 3), and here is compared with one integer from `DetectorConfig`. If the comparison holds, a signal is appended, and the same `n_out` is written into the explanation. The return value sets `is_flagged` to `bool(signals)`.

The function looks at three of the nine features: `n_in`, `n_out` and `max_equal_output_count`. Fee rate, round amounts and the RBF observation are validated but trigger nothing. Before any rule runs, `_validate_features()` rejects a row with missing or impossible values, so a bad row becomes an error instead of an unflagged row.

`lab.py` calls this same function. Read `default_cases()` there: each case is a name, a kind, and feature counts.

## Change one small thing

Raise the fan-out threshold from 10 to 20 outputs and screen the same file again:

```powershell
.\.venv\Scripts\python.exe -m blockpulse detect data/learning/ex03-a/features.jsonl --output data/learning/ex04-min20 --many-outputs-min 20
```

My run printed:

```text
  "flagged_rows": 101,
  "signal_counts": { "many_outputs": 65, "many_inputs": 36 },
```

Eighteen rows are no longer flagged, and `many_inputs` did not move. Every one of the eighteen has between 11 and 19 outputs; one is `01c1d62a...` with 1 input and 14 outputs. Nothing about those transactions changed. Only our cut-off did. The lab's `threshold_sweep.csv` shows the same effect on the hand-made cases: at threshold 20, `batch_payout_shape` (12 outputs) has `matched` equal to `false`.

The thresholds used are written into each run's `summary.json`, so a result can always be traced to its settings. If you change a default in `DetectorConfig` instead of passing an option, bump `RULESET_VERSION` in `detector.py`, because rows screened under different rules must not look alike. Then rerun the tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_detector.py tests/test_lab.py tests/test_detect.py
```

## Question to carry forward

`batch_payout_shape` and `benign_batch_counterexample` get the same flag. What information would the detector need to tell those two cases apart? List what is in the feature row, then what is not, and say where the missing information could come from. The [review rubric](../guides/02-structural-rules.md#review-rubric) shows how to record a flagged row when that information is absent.

## What was run

Verified on Linux on 2026-11-24: `lab`, `synth-capture`, `process`, both `detect` runs (119 and 101 flagged rows on the generated capture), and `pytest tests/test_detector.py tests/test_lab.py tests/test_detect.py` (6, 1 and 2 passed). Nothing in this exercise needs a network or a service.
