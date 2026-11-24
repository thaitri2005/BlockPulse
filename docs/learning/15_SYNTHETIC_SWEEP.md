# Exercise 15: testing a detector on shapes you define

The goal is to understand what you can learn about a detector when real data has no labels: build transactions whose shape you chose, so the expected answer is defined, and watch how each detector responds. A **sweep** runs one pattern at many sizes (a fan-out with 3 outputs, then 5, 8, 10, up to 300). A **benign control** is an ordinary-looking case, such as a plain two-output payment, that a detector should have no reason to flag. The labels are this project's own definitions, so a sweep shows behaviour on chosen shapes. It is not accuracy on real traffic.

This continues from [Exercise 14](14_THRESHOLD_WITHOUT_LABELS.md). The model here was trained on random practice rows, so its column in the output illustrates the mechanics only. The recorded results are in the [sweep report](../experiments/2026-10-14-m3-synthetic-sweep.md) and [guide 06 §4](../guides/06-ml-evaluation.md#4-synthetic-sweep).

## Run it

Needs `data/learning/ml` with the manifest, model and evaluation from Exercises 12 to 14.

```powershell
$py = ".\.venv\Scripts\python.exe"
$w = "data/learning/ml"
& $py -m blockpulse sweep $w/manifest.json $w/model $w/eval --root $w/screened --output $w/sweep
```

It writes `cases.csv` (every case, its label, both detectors' decisions), `sweep.csv` (rates per pattern and size) and `summary.json`. The evaluation folder supplies the threshold. None of the generated cases goes into training or calibration.

## Open the output

From my run, `summary.json` (some fields left out):

```text
"total_cases": 780,
"rules_match_expected_cases": 780,
"target":         { "cases": 600, "rule_flag_rate": 0.567, "model_flag_rate": 0.287 },
"benign_control": { "cases": 180, "rule_flag_rate": 0.0,   "model_flag_rate": 0.161 },
```

and a few lines of `sweep.csv`:

```text
family,kind,severity,cases,rule_flag_rate,model_flag_rate,median_model_score,median_calibration_percentile,rules_match_expected_rate
fan_out,target,8,20,0.0,0.0,0.5079,0.39,1.0
fan_out,target,10,20,1.0,0.0,0.5388,0.7217,1.0
fan_out,target,50,20,1.0,0.0,0.5654,0.9067,1.0
fan_out,target,100,20,1.0,0.0,0.565,0.905,1.0
fan_out,target,300,20,1.0,0.0,0.5602,0.88,1.0
small_consolidation,benign_control,0,20,0.0,0.6,0.6015,0.98,1.0
large_payment,benign_control,1000000000,20,0.0,0.0,0.5355,0.695,1.0
large_payment,benign_control,100000000000,20,0.0,0.0,0.5345,0.6933,1.0
```

Read the rules column first. It is 0.0 at 8 outputs and 1.0 at 10: a rule is a step at a fixed size. All 780 cases produced exactly the rule IDs their label expects. That confirms the rules do what they say; it does not say the rules are useful.

Then the model column. The median score rises from 8 to 50 outputs and then stops: 0.5654, 0.565, 0.5602 for 50, 100 and 300. This is **saturation**: past the range the model saw in training, a larger size no longer changes the score. The practice training rows have at most 4 outputs, so the plateau comes early and stays under the threshold (0.5959). It is the "no extrapolation" limit from Exercise 13, seen from the outside. The two `large_payment` lines show the same thing for output value.

The practice model also flags 60% of small consolidations, a benign control. Do not read anything into which controls this toy model flags. On the recorded data the model's worst benign family went from 97.5% flagged to 12.5% once it had representative training data (guide 06).

## Read one function

Open `cases()` in [sweep.py](../../src/blockpulse/sweep.py) and trace one decision: the label of a fan-out.

```python
for variant in range(VARIANTS):
    rng = random.Random(f"{seed}-{variant}")
    for n in FAN_SIZES:
        add("fan_out", "target", n, variant, ["many_outputs"] if n >= 10 else [], 1, _payment_values(rng, n), 2.0 + rng.random() * 3, rng)
```

For each of 20 variants and each size `n`, it builds a transaction with 1 input and `n` outputs, and labels it: `many_outputs` is expected when `n >= 10`, nothing otherwise. The label is written here, before any detector runs. `_build()` then makes the transaction internally consistent (inputs equal outputs plus fee), and `run_sweep()` passes it through `extract_features()`, the same function real data uses. Look at one:

```powershell
& $py -c "from blockpulse.sweep import cases; c = cases(11); print(len(c)); one = next(x for x in c if x['family'] == 'fan_out' and x['severity'] == 10); print({k: one[k] for k in ('family', 'kind', 'severity', 'variant', 'expected_rule_ids')}); t = one['transaction']; print(len(t['vin']), len(t['vout']), t['fee'], sum(v['prevout']['value'] for v in t['vin']) - sum(o['value'] for o in t['vout']))"
```

```text
780
{'family': 'fan_out', 'kind': 'target', 'severity': 10, 'variant': 0, 'expected_rule_ids': ['many_outputs']}
1 10 1419 1419
```

One input, ten outputs, and inputs minus outputs equals the fee of 1,419 satoshis. Count the cells to see where 780 comes from: 9 fan-out sizes, 9 fan-in sizes, 6 equal-output counts, 6 fee rates, 5 single controls and 4 large-payment sizes make 39, times 20 variants.

Notice the `fee_outlier` family has an empty label at every rate, because no rule looks at fees. "The rules match their labels" on those cases only means the rules stayed silent as designed.

## Change one small thing

Extend the sweep past its current edge. In `sweep.py`, add `1000` to the end of `FAN_SIZES`, then run the sweep into a new folder:

```powershell
& $py -m blockpulse sweep $w/manifest.json $w/model $w/eval --root $w/screened --output $w/sweep-fan1000
```

In my run the total became 820 cases, all 820 matched their labels, and the new line read:

```text
fan_out,target,1000,20,1.0,0.0,0.5625,0.89,1.0
```

A fan-out of 1,000 scores 0.5625, between the medians for 300 and 50. More than three times larger, no more unusual to the model. Then rerun the tests and undo the edit (or keep it and note the new case count in guide 06):

```powershell
& $py -m pytest -q tests/test_sweep.py
```

A second experiment needs no edit: add `--seed 12 --output $w/sweep-seed12`. The rule rates stay the same; in my run the model's target rate moved from 0.287 to 0.312 and its benign rate from 0.161 to 0.128. Twenty variants per cell is a small sample.

## Question to carry forward

A benign control is "ordinary behaviour we chose to generate". A small batch from an exchange and any other transaction with 2 inputs and 6 outputs have identical features. What could a sweep never tell you about a detector, however many shapes you add? And who decides whether a detector that passes the sweep takes over from the rules? That is Exercise 16.

## What was run for this page

- Ran (Linux, 2026-11-24, outputs under `/tmp`): `sweep` with seeds 11 and 12, the `cases()` one-liner, and `tests/test_sweep.py` (2 passed, on the unmodified code). Outputs are copied from those runs.
- Ran differently: the `FAN_SIZES` experiment was done by setting `blockpulse.sweep.FAN_SIZES` in memory and calling `run_sweep()`, because that session did not modify existing files. The 820 cases and the `fan_out,target,1000` line come from that run; the test file was not rerun with the edit in place.
- Not run: anything on the recorded dataset, which is not in the repository. The recorded figures quoted above come from guide 06.
