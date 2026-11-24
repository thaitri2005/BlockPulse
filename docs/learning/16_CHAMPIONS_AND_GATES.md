# Exercise 16: who is in charge? Champions, gates, and rollback

The goal is to understand how BlockPulse decides which detector is active, and how that decision can be audited and undone. You will build a small registry in a scratch folder, promote two detectors, roll one back, and then feed a hand-typed candidate through the promotion gate to see it rejected. Nothing here touches `data/detectors/`, where the real registry lives.

Three terms first. A **bundle** is a folder that describes one detector completely: its thresholds or model file, its versions, and checksums. The **champion** is the bundle currently in charge. A **promotion gate** is a fixed list of checks a candidate must pass before it may become champion.

## Run it

From PowerShell in the project folder:

```powershell
$py = ".\.venv\Scripts\python.exe"
$reg = "$env:TEMP\bp-learn\detectors"
& $py -m blockpulse detectors --root $reg package-rules
& $py -m blockpulse detectors --root $reg promote 01afceac9eea --reason "baseline rules"
& $py -m blockpulse detectors --root $reg package-rules --many-outputs-min 20
& $py -m blockpulse detectors --root $reg promote 7b4efc7635e1 --reason "try a higher fan-out threshold"
& $py -m blockpulse detectors --root $reg rollback --reason "back to the default thresholds"
& $py -m blockpulse detectors --root $reg status
```

`package-rules` writes a bundle for the structural rules and registers it. The detector ID is a hash of the ruleset version and thresholds, so the default rules always get `01afceac9eea` (the same ID as the real champion in [ML evaluation §5](../guides/06-ml-evaluation.md#5-detector-bundles-registry-and-promotion-gate)) and the variant with `--many-outputs-min 20` gets `7b4efc7635e1`.

## Open the output

Open `registry.jsonl` in the scratch folder. It has one line per event. Reduced to three fields, the run above produced:

```text
registered   01afceac9eea
promoted     01afceac9eea   baseline rules
registered   7b4efc7635e1
promoted     7b4efc7635e1   try a higher fan-out threshold
rolled_back  01afceac9eea   back to the default thresholds
```

This is an **event log**: a file that is only ever appended to, where each line records something that happened. No line says "the champion is X". After the rollback, `status` reported:

```json
{
  "champion": "01afceac9eea",
  "previous_champions": [],
  "registered": {
    "01afceac9eea": "structural-rules-v1",
    "7b4efc7635e1": "structural-rules-v1"
  },
```

Run `status` between the second promote and the rollback and you will see `"champion": "7b4efc7635e1"` with `"previous_champions": ["01afceac9eea"]`. A second rollback is refused with `there is no earlier champion to roll back to`.

## Read one function

Open [`src/blockpulse/registry.py`](../../src/blockpulse/registry.py) and read `state()`. It answers "who is the champion?" by replaying the log from the first line:

```python
for event in events:
    kind = event["type"]
    if kind == "registered":
        registered[event["detector_id"]] = event
    elif kind == "promoted":
        if champion is not None:
            previous.append(champion)
        champion = event["detector_id"]
    elif kind == "rolled_back":
        champion = previous.pop()
```

Trace `champion` through the five events above by hand: `None`, `01af…`, `01af…`, `7b4e…`, `01af…`. A rollback is not an edit of history. It is one more event, and the stack `previous` is what it walks back through.

Then read `evaluate_promotion()`. The rules have no gate (they are the baseline). A model candidate is checked against `PROMOTION_CRITERIA` using the evidence stored inside its own bundle. To see that without training a model, save this as `$env:TEMP\bp-learn\gate_demo.py`:

```python
from blockpulse.registry import evaluate_promotion

# A hand-typed candidate. The numbers are made up to exercise the gate; they are not results.
bundle = {
    "family": "isolation-forest-v1",
    "deterministic_refit_identical": True,
    "evidence": {
        "alert_budget_per_1000": 10.0,
        "test_model_alerts_per_1000": 25.0,
        "benign_control_flag_rates": {"small_batch": 0.05, "ordinary_payment": 0.0},
        "rule_scope_flag_rate": 0.95,
    },
}
gate = evaluate_promotion(bundle)
print("passed:", gate["passed"])
for c in gate["criteria"]:
    print(f"{c['name']}: value {c['value']}, requires {c['requirement']}, passed {c['passed']}")
```

Run it with `& $py "$env:TEMP\bp-learn\gate_demo.py"`:

```text
passed: False
deterministic_refit: value True, requires true, passed True
test_alert_rate_vs_budget: value 2.5, requires <= 2.0x the calibration budget, passed False
worst_benign_control_flag_rate: value 0.05, requires <= 0.1, passed True
rule_scope_flag_rate: value 0.95, requires >= 0.9, passed True
```

Follow the one failing value: `25.0 / 10.0` becomes `ratio`, which is compared with `max_test_rate_over_budget`. One failed criterion is enough, because `passed` is `all(...)`. In `promote()`, a failed gate appends a `promotion_rejected` event with the failed criteria and leaves the champion alone. That is why the two real Isolation Forest candidates in the guide are still results: the log records what was tried, with which evidence, and why it lost.

None of these criteria measure accuracy. There are no labels, so the gate checks operating behaviour: alert volume, behaviour on benign control shapes, and coverage of the shapes the rules are defined to match.

## Change one small thing

In `PROMOTION_CRITERIA`, change `max_test_rate_over_budget` from `2.0` to `3.0` and run `gate_demo.py` again. The candidate now passes, with the requirement printed as `<= 3.0x the calibration budget`. Then run:

```powershell
& $py -m pytest -q tests/test_registry.py
```

Put the value back to `2.0` afterwards. The comment above the dictionary explains why this matters: the criteria were written after the first results were seen, so loosening one to let a candidate through would turn a decision into a rationalisation.

## Question to carry forward

After the rollback, `previous_champions` is empty and `7b4efc7635e1` is no longer on the stack. If you wanted it back, which command would you run, and what would the log look like afterwards? What would be lost if rollback were implemented by deleting the last `promoted` line instead?

## What was verified

Run on 2026-11-24 on Linux with a scratch registry: every `detectors` command above, `gate_demo.py` (both with the criterion at 2.0 and, patched in memory rather than by editing the file, at 3.0), and `pytest tests/test_registry.py` (5 passed). `package-model` was not run; it needs a trained model, an evaluation and a sweep on the recorded dataset.
