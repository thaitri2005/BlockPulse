# 02 · Structural rules

Three transparent rules screen each transaction for unusual shape; they are the current champion detector ([ML evaluation](06-ml-evaluation.md)). Milestone M2.0.

The first offline structural baseline is implemented and checked against controlled cases and a ten-minute real observation. This validates rule mechanics, not real-world detection quality.

It adds a small offline rules layer after Phase 0's capture and feature extraction:

```text
messages.jsonl -> process -> transaction-v2 features.jsonl -> detect -> alerts + review table
```

It is deliberately a first baseline rather than a machine-learning model. Every signal names a rule and explains which observed counts crossed its threshold. A signal describes transaction shape only; it is not a fraud probability, a claim about intent, or a ground-truth label.

## Run it

The `transaction-v1` output does not have the added fields. Reprocess the saved raw capture to produce a current feature file, then screen it:

```powershell
.\.venv\Scripts\python.exe -m blockpulse process data/raw/regional-check-01/messages.jsonl
.\.venv\Scripts\python.exe -m blockpulse detect data/processed/<new-run>/features.jsonl
```

Each command prints its actual output path. Detection can also be run on the included synthetic demo after processing it. Tune the thresholds explicitly when running a controlled experiment; defaults are provisional and are not calibrated against labels.

## What the rules check

| Rule | Default trigger | Why it is a weak signal |
| --- | --- | --- |
| `many_outputs` | At most 2 inputs and at least 10 outputs | Payouts and batching can naturally create this shape. |
| `many_inputs` | At least 10 inputs and at most 2 outputs | Consolidation and ordinary wallet behavior can naturally create this shape. |
| `repeated_output_values` | At least 3 inputs and 3 or more positive outputs sharing the same amount | Equal-value outputs occur in many benign cases; this rule is especially broad. |

Rules can overlap. A row can have multiple signals, each kept separately with an explanation. Thresholds are available as `detect` command options; the exact values used are written to the run summary.

The `transaction-v2` shared feature set adds:

- `max_equal_output_count`: maximum number of positive outputs with an identical satoshi value.
- `round_amount_fraction`: fraction of positive outputs divisible by 100,000 satoshis. It is a descriptive feature and does not trigger a default rule.
- `rbf_signaled`: true if any input sequence signals replaceability, false if all sequences are present and none signal it, or null when the observation is incomplete/ambiguous. It is descriptive and does not trigger a default rule.

Zero-value outputs are excluded from these descriptive value-pattern features. Transaction-v1 results remain historical artifacts; detection requires transaction-v2, so re-run `process` on raw messages to upgrade.

## Outputs

The unique output directory contains:

| File | Contents |
| --- | --- |
| `screened.jsonl` | Every valid unique transaction row, features, signals, and interpretation. |
| `alerts.jsonl` | Only rows with one or more signals. |
| `screened.csv` | Flat review-friendly view, including rule IDs and explanations. |
| `errors.jsonl` | Malformed, stale-version, or conflicting feature rows. |
| `summary.json` | Input checksum, versions, thresholds, row counts, signal counts, and limitations. |

Rows with the same transaction ID and same feature values are deduplicated. Conflicting values are reported as an issue; the first row remains authoritative. Re-running against the same source path and code produces identical outputs. Existing output directories are not overwritten.

## Controlled rule lab

Run `blockpulse lab` to check the rule mechanics without using live transaction data:

```powershell
$run = [guid]::NewGuid().ToString("N")
.\.venv\Scripts\python.exe -m blockpulse lab --output "data/labs/run-$run"
```

The lab includes ordinary controls, fan-in/fan-out/repeated-value shapes, threshold boundaries, and benign counterexamples. In particular, each benign counterexample has the same feature counts as a target-shape example. The detector therefore gives the pair the same rule signal, demonstrating that these features alone cannot tell why a transaction has that shape.

It writes `cases.csv` and `cases.jsonl` with the expected and actual rule IDs, plus `threshold_sweep.csv` and `threshold_sweep.jsonl`. The sweep changes the fan-in or fan-out count threshold to 5, 10, and 20, one threshold at a time, and shows which scenarios then match. `summary.json` records the rule and feature versions, counts, and scenario checksum.

“Expected rule IDs” means the rule pattern the synthetic case was designed to exercise. It is a check that the code behaves as specified, not a fraud label or precision/recall metric. These controls test deterministic behavior and threshold sensitivity only; they do not estimate real-world detection quality.

When reviewing recorded rows, use the short [Phase 1 review rubric](02-structural-rules.md#review-rubric). It separates rule correctness, data quality, independently verified benign context, and unknown context.

## Limits

The ten-minute real capture produced 2,732 complete feature rows and 73 structural signals. The rules and each signal were checked against the archived inputs, but the sample has no independent labels, so calibration and detection quality remain unknown. The controlled lab highlights benign shapes that also trigger the rules. The [manual-review rubric](02-structural-rules.md#review-rubric) is ready for future labeled evaluation. Keep controlled results separate from real observations; do not call synthetic rule checks accuracy.

Results are loaded into Postgres and served by the API ([04](04-postgres-api-dashboard.md)), and written to the lake ([05](05-lake-and-athena.md)).

## Review rubric

Use this checklist when looking at rows from `blockpulse detect`. It checks whether the recorded structure and the code's rule agree. It does not decide whether a transaction is good, bad, or suspicious.

For each flagged row:

1. **Check the record.** Confirm it came from a complete `added` transaction and the feature row has the current feature-set version. If the source entry is incomplete, classify it as a data-quality issue and do not interpret its rule result.
2. **Check the rule.** Compare `n_in`, `n_out`, and `max_equal_output_count` with the threshold values in `summary.json`. Confirm each listed rule should fire and note any rule that is missing or unexpected.
3. **Check context only when independently available.** A batching or consolidation explanation must come from evidence outside the rule itself. If there is no independent evidence, use `context_unknown`; do not infer intent from counts, amounts, replaceability, or a flag.
4. **Record the review.** Use one outcome below and write the evidence briefly. Keep transaction IDs as internal references; do not publish claims about people or wallet owners.

| Review outcome | Use it when |
| --- | --- |
| `rule_applied_as_written` | The feature values are valid and all listed signals match the recorded thresholds. |
| `rule_mismatch` | A signal is missing or appears despite the rule condition not being met. |
| `data_quality_issue` | Required source details or feature values are missing, malformed, or contradictory. |
| `benign_context_independently_verified` | Separate evidence supports an ordinary explanation; cite that evidence in private notes. |
| `context_unknown` | There is no independent context. This is the default for real mempool rows. |

Review a small sample of unflagged rows too, especially rows just below a threshold. Record structural patterns the current rules do not cover. Do not call these false negatives without an independently defined target label.

For a threshold comparison, change one setting at a time and preserve the original run. Write down the question before looking at results (for example, “How many of these cases match when the fan-in threshold moves from 10 to 20?”). Do not select a threshold because it appears to separate the current sample; that would only fit this sample.

Do not report accuracy, precision, recall, or fraud-detection performance from the synthetic lab or unlabeled mempool sample. Those measures require independent labels, a sampling method, and a held-out dataset.
