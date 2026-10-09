# Phase 1 review rubric

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
