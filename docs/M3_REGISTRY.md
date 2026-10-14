# M3.5: detector bundles, registry, and rollback

Fifth step of [milestone M3](PROJECT_PLAN.md#m3--ml-comparison-and-reproducible-evaluation). It makes "which detector is active, and why" an auditable fact instead of a convention.

## Concepts

- **Bundle.** A self-describing, checksummed detector: schema, threshold, training reference percentiles (a 101-point quantile grid per feature, for describing which features look unusual), the manifest it was trained on, and checksums of the evaluation and sweep evidence. The rule baseline gets a bundle too, so rules and model are handled the same way. The detector ID for a model is the one printed by `evaluate-iforest`.
- **Registry.** `data/detectors/registry.jsonl`, an append-only file of events: registered, promoted, promotion_rejected, rolled_back. The champion and the rollback history are computed by replaying it, so nothing is edited in place and everything is auditable.
- **Gate.** A model can become champion only if it clears the criteria in `PROMOTION_CRITERIA` (`src/blockpulse/registry.py`), checked against the evidence packaged in its bundle. A failed attempt is recorded with the failed criteria.
- **Compatibility check.** Loading a bundle verifies its schema version, the feature-set version and column order, and the model file's checksum. A mismatch raises an error; nothing falls back to another detector silently.

## Run it

```powershell
$py = ".\.venv\Scripts\python.exe"
& $py -m blockpulse detectors package-rules                      # bundle + register the rule baseline
& $py -m blockpulse detectors promote <rules id> --reason "..."   # baseline needs no gate
& $py -m blockpulse detectors package-model data/datasets/dataset-<id>.json data/models/<model> data/evaluations/<eval> data/sweeps/<sweep>
& $py -m blockpulse detectors promote <model id> --reason "..."   # exit code 4 if the gate rejects it
& $py -m blockpulse detectors rollback --reason "..."
& $py -m blockpulse detectors status
```

In code: `registry.champion_directory(root)` returns the active bundle (and fails if there is none), and `registry.score_rows(directory, rows)` applies a bundle to feature rows.

## Promotion criteria

| Criterion | Requirement | Why |
| --- | --- | --- |
| Deterministic refit | Same seed and data give identical scores | Reproducibility |
| Test alert rate | At most 2.0x the calibration budget | A threshold that doesn't hold its volume is hard to operate |
| Worst benign-control family | Flagged at most 10% of the time | Don't flag ordinary behavior |
| Rule-scope coverage | At least 90% of the synthetic patterns the rules are defined to catch | A replacement shouldn't lose what the baseline does |

These numbers were chosen on 2026-10-14 after seeing the first results, so they document a decision rather than pre-registering a test. Change them deliberately and record why.

## Result on the current data

| Event | Outcome |
| --- | --- |
| Rule baseline `01afceac9eea` | Registered and promoted: champion |
| Isolation Forest `53a956583b6f` | Registered; promotion rejected (test rate 2.62x, worst benign control 97.5%, rule-scope coverage 82.6%) |

The rules remain champion. Rolling back is tested with rule bundles of different thresholds, including the case where the rollback target has gone bad: the current champion stays in place.

## Limits

The registry is a local file with one writer, with no locking or access control. The gate checks packaged evidence; it doesn't re-run the evaluation (use `scripts/reproduce_m3.ps1` for that). Reference percentiles are stored in the bundle but the evaluation command still recomputes them from the train split; they are equivalent by construction.
