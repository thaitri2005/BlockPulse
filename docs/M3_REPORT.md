# M3 detection report: rules versus Isolation Forest

Date: 2026-10-14. Data: 15,515 live transactions from 5 capture sessions of 5 to 10 minutes each (Oct 9 to Oct 14). This report states what the evidence supports, what it doesn't, and the resulting decision.

## Question

Can an unsupervised model on the shared structural features add value over the transparent rule baseline, and is it good enough to replace it?

## Short answer

**No, not yet. The rules stay champion and the model stays a registered challenger.** The model reacts to size more gradually than the rules do, but on this evidence it flags ordinary behavior, misses what it has never seen, and doesn't hold its alert rate from one window to the next. None of that is a finding about real-world detection accuracy, because no labels exist.

## What was built and run

| Step | Result | Detail |
| --- | --- | --- |
| Dataset manifest | Manifest `acc0c2dc3985` | Train 8,186 / calibration 5,021 / test 2,308 rows, split by whole session in time order ([guide](M3_DATASET.md)) |
| Model | Isolation Forest, 100 trees, seed 7 | Trained on the train split only; 1.5 s fit; identical on refit ([guide](M3_MODEL.md)) |
| Threshold | 0.5811 | Top 25 per 1,000 of the calibration session ([guide](M3_EVALUATION.md)) |
| Held-out test | Oct 14 session, scored once | [Results](experiments/2026-10-14-m3-first-evaluation.md) |
| Synthetic sweep | 780 labeled cases | [Results](experiments/2026-10-14-m3-synthetic-sweep.md) |
| Packaging | Detector bundles, registry, gate | [Guide](M3_REGISTRY.md) |

## Evidence

| Measure | Rules | Model |
| --- | ---: | ---: |
| Flagged per 1,000, calibration session | 11.8 | 24.9 (set by the budget) |
| Flagged per 1,000, test session | 39.9 | 65.4 |
| Synthetic patterns the rules are defined to catch, flagged | 100% | 82.6% |
| Synthetic fee outliers up to 2,000 sat/vB, flagged | 0% | 0% |
| Benign controls flagged (five of six families) | 0% | 0% |
| Benign "large payment" controls flagged | 0% | 97.5% |

## Conclusions the evidence supports

1. **The pipeline is reproducible.** Rebuilding the manifest, retraining, recalibrating and re-running the sweep from the same inputs reproduced the manifest hash, model checksum, detector ID, threshold, test flag count, and synthetic cases byte for byte (`scripts/reproduce_m3.ps1`, 6 of 6 checks).
2. **The rules behave exactly as specified.** All 780 synthetic cases produced the rule IDs their labels expect.
3. **The model's behavior differs from the rules in specific, observable ways.** It scores shapes continuously rather than at a fixed cutoff, is more sensitive to many inputs than many outputs, cannot see values beyond its training range (fee rates above 100 sat/vB look the same), and treats large total output values as unusual.
4. **A threshold set on one window did not carry over to the next.** The model's flag rate rose 2.6x between calibration and test, and the rules' rose 3.4x, so the later window differs; the data cannot separate drift from one unusual day.
5. **The model failed three of four promotion criteria** (test alert rate 2.62x the budget against a limit of 2.0x; worst benign control 97.5% against 10%; rule-scope coverage 82.6% against 90%) and passed determinism.

## What the evidence does not support

- Any statement of accuracy, precision, recall, or false-positive rate on real traffic. There are no independent labels.
- That the model's extra flags are more interesting than the rules' flags. Of its 68 test-only flags, 36 were driven by `round_amount_fraction` and looked like ordinary payments.
- That either detector finds fraud, or that a flagged transaction is anything other than structurally unusual.
- That the sweep's "benign" controls represent real benign traffic; they are patterns we chose to generate.
- Stable thresholds. Calibration and test are each one short window.
- Anything about times of day or weekdays not sampled (sessions were early morning or evening UTC only, and none on a weekend).

## Decision and why

The rules remain the champion. The promotion criteria were written on 2026-10-14 after seeing the first evaluation and sweep, so they record a decision, not a pre-registered test; they are still reasonable bars (stay near the stated alert budget, don't flag ordinary behavior, catch what the rules define as in scope). A model that can't clear them has no case for replacing a transparent rule set that can be read and explained.

The plan expected this outcome as acceptable: a measured negative result is useful. It also leaves the model worth keeping as a challenger, since it graded risk where the rules are binary.

MLflow was not adopted. A local append-only registry with the same operations (register, promote with a gate, roll back) covers the present need without another service. Reconsider when there are several models or more than one person.

## What would change the decision

- More sessions across hours and weekdays, and a manifest rebuild, retraining, recalibration, and a fresh sweep.
- A deliberate alert budget instead of the 25-per-1,000 placeholder (that is about 750 alerts an hour on this traffic).
- Feature changes aimed at the observed gaps, such as a fee feature that can see extreme rates, and an explicit treatment of large totals.
- Independent labels, even a small reviewed sample, so that agreement can become a measurement.
- A simple robust-statistics baseline as a third comparator, which the plan lists and this milestone did not build.

## Risks to keep in view

- The environment reuses system packages with a SciPy/NumPy version mismatch and a joblib deprecation warning. Everything runs and reproduces, but a clean pinned virtual environment is worth doing before the M4 release.
- Capture gaps are recorded as a lower bound; some transactions were never observed.
- Public-provider data is one provider's view of the mempool, not the whole network.
