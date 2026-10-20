"""Labeled synthetic sweep: how do the rules and the model respond to shapes where the expected answer is defined?

Everything here is synthetic and labeled as such. Transactions are generated with consistent values (inputs
equal outputs plus fee), then run through the same feature function as real data. Results describe detector
behavior on these patterns only; they say nothing about real-world detection accuracy or false-positive rates.
"""

import csv
import hashlib
import json
import random
from pathlib import Path

from blockpulse.dataset import load_split
from blockpulse.detector import DetectorConfig, screen_transaction
from blockpulse.features import extract_features
from blockpulse.model import feature_matrix, load_model, unusualness

SWEEP_SCHEMA = 1
VARIANTS = 20
FAN_SIZES = (3, 5, 8, 10, 15, 20, 50, 100, 300)
EQUAL_COUNTS = (2, 3, 5, 10, 20, 50)
FEE_RATES = (1, 5, 20, 100, 500, 2000)
WHALE_VALUES = (10**8, 10**9, 10**10, 10**11)


def _vsize(n_in: int, n_out: int) -> int:
    """Approximate P2WPKH transaction size in virtual bytes."""
    return round(10.5 + 68 * n_in + 31 * n_out)


def _build(rng: random.Random, n_in: int, outputs: list[int], fee_rate: float) -> dict:
    """A transaction whose inputs exactly cover outputs plus fee."""
    vsize = _vsize(n_in, len(outputs))
    fee = max(1, round(fee_rate * vsize))
    total = sum(outputs) + fee
    cuts = sorted(rng.sample(range(1, total), n_in - 1)) if n_in > 1 else []  # distinct, so every part is positive
    parts = [b - a for a, b in zip([0] + cuts, cuts + [total])]
    rbf = rng.random() < 0.68  # roughly the share observed in real captures
    return {
        "vin": [{"txid": f"{rng.getrandbits(256):064x}", "vout": 0, "prevout": {"value": v},
                 "sequence": 0xFFFFFFFD if rbf else 0xFFFFFFFF} for v in parts],
        "vout": [{"value": value} for value in outputs],
        "weight": vsize * 4, "fee": fee,
    }


def _payment_values(rng: random.Random, n: int, low=2_000, high=5_000_000) -> list[int]:
    """Distinct, non-round values spread over a typical payment range (log-uniform)."""
    values = set()
    while len(values) < n:
        values.add(int(low * (high / low) ** rng.random()) // 7 * 7 + 3)
    return list(values)


def cases(seed: int) -> list[dict]:
    """Every synthetic case with its label. Deterministic for a given seed."""
    out = []

    def add(family, kind, severity, variant, expected_rules, n_in, outputs, fee_rate, rng):
        out.append({"family": family, "kind": kind, "severity": severity, "variant": variant,
                    "expected_rule_ids": expected_rules, "transaction": _build(rng, n_in, outputs, fee_rate)})

    for variant in range(VARIANTS):
        rng = random.Random(f"{seed}-{variant}")
        for n in FAN_SIZES:
            add("fan_out", "target", n, variant, ["many_outputs"] if n >= 10 else [], 1, _payment_values(rng, n), 2.0 + rng.random() * 3, rng)
            add("fan_in", "target", n, variant, ["many_inputs"] if n >= 10 else [], n, _payment_values(rng, 1), 2.0 + rng.random() * 3, rng)
        for k in EQUAL_COUNTS:  # k equal-value outputs plus one change output; 3 inputs
            amount = _payment_values(rng, 1)[0]
            change = _payment_values(rng, 1)
            add("equal_outputs", "target", k, variant, ["repeated_output_values"] if k >= 3 else [], 3,
                [amount] * k + change, 2.0 + rng.random() * 3, rng)
        for rate in FEE_RATES:
            add("fee_outlier", "target", rate, variant, [], 1, _payment_values(rng, 2), float(rate), rng)
        # Benign controls: ordinary behaviour the detectors should not need to flag.
        add("ordinary_payment", "benign_control", 0, variant, [], rng.choice((1, 1, 2)), _payment_values(rng, 2), 1.0 + rng.random() * 4, rng)
        add("round_amount_payment", "benign_control", 0, variant, [], 1, [rng.choice((50_000, 100_000, 500_000, 1_000_000)), _payment_values(rng, 1)[0]], 2.0 + rng.random() * 3, rng)  # noqa: E501
        add("high_fee_payment", "benign_control", 0, variant, [], 1, _payment_values(rng, 2), 30.0 + rng.random() * 70, rng)
        add("small_batch", "benign_control", 0, variant, [], 2, _payment_values(rng, 6), 2.0 + rng.random() * 3, rng)
        add("small_consolidation", "benign_control", 0, variant, [], 4, _payment_values(rng, 1), 2.0 + rng.random() * 3, rng)
        for value in WHALE_VALUES:
            add("large_payment", "benign_control", value, variant, [], 1, [value, _payment_values(rng, 1)[0]], 2.0 + rng.random() * 3, rng)
    return out


def run_sweep(manifest: dict, root: Path, model_dir: Path, evaluation_dir: Path, output: Path, *, seed: int = 11) -> dict:
    """Score the synthetic cases with the rules and with the calibrated model, and summarize by family and severity."""
    import numpy as np
    evaluation = json.loads((evaluation_dir / "summary.json").read_text(encoding="utf-8"))
    model, metadata = load_model(model_dir)
    if not (evaluation["model_sha256"] == metadata["model_sha256"] and evaluation["dataset_manifest_id"] == manifest["manifest_id"]):
        raise ValueError("evaluation, model, and manifest do not belong together")
    threshold = evaluation["threshold"]["threshold"]
    output.mkdir(parents=True, exist_ok=False)

    calibration_scores = np.sort(unusualness(model, feature_matrix(load_split(manifest, "calibration", root))[0]))
    config = DetectorConfig()
    labeled = cases(seed)
    features = [extract_features(case["transaction"]) for case in labeled]
    matrix, kept, skipped = feature_matrix([{"features": f} for f in features])
    if skipped:
        raise ValueError("synthetic cases should never have missing features")
    scores = unusualness(model, matrix)
    percentiles = np.searchsorted(calibration_scores, scores, side="right") / len(calibration_scores)

    rows = []
    for case, feats, score, percentile in zip(labeled, features, scores, percentiles):
        screened = screen_transaction(feats, config)
        rule_ids = sorted(s["rule_id"] for s in screened["signals"])
        rows.append({
            "family": case["family"], "kind": case["kind"], "severity": case["severity"], "variant": case["variant"],
            "expected_rule_ids": ";".join(case["expected_rule_ids"]), "rule_ids": ";".join(rule_ids),
            "rules_match_expected": rule_ids == sorted(case["expected_rule_ids"]),
            "rule_flag": screened["is_flagged"], "model_score": round(float(score), 6),
            "model_flag": bool(score > threshold), "calibration_percentile": round(float(percentile), 4),
            **{name: feats[name] for name in ("n_in", "n_out", "vsize", "fee_rate_sat_vb", "max_equal_output_count", "round_amount_fraction")},
        })

    groups = {}
    for row in rows:
        groups.setdefault((row["family"], row["kind"], row["severity"]), []).append(row)
    table = []
    for (family, kind, severity), members in groups.items():
        n = len(members)
        table.append({
            "family": family, "kind": kind, "severity": severity, "cases": n,
            "rule_flag_rate": round(sum(m["rule_flag"] for m in members) / n, 3),
            "model_flag_rate": round(sum(m["model_flag"] for m in members) / n, 3),
            "median_model_score": round(float(np.median([m["model_score"] for m in members])), 4),
            "median_calibration_percentile": round(float(np.median([m["calibration_percentile"] for m in members])), 4),
            "rules_match_expected_rate": round(sum(m["rules_match_expected"] for m in members) / n, 3),
        })
    with (output / "cases.csv").open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with (output / "sweep.csv").open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(table[0]))
        writer.writeheader()
        writer.writerows(table)

    def overall(kind):
        members = [r for r in rows if r["kind"] == kind]
        return {"cases": len(members), "rule_flag_rate": round(sum(r["rule_flag"] for r in members) / len(members), 3),
                "model_flag_rate": round(sum(r["model_flag"] for r in members) / len(members), 3)}

    summary = {
        "schema_version": SWEEP_SCHEMA, "seed": seed, "variants_per_cell": VARIANTS, "detector_id": evaluation["detector_id"],
        "dataset_manifest_id": manifest["manifest_id"], "threshold": threshold,
        "alert_budget_per_1000": evaluation["alert_budget_per_1000"], "total_cases": len(rows),
        "rules_match_expected_cases": sum(r["rules_match_expected"] for r in rows),
        "target": overall("target"), "benign_control": overall("benign_control"),
        "cases_sha256": hashlib.sha256((output / "cases.csv").read_bytes()).hexdigest(),
        "purpose": ("Behavior of the rules and the model on labeled synthetic patterns. Not real-world accuracy: the patterns "
                    "are generated by this project, the controls are not real traffic, and structure alone cannot separate "
                    "a benign batch or consolidation from any other transaction with the same shape."),
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary
