"""Calibrate an alert threshold on the calibration split, score the test split, compare with the rules.

There are no independent labels. Everything here describes alert volume and agreement between two
detectors; none of it measures whether either detector is right.
"""

import csv
import hashlib
import json
from pathlib import Path

from blockpulse.dataset import load_split
from blockpulse.model import MODEL_COLUMNS, feature_matrix, load_model, unusualness

EVALUATION_SCHEMA = 1


def choose_threshold(scores, alerts_per_1000: float) -> dict:
    """Threshold such that `score > threshold` flags the top k = floor(rate * n) calibration rows.

    With tied scores at the boundary the count can be lower than k; the actual count is reported.
    """
    import numpy as np
    if not 0 < alerts_per_1000 < 1000:
        raise ValueError("alerts_per_1000 must be between 0 and 1000")
    ordered = np.sort(np.asarray(scores))[::-1]
    k = int(alerts_per_1000 / 1000 * len(ordered))
    if k < 1:
        raise ValueError(f"A budget of {alerts_per_1000} per 1,000 flags nothing among {len(ordered)} calibration rows")
    threshold = float(ordered[k])
    return {"threshold": threshold, "target_flags": k, "actual_flags": int((ordered > threshold).sum())}


def most_unusual_features(vector, reference, limit: int = 3) -> list[dict]:
    """The features whose value sits furthest into a tail of the *training* distribution.

    Descriptive context only: it is not a causal explanation and not a formal attribution of the forest.
    """
    import numpy as np
    found = []
    for index, name in enumerate(MODEL_COLUMNS):
        column = reference[:, index]
        percentile = (np.sum(column < vector[index]) + 0.5 * np.sum(column == vector[index])) / len(column)
        found.append({"feature": name, "training_percentile": round(float(percentile), 4),
                      "extremeness": round(float(abs(percentile - 0.5) * 2), 4)})
    found.sort(key=lambda item: (-item["extremeness"], item["feature"]))
    return found[:limit]


def _session_hours(manifest: dict, split: str) -> float | None:
    seconds = 0.0
    for session in manifest["sessions"]:
        if session["split"] == split:
            elapsed = (session.get("capture") or {}).get("elapsed_seconds")
            if elapsed is None:
                return None
            seconds += elapsed
    return seconds / 3600 if seconds else None


def _overlap(model_flags, rule_flags) -> dict:
    both = sum(m and r for m, r in zip(model_flags, rule_flags))
    model_only = sum(m and not r for m, r in zip(model_flags, rule_flags))
    rules_only = sum(r and not m for m, r in zip(model_flags, rule_flags))
    neither = len(model_flags) - both - model_only - rules_only
    union = both + model_only + rules_only
    return {"both": both, "model_only": model_only, "rules_only": rules_only, "neither": neither,
            "jaccard": round(both / union, 4) if union else None}


def evaluate_detector(manifest: dict, root: Path, model_dir: Path, output: Path, *, alerts_per_1000: float = 25.0,
                      top_examples: int = 25) -> dict:
    import numpy as np
    model, metadata = load_model(model_dir)
    if metadata["dataset_manifest_id"] != manifest["manifest_id"]:
        raise ValueError("This model was trained on a different dataset manifest; build and train again")
    output.mkdir(parents=True, exist_ok=False)

    train_matrix, _, _ = feature_matrix(load_split(manifest, "train", root))
    results, tables = {}, {}
    for split in ("calibration", "test"):
        rows = load_split(manifest, split, root)
        matrix, kept, skipped = feature_matrix(rows)
        tables[split] = (rows, matrix, kept, unusualness(model, matrix))
        results[split] = {"rows": len(rows), "scored": len(kept), "skipped_missing_values": skipped}

    calibration = choose_threshold(tables["calibration"][3], alerts_per_1000)
    threshold = calibration["threshold"]
    detector_id = hashlib.sha256(json.dumps(
        [metadata["model_sha256"], threshold, alerts_per_1000, manifest["manifest_id"]]).encode("utf-8")).hexdigest()[:12]

    flagged_examples = []
    for split in ("calibration", "test"):
        rows, matrix, kept, scores = tables[split]
        model_flags = [bool(s > threshold) for s in scores]
        rule_flags = [bool(rows[i]["is_flagged"]) for i in kept]
        hours = _session_hours(manifest, split)
        results[split].update({
            "model_flagged": sum(model_flags), "rule_flagged": sum(rule_flags),
            "model_alerts_per_1000": round(1000 * sum(model_flags) / len(kept), 2),
            "rule_alerts_per_1000": round(1000 * sum(rule_flags) / len(kept), 2),
            "model_alerts_per_hour": round(sum(model_flags) / hours, 1) if hours else None,
            "capture_hours": round(hours, 4) if hours else None,
            "agreement": _overlap(model_flags, rule_flags),
            "score_percentiles": {f"p{p}": round(float(np.percentile(scores, p)), 4) for p in (50, 90, 99, 100)},
        })
        if split == "test":
            order = np.argsort(-scores)
            for position in order[:top_examples]:
                row = rows[kept[position]]
                flagged_examples.append({
                    "rank": len(flagged_examples) + 1, "txid": row["txid"], "score": round(float(scores[position]), 6),
                    "model_flag": model_flags[position], "rule_flag": rule_flags[position],
                    "rule_ids": ";".join(s["rule_id"] for s in row["signals"]),
                    "n_in": row["features"]["n_in"], "n_out": row["features"]["n_out"],
                    "vsize": row["features"]["vsize"], "fee_rate_sat_vb": row["features"]["fee_rate_sat_vb"],
                    "most_unusual_features": ";".join(
                        f"{f['feature']}@p{round(f['training_percentile'] * 100)}"
                        for f in most_unusual_features(matrix[position], train_matrix)),
                })
    summary = {
        "schema_version": EVALUATION_SCHEMA, "detector_id": detector_id, "dataset_manifest_id": manifest["manifest_id"],
        "model_sha256": metadata["model_sha256"], "feature_set_version": metadata["feature_set_version"],
        "alert_budget_per_1000": alerts_per_1000,
        "threshold": {**calibration, "calibrated_on": manifest["splits"]["calibration"]["sessions"],
                      "rule": "flag when score > threshold; threshold is the calibration score ranked just below the budget"},
        "calibration": results["calibration"], "test": results["test"],
        "test_sessions": manifest["splits"]["test"]["sessions"],
        "interpretation": [
            "There are no independent labels. 'agreement' counts how often the model and rules flag the same rows; it is not accuracy.",
            "The calibration split sets the model's flag rate by construction. The test flag rate shows how that carries to a later window.",
            "Calibration and test are each a single short capture window, so the numbers are a pipeline check and will move with more data.",
        ],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with (output / "test_top_scores.csv").open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(flagged_examples[0]))
        writer.writeheader()
        writer.writerows(flagged_examples)
    return summary
