"""Detector bundles and a local, append-only registry with promotion gates and rollback.

A *bundle* is a self-describing detector: schema, threshold, reference statistics, and checksums of the
evidence behind it. The *registry* records which bundles exist and which one is the champion. Every change
is an appended event, so the history can be audited and a rollback is just another event.
"""

import csv
from dataclasses import asdict
import hashlib
import json
import shutil
from pathlib import Path

from blockpulse.detector import DetectorConfig, RULESET_VERSION, screen_transaction
from blockpulse.features import FEATURE_SET_VERSION
from blockpulse.storage import utc_now

BUNDLE_SCHEMA = 1
RULES_FAMILY = "structural-rules-v1"
MODEL_FAMILY = "isolation-forest-v1"
REFERENCE_GRID_POINTS = 101

# Promotion criteria for a candidate that has to beat the champion's practical behavior. These were written on
# 2026-10-14, after the first evaluation and sweep were seen, so they record a decision rather than a pre-registered test.
PROMOTION_CRITERIA = {
    "max_test_rate_over_budget": 2.0,      # test alert rate may be at most this multiple of the calibration budget
    "max_benign_control_family_rate": 0.10,  # no benign-control family may be flagged more often than this
    "min_rule_scope_flag_rate": 0.90,      # must flag at least this share of synthetic patterns the rules are defined to catch
}


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


# ---------------------------------------------------------------- bundles

def package_rules(output_root: Path, config: DetectorConfig | None = None) -> Path:
    """Write a bundle for the rule baseline. Its ID is a hash of the rules and thresholds."""
    config = config or DetectorConfig()
    detector_id = _sha256_bytes(json.dumps([RULES_FAMILY, RULESET_VERSION, asdict(config)], sort_keys=True).encode())[:12]
    directory = output_root / detector_id
    directory.mkdir(parents=True, exist_ok=False)
    bundle = {
        "schema_version": BUNDLE_SCHEMA, "detector_id": detector_id, "family": RULES_FAMILY,
        "ruleset_version": RULESET_VERSION, "feature_set_version": FEATURE_SET_VERSION, "config": asdict(config),
        "description": "Transparent structural rules; thresholds are provisional and uncalibrated.",
    }
    (directory / "bundle.json").write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return directory


def _evidence(evaluation: dict, sweep_dir: Path) -> dict:
    with (sweep_dir / "cases.csv").open(encoding="utf-8", newline="") as handle:
        cases = list(csv.DictReader(handle))
    families: dict[str, list[bool]] = {}
    scope = []
    for case in cases:
        flagged = case["model_flag"] == "True"
        if case["kind"] == "benign_control":
            families.setdefault(case["family"], []).append(flagged)
        elif case["expected_rule_ids"]:
            scope.append(flagged)
    return {
        "alert_budget_per_1000": evaluation["alert_budget_per_1000"],
        "test_model_alerts_per_1000": evaluation["test"]["model_alerts_per_1000"],
        "benign_control_flag_rates": {name: round(sum(v) / len(v), 3) for name, v in sorted(families.items())},
        "rule_scope_flag_rate": round(sum(scope) / len(scope), 3), "rule_scope_cases": len(scope),
    }


def package_model(manifest: dict, screened_root: Path, model_dir: Path, evaluation_dir: Path, sweep_dir: Path,
                  output_root: Path) -> Path:
    """Bundle a trained model with its threshold, training reference percentiles, and evidence checksums."""
    import numpy as np
    from blockpulse.dataset import load_split
    from blockpulse.model import MODEL_COLUMNS, feature_matrix, load_model

    model, metadata = load_model(model_dir)
    evaluation = json.loads((evaluation_dir / "summary.json").read_text(encoding="utf-8"))
    sweep = json.loads((sweep_dir / "summary.json").read_text(encoding="utf-8"))
    if not (metadata["dataset_manifest_id"] == evaluation["dataset_manifest_id"] == sweep["dataset_manifest_id"] == manifest["manifest_id"]
            and evaluation["model_sha256"] == metadata["model_sha256"] and sweep["detector_id"] == evaluation["detector_id"]):
        raise ValueError("model, evaluation, sweep, and manifest do not belong together")
    detector_id = evaluation["detector_id"]
    directory = output_root / detector_id
    directory.mkdir(parents=True, exist_ok=False)

    train, _, _ = feature_matrix(load_split(manifest, "train", screened_root))
    grid = np.linspace(0, 1, REFERENCE_GRID_POINTS)
    shutil.copyfile(model_dir / "model.joblib", directory / "model.joblib")
    bundle = {
        "schema_version": BUNDLE_SCHEMA, "detector_id": detector_id, "family": MODEL_FAMILY,
        "feature_set_version": metadata["feature_set_version"], "model_columns": metadata["model_columns"],
        "transforms": metadata["transforms"], "missing_value_policy": metadata["missing_value_policy"],
        "score_direction": metadata["score_direction"], "model_sha256": metadata["model_sha256"],
        "threshold": evaluation["threshold"]["threshold"], "alert_budget_per_1000": evaluation["alert_budget_per_1000"],
        "calibrated_on": evaluation["threshold"]["calibrated_on"],
        "dataset_manifest_id": manifest["manifest_id"], "train_sessions": metadata["train_sessions"],
        "training": metadata["training"], "parameters": metadata["parameters"], "seed": metadata["seed"],
        "deterministic_refit_identical": metadata["deterministic_refit_identical"], "environment": metadata["environment"],
        "reference_quantile_grid": {name: [round(float(v), 6) for v in np.quantile(train[:, i], grid)]
                                    for i, name in enumerate(MODEL_COLUMNS)},
        "evidence": _evidence(evaluation, sweep_dir),
        "evidence_sha256": {"evaluation_summary": _sha256_file(evaluation_dir / "summary.json"),
                            "sweep_summary": _sha256_file(sweep_dir / "summary.json"),
                            "sweep_cases": _sha256_file(sweep_dir / "cases.csv")},
    }
    (directory / "bundle.json").write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return directory


def load_bundle(directory: Path) -> dict:
    """Read a bundle and check it is compatible with this code. Raises ValueError rather than guessing."""
    bundle = json.loads((directory / "bundle.json").read_text(encoding="utf-8"))
    if bundle["schema_version"] != BUNDLE_SCHEMA:
        raise ValueError(f"unsupported bundle schema {bundle['schema_version']}")
    if bundle["feature_set_version"] != FEATURE_SET_VERSION:
        raise ValueError(f"bundle expects feature set {bundle['feature_set_version']}; code uses {FEATURE_SET_VERSION}")
    if bundle["family"] == RULES_FAMILY:
        if bundle["ruleset_version"] != RULESET_VERSION:
            raise ValueError("bundle was made for a different ruleset version")
        DetectorConfig(**bundle["config"])  # validates the thresholds
    elif bundle["family"] == MODEL_FAMILY:
        from blockpulse.model import MODEL_COLUMNS
        if bundle["model_columns"] != list(MODEL_COLUMNS):
            raise ValueError("bundle model columns differ from this code's feature columns")
        if _sha256_file(directory / "model.joblib") != bundle["model_sha256"]:
            raise ValueError("model.joblib does not match the checksum in bundle.json")
    else:
        raise ValueError(f"unknown detector family {bundle['family']!r}")
    return bundle


def score_rows(directory: Path, rows: list[dict]) -> list[dict]:
    """Apply a bundle to screened-style rows ({'features': {...}}). Returns [{'flag': bool, 'score': float | None}]."""
    bundle = load_bundle(directory)
    if bundle["family"] == RULES_FAMILY:
        config = DetectorConfig(**bundle["config"])
        return [{"flag": screen_transaction(row["features"], config)["is_flagged"], "score": None} for row in rows]
    import joblib
    from blockpulse.model import feature_matrix, unusualness
    matrix, kept, _ = feature_matrix(rows)
    scores = unusualness(joblib.load(directory / "model.joblib"), matrix)
    results = [{"flag": None, "score": None} for _ in rows]  # None: row had missing values and cannot be scored
    for index, score in zip(kept, scores):
        results[index] = {"flag": bool(score > bundle["threshold"]), "score": float(score)}
    return results


# ---------------------------------------------------------------- registry

def _registry_path(root: Path) -> Path:
    return root / "registry.jsonl"


def read_events(root: Path) -> list[dict]:
    path = _registry_path(root)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _append(root: Path, event: dict) -> dict:
    event = {"at": utc_now(), **event}
    with _registry_path(root).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return event


def state(events: list[dict]) -> dict:
    """Replay the events: registered bundles, the current champion, and the stack rollback would walk back through."""
    registered, champion, previous = {}, None, []
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
    return {"registered": registered, "champion": champion, "previous_champions": previous}


def register(root: Path, directory: Path) -> dict:
    """Record a bundle in the registry (validating it first). Registering again is a no-op."""
    bundle = load_bundle(directory)
    current = state(read_events(root))
    digest = _sha256_file(directory / "bundle.json")
    existing = current["registered"].get(bundle["detector_id"])
    if existing:
        if existing["bundle_sha256"] != digest:
            raise ValueError("a different bundle is already registered under this detector ID")
        return existing
    return _append(root, {"type": "registered", "detector_id": bundle["detector_id"], "family": bundle["family"],
                          "directory": directory.name, "bundle_sha256": digest})


def evaluate_promotion(bundle: dict) -> dict:
    """Check a model candidate against PROMOTION_CRITERIA using the evidence packaged inside its bundle."""
    if bundle["family"] == RULES_FAMILY:
        return {"passed": True, "criteria": [], "note": "baseline rules have no promotion gate"}
    evidence, limits = bundle["evidence"], PROMOTION_CRITERIA
    ratio = evidence["test_model_alerts_per_1000"] / evidence["alert_budget_per_1000"]
    worst_name, worst_rate = max(evidence["benign_control_flag_rates"].items(), key=lambda item: item[1])
    criteria = [
        {"name": "deterministic_refit", "value": bundle["deterministic_refit_identical"], "requirement": "true",
         "passed": bool(bundle["deterministic_refit_identical"])},
        {"name": "test_alert_rate_vs_budget", "value": round(ratio, 2),
         "requirement": f"<= {limits['max_test_rate_over_budget']}x the calibration budget", "passed": ratio <= limits["max_test_rate_over_budget"]},
        {"name": "worst_benign_control_flag_rate", "value": worst_rate, "detail": worst_name,
         "requirement": f"<= {limits['max_benign_control_family_rate']}", "passed": worst_rate <= limits["max_benign_control_family_rate"]},
        {"name": "rule_scope_flag_rate", "value": evidence["rule_scope_flag_rate"],
         "requirement": f">= {limits['min_rule_scope_flag_rate']}", "passed": evidence["rule_scope_flag_rate"] >= limits["min_rule_scope_flag_rate"]},
    ]
    return {"passed": all(c["passed"] for c in criteria), "criteria": criteria}


def promote(root: Path, detector_id: str, reason: str) -> dict:
    """Make a registered bundle the champion if it passes its gate; otherwise record the rejection."""
    if not reason.strip():
        raise ValueError("a reason is required")
    current = state(read_events(root))
    registered = current["registered"].get(detector_id)
    if registered is None:
        raise ValueError(f"{detector_id} is not registered")
    directory = root / registered["directory"]
    if _sha256_file(directory / "bundle.json") != registered["bundle_sha256"]:
        raise ValueError("bundle.json has changed since registration")
    bundle = load_bundle(directory)
    gate = evaluate_promotion(bundle)
    if not gate["passed"]:
        failed = [c["name"] for c in gate["criteria"] if not c["passed"]]
        _append(root, {"type": "promotion_rejected", "detector_id": detector_id, "reason": reason, "failed_criteria": failed,
                       "criteria": gate["criteria"]})
        return {"promoted": False, "failed_criteria": failed, "criteria": gate["criteria"], "champion": current["champion"]}
    if current["champion"] == detector_id:
        return {"promoted": False, "champion": detector_id, "note": "already the champion"}
    _append(root, {"type": "promoted", "detector_id": detector_id, "reason": reason, "replaced": current["champion"],
                   "criteria": gate["criteria"]})
    return {"promoted": True, "champion": detector_id, "replaced": current["champion"]}


def rollback(root: Path, reason: str) -> dict:
    """Return to the previous champion, after re-validating its bundle."""
    if not reason.strip():
        raise ValueError("a reason is required")
    current = state(read_events(root))
    if not current["previous_champions"]:
        raise ValueError("there is no earlier champion to roll back to")
    target = current["previous_champions"][-1]
    load_bundle(root / current["registered"][target]["directory"])  # an invalid target leaves the current champion in place
    _append(root, {"type": "rolled_back", "detector_id": target, "from": current["champion"], "reason": reason})
    return {"champion": target, "rolled_back_from": current["champion"]}


def champion_directory(root: Path) -> Path:
    """The active detector's bundle directory. Fails loudly instead of substituting another detector."""
    current = state(read_events(root))
    if current["champion"] is None:
        raise ValueError("no champion is set; promote a detector first")
    directory = root / current["registered"][current["champion"]]["directory"]
    load_bundle(directory)
    return directory
