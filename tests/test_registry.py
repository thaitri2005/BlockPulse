import json

import pytest

from blockpulse import registry
from blockpulse.detector import DetectorConfig


def rules_bundle(root, **config):
    return registry.package_rules(root, DetectorConfig(**config))


def test_promote_replace_and_rollback_walk_the_history(tmp_path):
    a = rules_bundle(tmp_path)
    b = rules_bundle(tmp_path, many_outputs_min=20)
    for directory in (a, b):
        registry.register(tmp_path, directory)
    assert registry.register(tmp_path, a)["detector_id"] == a.name  # registering twice is a no-op
    assert len(registry.read_events(tmp_path)) == 2

    with pytest.raises(ValueError, match="no champion"):
        registry.champion_directory(tmp_path)
    assert registry.promote(tmp_path, a.name, "initial baseline")["promoted"] is True
    second = registry.promote(tmp_path, b.name, "stricter fan-out")
    assert second["promoted"] and second["replaced"] == a.name
    assert registry.champion_directory(tmp_path) == b

    assert registry.rollback(tmp_path, "b was too strict")["champion"] == a.name
    assert registry.state(registry.read_events(tmp_path))["champion"] == a.name
    with pytest.raises(ValueError, match="no earlier champion"):
        registry.rollback(tmp_path, "again")
    with pytest.raises(ValueError, match="reason"):
        registry.promote(tmp_path, b.name, "  ")


def test_tampered_or_incompatible_bundles_are_refused(tmp_path):
    directory = rules_bundle(tmp_path)
    registry.register(tmp_path, directory)
    path = directory / "bundle.json"
    original = path.read_text(encoding="utf-8")

    path.write_text(original.replace('"many_outputs_min": 10', '"many_outputs_min": 11'), encoding="utf-8")
    with pytest.raises(ValueError, match="changed since registration"):
        registry.promote(tmp_path, directory.name, "x")

    for field, value, message in (("feature_set_version", "transaction-v0", "feature set"), ("ruleset_version", "old", "ruleset"),
                                  ("family", "mystery", "unknown detector family")):
        bundle = json.loads(original)
        bundle[field] = value
        path.write_text(json.dumps(bundle), encoding="utf-8")
        with pytest.raises(ValueError, match=message):
            registry.load_bundle(directory)


def test_rollback_to_a_broken_bundle_keeps_the_current_champion(tmp_path):
    a, b = rules_bundle(tmp_path), rules_bundle(tmp_path, many_inputs_min=30)
    for directory in (a, b):
        registry.register(tmp_path, directory)
    registry.promote(tmp_path, a.name, "one")
    registry.promote(tmp_path, b.name, "two")
    (a / "bundle.json").write_text("{}", encoding="utf-8")
    with pytest.raises(KeyError):
        registry.rollback(tmp_path, "try")
    assert registry.state(registry.read_events(tmp_path))["champion"] == b.name


def model_bundle(**overrides):
    evidence = {"alert_budget_per_1000": 25.0, "test_model_alerts_per_1000": 30.0,
                "benign_control_flag_rates": {"a": 0.0, "b": 0.05}, "rule_scope_flag_rate": 0.95}
    evidence.update(overrides)
    return {"family": registry.MODEL_FAMILY, "deterministic_refit_identical": True, "evidence": evidence}


def failed(bundle):
    return [c["name"] for c in registry.evaluate_promotion(bundle)["criteria"] if not c["passed"]]


def test_promotion_gate_names_each_failed_criterion():
    assert registry.evaluate_promotion(model_bundle())["passed"] is True
    assert failed(model_bundle(test_model_alerts_per_1000=65.4)) == ["test_alert_rate_vs_budget"]
    assert failed(model_bundle(benign_control_flag_rates={"large_payment": 0.98})) == ["worst_benign_control_flag_rate"]
    assert failed(model_bundle(rule_scope_flag_rate=0.6)) == ["rule_scope_flag_rate"]
    assert registry.evaluate_promotion({**model_bundle(), "deterministic_refit_identical": False})["passed"] is False


def test_model_bundle_scores_like_the_evaluation_and_carries_its_evidence(tmp_path):
    pytest.importorskip("sklearn")
    from blockpulse.dataset import load_split
    from blockpulse.evaluate import evaluate_detector
    from blockpulse.model import train_isolation_forest
    from blockpulse.sweep import run_sweep
    from tests.test_model import make_manifest

    manifest, root = make_manifest(tmp_path)
    train_isolation_forest(manifest, root, tmp_path / "model", seed=3, n_estimators=50)
    evaluation = evaluate_detector(manifest, root, tmp_path / "model", tmp_path / "eval", alerts_per_1000=50)
    run_sweep(manifest, root, tmp_path / "model", tmp_path / "eval", tmp_path / "sweep", seed=2)
    detectors = tmp_path / "detectors"
    detectors.mkdir()
    directory = registry.package_model(manifest, root, tmp_path / "model", tmp_path / "eval", tmp_path / "sweep", detectors)
    assert directory.name == evaluation["detector_id"]

    bundle = registry.load_bundle(directory)
    grid = bundle["reference_quantile_grid"]["n_in"]
    assert len(grid) == registry.REFERENCE_GRID_POINTS and grid == sorted(grid)
    assert bundle["evidence"]["rule_scope_cases"] > 0

    results = registry.score_rows(directory, load_split(manifest, "test", root))
    assert sum(r["flag"] for r in results) == evaluation["test"]["model_flagged"]  # same decisions as the evaluation
    assert all(r["score"] is not None for r in results)

    registry.register(detectors, directory)
    outcome = registry.promote(detectors, directory.name, "try the model")
    assert "promoted" in outcome  # whether the toy data passes the gate is not the point; the outcome is recorded
    assert registry.read_events(detectors)[-1]["detector_id"] == directory.name

    (directory / "model.joblib").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        registry.load_bundle(directory)
