import json

import pytest

pytest.importorskip("sklearn")

from blockpulse.evaluate import evaluate_detector
from blockpulse.features import extract_features
from blockpulse.model import train_isolation_forest
from blockpulse.sweep import VARIANTS, cases, run_sweep
from tests.test_model import make_manifest


def test_cases_are_deterministic_consistent_and_have_the_labeled_shape():
    first, second = cases(5), cases(5)
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)
    assert json.dumps(cases(6), sort_keys=True) != json.dumps(first, sort_keys=True)
    for case in first:
        tx = case["transaction"]
        inputs = sum(v["prevout"]["value"] for v in tx["vin"])
        assert inputs == sum(o["value"] for o in tx["vout"]) + tx["fee"]  # internally consistent
        assert all(v["prevout"]["value"] > 0 for v in tx["vin"])
        f = extract_features(tx)
        if case["family"] == "fan_out":
            assert (f["n_in"], f["n_out"]) == (1, case["severity"])
        if case["family"] == "fan_in":
            assert (f["n_in"], f["n_out"]) == (case["severity"], 1)
        if case["family"] == "equal_outputs":
            assert f["max_equal_output_count"] == case["severity"]
        if case["family"] == "fee_outlier":
            assert f["fee_rate_sat_vb"] == pytest.approx(case["severity"], rel=0.02)


def test_sweep_runs_and_rules_match_their_labels(tmp_path):
    manifest, root = make_manifest(tmp_path)
    train_isolation_forest(manifest, root, tmp_path / "model", seed=3, n_estimators=50)
    evaluate_detector(manifest, root, tmp_path / "model", tmp_path / "eval", alerts_per_1000=50)
    summary = run_sweep(manifest, root, tmp_path / "model", tmp_path / "eval", tmp_path / "sweep", seed=2)
    assert summary["total_cases"] == len(cases(2))
    assert summary["rules_match_expected_cases"] == summary["total_cases"]  # the rules do what the labels say
    assert summary["target"]["cases"] + summary["benign_control"]["cases"] == summary["total_cases"]
    rows = (tmp_path / "sweep" / "sweep.csv").read_text().splitlines()
    assert any(line.startswith("fan_out,target,100,") and f",{VARIANTS}," in line for line in rows)
    with pytest.raises(ValueError, match="do not belong together"):
        run_sweep({**manifest, "manifest_id": "other"}, root, tmp_path / "model", tmp_path / "eval", tmp_path / "sweep2")
