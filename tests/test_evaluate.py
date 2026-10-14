import json

import pytest

pytest.importorskip("sklearn")

from blockpulse.evaluate import choose_threshold, evaluate_detector, most_unusual_features
from blockpulse.model import MODEL_COLUMNS, train_isolation_forest
from tests.test_model import make_manifest


def test_choose_threshold_flags_the_budgeted_top_rows():
    scores = [i / 100 for i in range(100)]
    chosen = choose_threshold(scores, 50)  # 50 per 1,000 of 100 rows -> 5 flagged
    assert chosen["target_flags"] == 5 and chosen["actual_flags"] == 5
    assert sum(s > chosen["threshold"] for s in scores) == 5
    with pytest.raises(ValueError, match="flags nothing"):
        choose_threshold(scores, 1)
    with pytest.raises(ValueError):
        choose_threshold(scores, 0)


def test_most_unusual_features_picks_the_tail_value():
    import numpy as np
    reference = np.tile(np.linspace(0, 1, 101).reshape(-1, 1), (1, len(MODEL_COLUMNS)))
    vector = np.full(len(MODEL_COLUMNS), 0.5)
    vector[MODEL_COLUMNS.index("vsize")] = 5.0  # far above everything seen in training
    top = most_unusual_features(vector, reference)
    assert top[0]["feature"] == "vsize" and top[0]["training_percentile"] == 1.0 and len(top) == 3


def test_evaluation_counts_agree_and_refuse_wrong_manifest(tmp_path):
    manifest, root = make_manifest(tmp_path)
    train_isolation_forest(manifest, root, tmp_path / "model", seed=3, n_estimators=50)
    summary = evaluate_detector(manifest, root, tmp_path / "model", tmp_path / "eval", alerts_per_1000=50)
    cal, test = summary["calibration"], summary["test"]
    assert cal["scored"] == cal["rows"] == 300
    assert cal["model_flagged"] == 15  # floor(50/1000 * 300), scores are continuous so no ties
    a = test["agreement"]
    assert a["both"] + a["model_only"] + a["rules_only"] + a["neither"] == test["scored"]
    assert test["model_flagged"] == a["both"] + a["model_only"]
    assert (tmp_path / "eval" / "test_top_scores.csv").read_text().count("\n") == 26  # header + 25
    assert json.loads((tmp_path / "eval" / "summary.json").read_text())["detector_id"] == summary["detector_id"]

    again = evaluate_detector(manifest, root, tmp_path / "model", tmp_path / "eval2", alerts_per_1000=50)
    assert again == summary  # reproducible

    with pytest.raises(ValueError, match="different dataset manifest"):
        evaluate_detector({**manifest, "manifest_id": "other"}, root, tmp_path / "model", tmp_path / "eval3")
