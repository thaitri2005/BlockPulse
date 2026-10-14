import json

import pytest

pytest.importorskip("sklearn")

from blockpulse.dataset import build_manifest
from blockpulse.model import MODEL_COLUMNS, feature_matrix, load_model, train_isolation_forest, unusualness
from tests.test_dataset import make_session


def row(**overrides):
    features = {"n_in": 1, "n_out": 2, "value_out_sat": 50_000, "fee_sat": 300, "vsize": 140, "fee_rate_sat_vb": 2.1,
                "max_equal_output_count": 1, "round_amount_fraction": 0.0, "rbf_signaled": True}
    features.update(overrides)
    return {"features": features}


def test_feature_matrix_transforms_orders_and_skips_missing():
    import numpy as np
    matrix, kept, skipped = feature_matrix([row(), row(rbf_signaled=None), row(n_in=399, rbf_signaled=False)])
    assert (kept, skipped) == ([0, 2], 1)
    assert matrix.shape == (2, len(MODEL_COLUMNS))
    assert matrix[0][MODEL_COLUMNS.index("n_in")] == pytest.approx(np.log1p(1))
    assert matrix[1][MODEL_COLUMNS.index("n_in")] == pytest.approx(np.log1p(399))
    assert matrix[0][MODEL_COLUMNS.index("rbf_signaled")] == 1.0 and matrix[1][MODEL_COLUMNS.index("rbf_signaled")] == 0.0


def make_manifest(tmp_path):
    root = tmp_path / "screened"
    import random
    rng = random.Random(0)
    for name, minute in (("s1", 10), ("s2", 20), ("s3", 30), ("s4", 40)):
        make_session(root, name, minute, [f"{name}-{i:04d}" for i in range(300)])
        path = root / name / "screened.jsonl"
        rows = [json.loads(line) for line in path.open()]
        for r in rows:  # vary the features so the forest has something to learn
            r["features"] = {"n_in": rng.randint(1, 3), "n_out": rng.randint(1, 4), "value_out_sat": rng.randint(1000, 10**7),
                             "fee_sat": rng.randint(100, 2000), "vsize": rng.randint(100, 400), "fee_rate_sat_vb": rng.random() * 10,
                             "max_equal_output_count": 1, "round_amount_fraction": 0.0, "rbf_signaled": rng.random() < 0.5}
        path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    return build_manifest(root), root


def test_train_saves_verifiable_deterministic_model_and_scores_outliers_higher(tmp_path):
    manifest, root = make_manifest(tmp_path)
    metadata = train_isolation_forest(manifest, root, tmp_path / "model", seed=3, n_estimators=50)
    assert metadata["dataset_manifest_id"] == manifest["manifest_id"]
    assert metadata["training"]["rows_used"] == manifest["splits"]["train"]["rows"]
    assert metadata["deterministic_refit_identical"] is True
    assert metadata["resources"]["fit_seconds"] > 0

    model, loaded = load_model(tmp_path / "model")
    ordinary, _, _ = feature_matrix([row(n_in=2, n_out=2, value_out_sat=5_000_000, fee_sat=1000, vsize=250, fee_rate_sat_vb=5.0)])
    odd, _, _ = feature_matrix([row(n_in=400, n_out=300, value_out_sat=10**11, fee_sat=10**5, vsize=90_000,
                                    fee_rate_sat_vb=1, max_equal_output_count=50, round_amount_fraction=1.0)])
    assert unusualness(model, odd)[0] > unusualness(model, ordinary)[0]

    with pytest.raises(FileExistsError):
        train_isolation_forest(manifest, root, tmp_path / "model")
    (tmp_path / "model" / "model.joblib").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        load_model(tmp_path / "model")


def test_too_little_training_data_is_refused(tmp_path):
    manifest, root = make_manifest(tmp_path)
    manifest["sessions"] = [s for s in manifest["sessions"]][:1] + manifest["sessions"][1:]
    for session in manifest["sessions"]:
        session["split"] = "calibration" if session["split"] == "train" else session["split"]
    with pytest.raises(ValueError, match="at least 100"):
        train_isolation_forest(manifest, root, tmp_path / "empty")
