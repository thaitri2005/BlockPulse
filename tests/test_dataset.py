import json

import pytest

from blockpulse.dataset import build_manifest, choose_cuts, load_split, write_manifest

FEATURES = {"n_in": 1, "n_out": 2}


def make_session(root, name, start_minute, txids, mode="ws", version="transaction-v2", flagged=()):
    folder = root / name
    folder.mkdir(parents=True)
    with (folder / "screened.jsonl").open("w", encoding="utf-8") as out:
        for k, txid in enumerate(txids):
            out.write(json.dumps({
                "txid": txid, "event_id": f"e-{txid}", "received_at": f"2026-10-09T02:{start_minute:02d}:{k % 60:02d}.000+00:00",
                "source_mode": mode, "feature_set_version": version, "features": FEATURES,
                "ruleset_version": "structural-rules-v1", "signal_count": int(txid in flagged), "is_flagged": txid in flagged,
                "signals": [],
            }) + "\n")
    (folder / "summary.json").write_text(json.dumps({"source_file": str(folder / "missing" / "features.jsonl")}), encoding="utf-8")


def ids(prefix, n):
    return [f"{prefix}{i:03d}" for i in range(n)]


def test_choose_cuts_matches_targets_and_is_deterministic():
    assert choose_cuts([100, 100, 100, 100, 100]) == (3, 4)  # 60/20/20
    assert choose_cuts([2732, 2203, 3251, 5021, 2308]) == (3, 4)
    assert choose_cuts([1, 1, 1]) == (1, 2)
    with pytest.raises(ValueError, match="At least 3"):
        choose_cuts([10, 10])


def test_manifest_is_chronological_excludes_bad_sessions_and_is_reproducible(tmp_path):
    root = tmp_path / "screened"
    # Sessions are created out of time order on purpose; the manifest must sort them.
    make_session(root, "s-late", 50, ids("e", 120))
    make_session(root, "s-a", 10, ids("a", 120))
    make_session(root, "s-b", 20, ids("b", 120))
    make_session(root, "s-c", 30, ids("c", 120))
    make_session(root, "s-d", 40, ids("d", 120), flagged={"d001"})
    make_session(root, "demo", 5, ids("x", 200), mode="synthetic")
    make_session(root, "tiny", 6, ids("t", 5))
    make_session(root, "old-features", 7, ids("o", 200), version="transaction-v1")

    manifest = build_manifest(root)
    assert [s["run_id"] for s in manifest["sessions"]] == ["s-a", "s-b", "s-c", "s-d", "s-late"]
    assert [s["split"] for s in manifest["sessions"]] == ["train", "train", "train", "calibration", "test"]
    reasons = {e["run_id"]: e["reason"] for e in manifest["excluded"]}
    assert set(reasons) == {"demo", "tiny", "old-features"}
    assert "not live" in reasons["demo"] and "fewer than" in reasons["tiny"] and "transaction-v1" in reasons["old-features"]
    assert manifest["splits"]["calibration"]["flagged_rows"] == 1
    assert build_manifest(root)["manifest_id"] == manifest["manifest_id"]

    path = tmp_path / "m.json"
    assert write_manifest(manifest, path) is True
    assert write_manifest(manifest, path) is False  # identical: no-op
    (root / "s-a" / "screened.jsonl").write_text("", encoding="utf-8")  # data change
    with pytest.raises(ValueError, match="different content"):
        write_manifest({**manifest, "manifest_id": "other"}, path)


def test_load_split_verifies_checksums_and_drops_cross_session_duplicates(tmp_path):
    root = tmp_path / "screened"
    make_session(root, "s1", 10, ids("a", 100))
    make_session(root, "s2", 20, ids("a", 20) + ids("b", 100))  # first 20 txids repeat s1's
    make_session(root, "s3", 30, ids("c", 100))
    manifest = build_manifest(root)
    assert manifest["sessions"][1]["txids_already_seen_in_earlier_sessions"] == 20
    assert len(load_split(manifest, "calibration", root)) == 100  # duplicates removed
    assert len(load_split(manifest, "train", root)) == 100
    (root / "s3" / "screened.jsonl").write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="has changed"):
        load_split(manifest, "test", root)
