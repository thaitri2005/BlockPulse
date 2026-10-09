import json

import pytest

from blockpulse.detect import detect_features
from blockpulse.features import FEATURE_NAMES, FEATURE_SET_VERSION, extract_features


def feature_row(txid, number, features):
    return {
        "schema_version": 1, "txid": txid, "event_id": f"event-{number}",
        "received_at": "2026-10-08T08:00:00.000+00:00", "source_mode": "synthetic",
        "feature_set_version": FEATURE_SET_VERSION, **features,
    }


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_detect_writes_explanations_and_replays_deterministically(tmp_path, transaction):
    base = extract_features(transaction)
    rows = [
        feature_row("a" * 64, 1, base),
        feature_row("c" * 64, 2, {**base, "n_in": 168}),
        feature_row("c" * 64, 3, {**base, "n_in": 168}),
    ]
    source = tmp_path / "features.jsonl"
    write_jsonl(source, rows)
    one, two = tmp_path / "one", tmp_path / "two"
    summary = detect_features(source, one)
    again = detect_features(source, two)
    assert summary == again
    assert summary["rows_read"] == 3
    assert summary["rows_screened"] == 2
    assert summary["flagged_rows"] == 1
    assert summary["signal_counts"] == {"many_inputs": 1}
    assert len((one / "screened.jsonl").read_text().splitlines()) == 2
    alerts = [json.loads(line) for line in (one / "alerts.jsonl").read_text().splitlines()]
    assert alerts[0]["signals"][0]["rule_id"] == "many_inputs"
    assert "168 inputs" in alerts[0]["signals"][0]["explanation"]
    assert (one / "screened.csv").read_bytes() == (two / "screened.csv").read_bytes()
    with pytest.raises(FileExistsError):
        detect_features(source, one)


def test_bad_and_stale_rows_are_diagnosed(tmp_path, transaction):
    base = extract_features(transaction)
    missing = {name: value for name, value in base.items() if name in FEATURE_NAMES[:-3]}
    rows = [
        {**feature_row("a" * 64, 1, base), "feature_set_version": "transaction-v1"},
        feature_row("b" * 64, 2, missing),
        feature_row("", 3, base),
    ]
    source = tmp_path / "features.jsonl"
    write_jsonl(source, rows)
    summary = detect_features(source, tmp_path / "out")
    assert summary["rows_screened"] == 0
    assert summary["issue_count"] == 3
    errors = [json.loads(line) for line in (tmp_path / "out" / "errors.jsonl").read_text().splitlines()]
    assert "Reprocess" in errors[0]["detail"]
