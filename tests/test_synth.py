import hashlib
import json

import pytest

from blockpulse.cli import main
from blockpulse.process import process_capture
from blockpulse.synth import generate_capture


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_same_seed_gives_byte_identical_captures_and_a_different_seed_does_not(tmp_path):
    first = generate_capture(tmp_path / "a", messages=40, seed=3)
    generate_capture(tmp_path / "b", messages=40, seed=3)
    generate_capture(tmp_path / "c", messages=40, seed=4)
    assert _sha(tmp_path / "a" / "messages.jsonl") == _sha(tmp_path / "b" / "messages.jsonl")
    assert _sha(tmp_path / "a" / "messages.jsonl") != _sha(tmp_path / "c" / "messages.jsonl")
    assert first["messages_saved"] == 40 and first["transactions"] == 160
    assert json.loads((tmp_path / "a" / "summary.json").read_text(encoding="utf-8")) == first


def test_every_record_is_labelled_synthetic(tmp_path):
    summary = generate_capture(tmp_path / "s", messages=5)
    rows = [json.loads(line) for line in (tmp_path / "s" / "messages.jsonl").read_text(encoding="utf-8").splitlines()]
    assert summary["source_mode"] == "synthetic"
    assert {row["source_mode"] for row in rows} == {"synthetic"}
    assert [row["message_id"] for row in rows] == [f"{summary['run_id']}:{n}" for n in range(1, 6)]


def test_the_normal_pipeline_accepts_a_synthetic_capture_without_issues(tmp_path):
    generate_capture(tmp_path / "s", messages=50, transactions_per_message=3)
    summary = process_capture(tmp_path / "s" / "messages.jsonl", tmp_path / "processed")
    # Amounts balance (inputs = outputs + fee) and every prevout is present, so nothing is incomplete.
    assert summary["features_written"] == 150 and summary["unique_transaction_ids"] == 150
    assert summary["issue_count"] == 0 and summary["incomplete_added_transactions"] == 0


def test_generation_refuses_bad_arguments_and_existing_output(tmp_path):
    with pytest.raises(ValueError, match="positive"):
        generate_capture(tmp_path / "x", messages=0)
    generate_capture(tmp_path / "s", messages=1)
    with pytest.raises(ValueError, match="already exists"):
        generate_capture(tmp_path / "s", messages=1)


def test_cli_writes_a_session_folder(tmp_path, capsys):
    assert main(["synth-capture", "--output", str(tmp_path / "s"), "--messages", "3", "--seed", "9"]) == 0
    assert (tmp_path / "s" / "summary.json").is_file()
    assert '"source_mode": "synthetic"' in capsys.readouterr().out


def test_a_synthetic_session_passes_every_lake_contract_including_capture_sessions(tmp_path):
    """A generated session must go through the whole batch path with nothing quarantined, so dbt and the shared
    queries work on generated data too."""
    pytest.importorskip("pyarrow")
    from blockpulse import collector
    layout = collector.Layout(tmp_path / "root")
    generate_capture(layout.raw / "session-synth", messages=30, seed=5)
    result = collector.process_pending(layout)
    assert result["failed"] == []
    report = json.loads((layout.root / "lake" / "_quality" / "session-synth.json").read_text(encoding="utf-8"))
    assert {name: table["rows_quarantined"] for name, table in report["tables"].items()} == {
        "transactions": 0, "lifecycle_events": 0, "capture_sessions": 0}
    assert report["tables"]["capture_sessions"]["rows_written"] == 1
