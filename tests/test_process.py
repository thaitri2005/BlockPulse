from copy import deepcopy
import json
from unittest.mock import patch

import pytest

from blockpulse.process import process_capture


def write_capture(path, records):
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")


def test_replay_deduplicates_and_is_byte_identical(tmp_path, transaction, record_factory):
    first = record_factory({"mempool-transactions": {"added": [transaction]}})
    second = record_factory({"mempool-transactions": {"added": [transaction], "mined": [transaction["txid"]]}}, 2)
    source = tmp_path / "messages.jsonl"
    write_capture(source, [first, first, second])
    with patch("socket.socket", side_effect=AssertionError("Offline processing tried the network")):
        summary = process_capture(source, tmp_path / "one")
        again = process_capture(source, tmp_path / "two")
    assert summary == again
    assert summary["features_written"] == 1
    assert summary["duplicate_messages"] == 1
    assert summary["duplicate_added_transactions"] == 1
    assert summary["event_counts"] == {"added": 2, "removed": 0, "mined": 1, "replaced": 0}
    for name in ("events.jsonl", "features.jsonl", "features.csv", "errors.jsonl", "summary.json"):
        assert (tmp_path / "one" / name).read_bytes() == (tmp_path / "two" / name).read_bytes()
    with pytest.raises(FileExistsError):
        process_capture(source, tmp_path / "one")


def test_bad_lines_partial_records_and_conflicts_do_not_hide_good_records(tmp_path, transaction, record_factory):
    incomplete = record_factory({"mempool-txids": {"added": [transaction["txid"]]}})
    full = record_factory({"mempool-transactions": {"added": [transaction]}}, 2)
    changed = deepcopy(transaction)
    changed["fee"] += 1
    conflict = record_factory({"mempool-transactions": {"added": [changed]}}, 3)
    source = tmp_path / "messages.jsonl"
    write_capture(source, [incomplete, full, conflict, [1, 2, 3]])
    with source.open("a", encoding="utf-8") as file:
        file.write("{truncated\n")
    summary = process_capture(source, tmp_path / "processed")
    assert summary["features_written"] == 1
    assert summary["incomplete_added_transactions"] == 1
    assert summary["conflicting_feature_rows"] == 1
    assert summary["issue_count"] == 4
    assert summary["added_field_presence"]["vin"] == 2
    assert len((tmp_path / "processed" / "events.jsonl").read_text().splitlines()) == 3


def test_message_conflict_is_not_silently_overwritten(tmp_path, transaction, record_factory):
    record = record_factory({"mempool-transactions": {"added": [transaction]}})
    changed = {**record, "raw_text": '{"mempool-transactions":{"removed":[]}}'}
    source = tmp_path / "messages.jsonl"
    write_capture(source, [record, changed])
    summary = process_capture(source, tmp_path / "processed")
    assert summary["conflicting_messages"] == 1
    assert summary["features_written"] == 1
