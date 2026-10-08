import json

from blockpulse.events import parse_record


def test_batch_counts_and_receipt_time(transaction, record_factory):
    record = record_factory({"mempool-transactions": {
        "sequence": 100,
        "added": [transaction, {**transaction, "txid": "c" * 64, "firstSeen": 0}],
        "removed": ["d" * 64], "mined": ["e" * 64], "replaced": ["f" * 64],
    }})
    parsed = parse_record(record)
    assert not parsed.issues
    assert len(parsed.events) == 5
    assert parsed.sequences == {"mempool-transactions": 100}
    assert len({event["event_id"] for event in parsed.events}) == 5
    assert all(event["event_time"] == record["received_at"] for event in parsed.events)
    assert parsed.events == parse_record(record).events


def test_control_invalid_json_and_unknown_shape_are_preserved(record_factory):
    control = parse_record(record_factory({"mempoolInfo": {"size": 1}}))
    assert not control.recognized and not control.events and not control.issues
    invalid = parse_record(record_factory({}, raw_text="{broken"))
    assert invalid.issues and not invalid.events
    invalid_number = parse_record(record_factory({}, raw_text='{"value": NaN}'))
    assert invalid_number.issues
    replacement = {"unsupported_old": "a" * 64, "unsupported_new": "b" * 64}
    parsed = parse_record(record_factory({"mempool-transactions": {"replaced": [replacement]}}))
    assert parsed.issues
    assert parsed.events[0]["payload"] == replacement
    assert parsed.events[0]["txid"] is None


def test_ids_only_and_bad_envelope(record_factory):
    parsed = parse_record(record_factory({"mempool-txids": {"added": ["a" * 64]}}))
    assert parsed.events[0]["payload"] == "a" * 64
    assert not parsed.issues
    assert parse_record(record_factory({}, schema_version=99)).issues
    assert parse_record(record_factory({}, received_at="2026-10-08")).issues
    assert parse_record([1, 2]).issues
