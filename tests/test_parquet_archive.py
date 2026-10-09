import json

import pyarrow.parquet as pq

from blockpulse.kafka_pipeline import ENRICHED_TOPIC
from blockpulse.parquet_archive import archive_enriched_to_parquet


class Message:
    def __init__(self, value, offset=0): self._value, self._offset = value, offset
    def value(self): return self._value
    def partition(self): return 1
    def offset(self): return self._offset
    def error(self): return None


class Consumer:
    def __init__(self, records): self.records, self.commits, self.subscriptions = list(records), [], []
    def subscribe(self, topics): self.subscriptions = topics
    def poll(self, timeout): return self.records.pop(0) if self.records else None
    def assignment(self): return [object()]
    def commit(self, *, message, asynchronous): self.commits.append(message.offset())
    def close(self): pass


def enriched(message_id="r:1"):
    return {
        "schema_version": 1, "message_id": message_id, "run_id": "r", "session_id": "s",
        "received_at": "2026-10-09T00:00:00+00:00", "source_mode": "synthetic",
        "message_type": "text", "raw_payload_sha256": "a" * 64,
        "events": [{"event_id": "event-1", "event_type": "added", "txid": "b" * 64, "payload": {"fee": 1}}],
        "parse_issues": [], "enrichment_summary": {"requests": 0, "enriched": 0},
    }


def test_parquet_archive_writes_before_commit_and_resumes_without_duplicate_files(tmp_path):
    row = enriched()
    first = Consumer([Message(json.dumps(row).encode(), 7)])
    output = tmp_path / "parquet"
    result = archive_enriched_to_parquet(output, "parquet-group", consumer_factory=lambda _config: first, max_messages=1)
    assert first.subscriptions == [ENRICHED_TOPIC]
    assert first.commits == [7]
    assert result["messages_archived"] == 1
    files = list((output / "messages").rglob("*.parquet"))
    assert len(files) == 1
    table = pq.read_table(files[0])
    assert table.num_rows == 1
    assert json.loads(table.column("events_json")[0].as_py())[0]["txid"] == "b" * 64
    digest_before = result["archive_sha256"]

    restart = Consumer([Message(json.dumps(row).encode(), 8)])
    resumed = archive_enriched_to_parquet(output, "parquet-group", consumer_factory=lambda _config: restart, max_messages=1)
    assert resumed["messages_archived"] == 0
    assert resumed["duplicate_messages_skipped"] == 1
    assert resumed["archive_messages_total"] == 1
    assert resumed["archive_sha256"] == digest_before


def test_parquet_archive_quarantines_invalid_record_before_offset_commit(tmp_path):
    bad = {"schema_version": 1, "message_id": "bad", "events": "not-a-list", "parse_issues": []}
    consumer = Consumer([Message(json.dumps(bad).encode(), 2)])
    result = archive_enriched_to_parquet(tmp_path / "invalid", "bad-group", consumer_factory=lambda _config: consumer, max_messages=1)
    assert result["invalid_messages"] == 1
    assert consumer.commits == [2]
    error = json.loads((tmp_path / "invalid/errors.jsonl").read_text().splitlines()[0])
    assert error["kind"] == "invalid_enriched_message"
