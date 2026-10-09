import json

import pytest
from unittest.mock import patch

from blockpulse.kafka_pipeline import RAW_TOPIC, RawKafkaPublisher, archive_topic, create_raw_topic, publish_capture


class FakeDelivery:
    def __init__(self, value, partition=0, offset=0, error=None):
        self._value, self._partition, self._offset, self._error = value, partition, offset, error

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return self._error


class FakeProducer:
    def __init__(self, config):
        self.config = config
        self.pending = []
        self.records = []

    def produce(self, topic, *, key, value, on_delivery):
        self.records.append((topic, key, value))
        self.pending.append(on_delivery)

    def poll(self, timeout):
        if self.pending:
            self.pending.pop(0)(None, None)

    def flush(self, timeout):
        while self.pending:
            self.poll(0)
        return 0


class FakeConsumer:
    def __init__(self, config, records, before_commit=None):
        self.config = config
        self.records = list(records)
        self.committed = []
        self.subscriptions = []
        self.before_commit = before_commit

    def subscribe(self, topics):
        self.subscriptions = topics

    def poll(self, timeout):
        return self.records.pop(0) if self.records else None

    def commit(self, *, message, asynchronous):
        if self.before_commit:
            self.before_commit(message, len(self.committed))
        self.committed.append((message.partition(), message.offset(), asynchronous))

    def assignment(self):
        return [object()]

    def close(self):
        pass


class DelayedAssignmentConsumer(FakeConsumer):
    def __init__(self, config, records):
        super().__init__(config, records)
        self.polls = 0

    def poll(self, timeout):
        self.polls += 1
        if self.polls == 1:
            return None
        return super().poll(timeout)

    def assignment(self):
        return [] if self.polls < 2 else [object()]


def envelope(message_id, text="sample"):
    return {
        "schema_version": 1,
        "run_id": "run-1",
        "session_id": "session-1",
        "message_id": message_id,
        "received_at": "2026-10-09T00:00:00+00:00",
        "source_mode": "synthetic",
        "message_type": "text",
        "raw_text": text,
    }


def test_create_topic_skips_existing_and_applies_bounded_retention():
    class Admin:
        def __init__(self, config):
            self.config = config
            self.created = None

        def list_topics(self, timeout):
            return type("Metadata", (), {"topics": {}})()

        def create_topics(self, topics, request_timeout):
            self.created = topics[0]
            return {RAW_TOPIC: type("Future", (), {"result": lambda self, timeout: None})()}

    class Topic:
        def __init__(self, name, **kwargs):
            self.name, self.kwargs = name, kwargs

    admin = Admin({})
    result = create_raw_topic(admin_factory=lambda config: admin, new_topic_factory=Topic)
    assert result == {"topic": RAW_TOPIC, "created": True, "partitions": 3}
    assert admin.created.kwargs["replication_factor"] == 1
    assert admin.created.kwargs["config"]["retention.ms"] == "604800000"


def test_replay_publishes_capture_envelopes_partitioned_by_session(tmp_path):
    source = tmp_path / "messages.jsonl"
    rows = [envelope("m1"), envelope("m2")]
    source.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    producers = []

    def factory(config):
        instance = FakeProducer(config)
        producers.append(instance)
        return instance

    summary = publish_capture(source, producer_factory=factory)
    assert summary["messages_published"] == 2
    assert producers[0].config["enable.idempotence"] is True
    assert [key for _, key, _ in producers[0].records] == [b"session-1", b"session-1"]
    assert [json.loads(value)["message_id"] for _, _, value in producers[0].records] == ["m1", "m2"]


def test_archive_is_restart_safe_and_deduplicates_before_committing(tmp_path):
    rows = [envelope("m1"), envelope("m2")]
    records = [FakeDelivery(json.dumps(row).encode(), offset=i) for i, row in enumerate(rows)]
    consumers = []
    fsync_count = 0

    def fake_fsync(_descriptor):
        nonlocal fsync_count
        fsync_count += 1

    def check_durable_before_commit(_message, prior_commits):
        assert fsync_count > prior_commits

    def factory(config):
        instance = FakeConsumer(config, records, check_durable_before_commit)
        consumers.append(instance)
        return instance

    output = tmp_path / "archive"
    with patch("blockpulse.kafka_pipeline.os.fsync", side_effect=fake_fsync):
        summary = archive_topic(output, "test-group", consumer_factory=factory, max_messages=2)
    assert summary["messages_archived"] == 2
    assert consumers[0].config["enable.auto.commit"] is False
    assert consumers[0].committed == [(0, 0, False), (0, 1, False)]

    replayed = [FakeDelivery(json.dumps(row).encode(), offset=i) for i, row in enumerate(rows)]
    def restart_factory(config):
        instance = FakeConsumer(config, replayed)
        consumers.append(instance)
        return instance

    resumed = archive_topic(output, "test-group", consumer_factory=restart_factory, max_messages=2)
    assert resumed["messages_archived"] == 0
    assert resumed["duplicate_messages_skipped"] == 2
    assert len((output / "messages.jsonl").read_text().splitlines()) == 2



def test_archive_waits_for_assignment_before_starting_idle_timeout(tmp_path):
    row = envelope("after-assignment")
    record = FakeDelivery(json.dumps(row).encode(), offset=0)
    output = tmp_path / "delayed-archive"
    summary = archive_topic(
        output,
        "delayed-group",
        consumer_factory=lambda config: DelayedAssignmentConsumer(config, [record]),
        max_messages=1,
    )
    assert summary["messages_consumed"] == 1
    assert summary["messages_archived"] == 1


def test_archive_records_conflicting_message_ids_and_refuses_wrong_resume(tmp_path):
    first = envelope("same", "first")
    second = envelope("same", "different")
    records = [FakeDelivery(json.dumps(row).encode(), offset=i) for i, row in enumerate((first, second))]
    output = tmp_path / "archive"
    summary = archive_topic(
        output, "stable-group", consumer_factory=lambda config: FakeConsumer(config, records), max_messages=2,
    )
    assert summary["messages_archived"] == 1
    assert summary["conflicting_messages"] == 1
    error = json.loads((output / "errors.jsonl").read_text().strip())
    assert error["kind"] == "conflicting_message_id"
    with pytest.raises(ValueError, match="topic/group_id differs"):
        archive_topic(output, "different-group", consumer_factory=lambda config: None, max_messages=1)


def test_kafka_replay_rejects_bad_source_before_publish(tmp_path):
    source = tmp_path / "bad.jsonl"
    source.write_text("{bad\n", encoding="utf-8")
    producers = []

    def factory(config):
        producer = FakeProducer(config)
        producers.append(producer)
        return producer

    with pytest.raises(ValueError, match="invalid JSON"):
        publish_capture(source, producer_factory=factory)
    assert producers[0].records == []


def test_live_publisher_acks_capture_envelopes():
    producers = []
    def factory(config):
        producer = FakeProducer(config)
        producers.append(producer)
        return producer
    publisher = RawKafkaPublisher(producer_factory=factory)
    publisher(envelope("m-live"))
    result = publisher.close()
    assert result["messages_queued"] == result["messages_acknowledged"] == 1
    assert producers[0].records[0][0] == RAW_TOPIC
    assert producers[0].config["enable.idempotence"] is True
