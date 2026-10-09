"""First Kafka-backed M1 path: replay raw captures and archive records locally."""

from datetime import datetime, timezone
import base64
import hashlib
import json
import os
from pathlib import Path
import time

from blockpulse.events import reject_nonfinite
from blockpulse.storage import write_summary

RAW_TOPIC = "blockpulse.raw.v1"
ENRICHED_TOPIC = "blockpulse.enriched.v1"
ERROR_TOPIC = "blockpulse.errors.v1"
DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9092"


def _kafka_types():
    try:
        from confluent_kafka import Consumer, Producer
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError as error:
        raise RuntimeError(
            "Kafka commands require the optional dependency; install with "
            "python -m pip install -e '.[kafka]'"
        ) from error
    return Producer, Consumer, AdminClient, NewTopic


def _utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _decode_envelope(raw: bytes, line_number: int) -> dict:
    try:
        row = json.loads(raw, parse_constant=reject_nonfinite)
    except (ValueError, TypeError, UnicodeError) as error:
        raise ValueError(f"line {line_number}: invalid JSON: {error}") from error
    if not isinstance(row, dict) or row.get("schema_version") != 1:
        raise ValueError(f"line {line_number}: expected a schema_version 1 capture envelope")
    if not isinstance(row.get("message_id"), str) or not row["message_id"]:
        raise ValueError(f"line {line_number}: message_id is required")
    return row


def create_raw_topic(bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS, *, admin_factory=None, new_topic_factory=None):
    """Create the versioned raw topic with local single-broker settings."""
    if admin_factory is None or new_topic_factory is None:
        _, _, default_admin, default_new_topic = _kafka_types()
        admin_factory = admin_factory or default_admin
        new_topic_factory = new_topic_factory or default_new_topic
    admin = admin_factory({"bootstrap.servers": bootstrap_servers, "client.id": "blockpulse-admin"})
    metadata = admin.list_topics(timeout=10)
    if RAW_TOPIC in metadata.topics:
        return {"topic": RAW_TOPIC, "created": False, "partitions": len(metadata.topics[RAW_TOPIC].partitions)}
    topic = new_topic_factory(
        RAW_TOPIC,
        num_partitions=3,
        replication_factor=1,
        config={"retention.ms": "604800000", "cleanup.policy": "delete"},
    )
    futures = admin.create_topics([topic], request_timeout=10)
    futures[RAW_TOPIC].result(timeout=12)
    return {"topic": RAW_TOPIC, "created": True, "partitions": 3}



def create_m1_topics(bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS):
    """Create the raw, enriched, and error topics used by the local M1 path."""
    raw = create_raw_topic(bootstrap_servers)
    _, _, admin_factory, new_topic_factory = _kafka_types()
    admin = admin_factory({"bootstrap.servers": bootstrap_servers, "client.id": "blockpulse-admin-m1"})
    metadata = admin.list_topics(timeout=10)
    created = []
    for topic_name in (ENRICHED_TOPIC, ERROR_TOPIC):
        if topic_name in metadata.topics:
            continue
        topic = new_topic_factory(
            topic_name, num_partitions=3, replication_factor=1,
            config={"retention.ms": "604800000", "cleanup.policy": "delete"},
        )
        futures = admin.create_topics([topic], request_timeout=10)
        futures[topic_name].result(timeout=12)
        created.append(topic_name)
    return {"topics": [RAW_TOPIC, ENRICHED_TOPIC, ERROR_TOPIC], "created": created, "raw_created": raw["created"], "partitions": 3}


class RawKafkaPublisher:
    """Synchronous Kafka sink for capture envelopes; local JSONL is written first."""

    def __init__(self, bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS, *, producer_factory=None, topic=RAW_TOPIC):
        if producer_factory is None:
            producer_factory = _kafka_types()[0]
        self.topic = topic
        self.producer = producer_factory({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "blockpulse-live-capture",
            "acks": "all",
            "enable.idempotence": True,
            "message.max.bytes": 8388608,
            "message.timeout.ms": 30000,
        })
        self.queued = 0
        self.acknowledged = 0
        self.errors = []
        self.started = time.monotonic()
        self.closed = False

    def __call__(self, envelope):
        value = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        key = str(envelope.get("session_id") or envelope.get("run_id") or envelope["message_id"]).encode("utf-8")
        def on_delivery(error, message):
            if error is not None:
                self.errors.append(str(error))
            else:
                self.acknowledged += 1
        while True:
            try:
                self.producer.produce(self.topic, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:
                self.producer.poll(0.1)
        self.producer.poll(0)
        self.queued += 1

    def close(self):
        if self.closed:
            return self.summary()
        remaining = self.producer.flush(30)
        self.closed = True
        if remaining:
            self.errors.append(f"{remaining} Kafka messages remained after flush timeout")
        if self.errors:
            raise RuntimeError(f"Kafka capture publish failed: {'; '.join(self.errors[:5])}")
        return self.summary()

    def summary(self):
        elapsed = max(time.monotonic() - self.started, 0.001)
        return {
            "topic": self.topic,
            "messages_queued": self.queued,
            "messages_acknowledged": self.acknowledged,
            "elapsed_seconds": round(elapsed, 3),
            "messages_per_second": round(self.acknowledged / elapsed, 3),
        }

def publish_capture(source: Path, bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS, *, producer_factory=None, topic=RAW_TOPIC):
    """Publish capture envelopes in file order, keyed by session for partition order."""
    source = source.resolve()
    if not source.is_file():
        raise ValueError(f"Capture file does not exist: {source}")
    if producer_factory is None:
        producer_factory = _kafka_types()[0]
    producer = producer_factory({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "blockpulse-replay",
        "acks": "all",
        "enable.idempotence": True,
        "message.max.bytes": 8388608,
        "message.timeout.ms": 30000,
    })
    started = time.monotonic()
    delivered = 0
    errors = []
    source_hash = hashlib.sha256()
    source_bytes = 0

    def on_delivery(error, message):
        if error is not None:
            errors.append(str(error))

    with source.open("rb") as rows:
        for line_number, raw in enumerate(rows, start=1):
            if not raw.strip():
                continue
            source_hash.update(raw)
            source_bytes += len(raw)
            envelope = _decode_envelope(raw, line_number)
            key = next(
                (envelope[field] for field in ("session_id", "run_id", "message_id")
                 if isinstance(envelope.get(field), str) and envelope[field]),
                envelope["message_id"],
            )
            value = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
            while True:
                try:
                    producer.produce(topic, key=key.encode("utf-8"), value=value, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.1)
            producer.poll(0)
            delivered += 1
    remaining = producer.flush(30)
    if remaining:
        errors.append(f"{remaining} Kafka messages were still queued after flush timeout")
    if errors:
        raise RuntimeError(f"Kafka publish failed after queuing {delivered} messages: {'; '.join(errors[:5])}")
    elapsed = max(time.monotonic() - started, 0.001)
    return {
        "topic": topic,
        "source_file": str(source),
        "source_sha256": source_hash.hexdigest(),
        "source_file_bytes": source_bytes,
        "messages_published": delivered,
        "elapsed_seconds": round(elapsed, 3),
        "messages_per_second": round(delivered / elapsed, 3),
        "partition_key": "session_id, then run_id, then message_id",
    }


def _hash_record(row: dict) -> str:
    encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _load_seen(path: Path) -> dict[str, str]:
    seen = {}
    if not path.exists():
        return seen
    with path.open("rb") as archive:
        for line_number, raw in enumerate(archive, start=1):
            row = _decode_envelope(raw, line_number)
            digest = _hash_record(row)
            prior = seen.get(row["message_id"])
            if prior is not None and prior != digest:
                raise ValueError(f"Archive contains conflicting message_id {row['message_id']!r}")
            seen[row["message_id"]] = digest
    return seen


def _append_jsonl(file, row: dict):
    encoded = (json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
    file.write(encoded)
    file.flush()
    os.fsync(file.fileno())



def _consumer_lag(consumer):
    try:
        assigned = consumer.assignment()
        positions = consumer.position(assigned)
        return sum(
            max(0, consumer.get_watermark_offsets(tp, timeout=1)[1] - position.offset)
            for tp, position in zip(assigned, positions) if position.offset >= 0
        )
    except Exception:
        return None

def archive_topic(
    output: Path,
    group_id: str,
    bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
    *,
    consumer_factory=None,
    topic=RAW_TOPIC,
    max_messages: int | None = None,
    idle_timeout: float = 3.0,
):
    """Consume into JSONL, fsync before committing offsets, and dedupe on restart."""
    if not group_id or not group_id.strip():
        raise ValueError("group_id is required")
    if max_messages is not None and (type(max_messages) is not int or max_messages <= 0):
        raise ValueError("max_messages must be a positive integer")
    if type(idle_timeout) not in (int, float) or idle_timeout <= 0:
        raise ValueError("idle_timeout must be greater than zero")
    if consumer_factory is None:
        consumer_factory = _kafka_types()[1]
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    archive_path = output / "messages.jsonl"
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("topic") != topic or manifest.get("group_id") != group_id:
            raise ValueError("Existing archive topic/group_id differs; use a new output directory or the original settings")
    elif archive_path.exists():
        raise ValueError("Existing archive has no manifest; refusing unsafe resume")
    else:
        manifest = {
            "schema_version": 1,
            "topic": topic,
            "group_id": group_id,
            "bootstrap_servers": bootstrap_servers,
            "created_at": _utc_now(),
            "archive_format": "capture-envelope-jsonl-v1",
        }
        write_summary(manifest_path, manifest)
    seen = _load_seen(archive_path)
    consumer = consumer_factory({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "client.id": "blockpulse-archive",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    started = _utc_now()
    consumer_started = time.monotonic()
    idle_since = None
    final_lag = None
    consumed = archived = duplicates = conflicts = errors = 0
    offsets = {}
    error_path = output / "errors.jsonl"
    try:
        with archive_path.open("ab") as archive, error_path.open("ab") as error_file:
            while max_messages is None or consumed < max_messages:
                message = consumer.poll(0.25)
                if message is None:
                    now = time.monotonic()
                    if not consumer.assignment():
                        if now - consumer_started >= 30:
                            raise RuntimeError("Kafka consumer did not receive a partition assignment within 30 seconds")
                        idle_since = None
                        continue
                    if idle_since is None:
                        idle_since = now
                    elif now - idle_since >= idle_timeout:
                        break
                    continue
                kafka_error = message.error()
                if kafka_error is not None:
                    raise RuntimeError(f"Kafka consumer error: {kafka_error}")
                consumed += 1
                idle_since = None
                partition, offset = message.partition(), message.offset()
                offsets[str(partition)] = max(offsets.get(str(partition), -1), offset)
                try:
                    row = _decode_envelope(message.value(), offset + 1)
                    digest = _hash_record(row)
                    prior = seen.get(row["message_id"])
                    if prior is None:
                        _append_jsonl(archive, row)
                        seen[row["message_id"]] = digest
                        archived += 1
                    elif prior == digest:
                        duplicates += 1
                    else:
                        _append_jsonl(error_file, {
                            "kind": "conflicting_message_id",
                            "message_id": row["message_id"],
                            "partition": partition,
                            "offset": offset,
                            "detail": "First archive record retained; conflicting Kafka value skipped.",
                        })
                        conflicts += 1
                except (ValueError, TypeError) as error:
                    _append_jsonl(error_file, {
                        "kind": "invalid_kafka_record",
                        "partition": partition,
                        "offset": offset,
                        "detail": str(error),
                        "value_base64": base64.b64encode(message.value() or b"").decode("ascii"),
                    })
                    errors += 1
                # Local append/error record is flushed and fsynced before this offset advances.
                consumer.commit(message=message, asynchronous=False)
    finally:
        final_lag = _consumer_lag(consumer)
        consumer.close()
    elapsed = max(time.monotonic() - consumer_started, 0.001)
    archive_hash = hashlib.sha256()
    archive_bytes = 0
    with archive_path.open("rb") as archive:
        for chunk in iter(lambda: archive.read(1024 * 1024), b""):
            archive_hash.update(chunk)
            archive_bytes += len(chunk)
    summary = {
        **manifest,
        "status": "complete",
        "started_at": started,
        "ended_at": _utc_now(),
        "messages_consumed": consumed,
        "messages_archived": archived,
        "duplicate_messages_skipped": duplicates,
        "conflicting_messages": conflicts,
        "invalid_messages": errors,
        "last_offsets_by_partition": offsets,
        "archive_sha256": archive_hash.hexdigest(),
        "archive_bytes": archive_bytes,
        "elapsed_seconds": round(elapsed, 3),
        "messages_per_second": round(consumed / elapsed, 3),
        "consumer_lag_messages": final_lag,
    }
    write_summary(manifest_path, summary)
    return summary
