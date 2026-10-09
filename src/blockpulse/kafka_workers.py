"""Kafka consumer workers for bounded enrichment and explicit error records."""

import base64
import json
import sys
import time
from datetime import datetime, timezone

from blockpulse.kafka_pipeline import (
    DEFAULT_BOOTSTRAP_SERVERS,
    ENRICHED_TOPIC,
    ERROR_TOPIC,
    RAW_TOPIC,
    _decode_envelope,
    _kafka_types,
)


def _log(event, **fields):
    row = {"timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"), "level": "INFO", "event": event, **fields}
    print(json.dumps(row, separators=(",", ":"), allow_nan=False), file=sys.stderr, flush=True)


def _produce_wait(producer, topic, key, value):
    errors = []
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    def delivered(error, _message):
        if error is not None:
            errors.append(str(error))
    while True:
        try:
            producer.produce(topic, key=key.encode("utf-8"), value=encoded, on_delivery=delivered)
            break
        except BufferError:
            producer.poll(0.1)
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"Kafka publish to {topic} failed: {remaining} pending; {errors[:3]}")


def _consumer_lag(consumer):
    try:
        positions = consumer.position(consumer.assignment())
        total = 0
        for tp, position in zip(consumer.assignment(), positions):
            high = consumer.get_watermark_offsets(tp, timeout=1)[1]
            if position.offset >= 0:
                total += max(0, high - position.offset)
        return total
    except Exception:
        return None


def enrich_topic(
    group_id: str,
    bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
    *,
    enricher,
    consumer_factory=None,
    producer_factory=None,
    max_messages=None,
    idle_timeout=3.0,
    assignment_timeout=30.0,
):
    """Consume raw envelopes, enrich incomplete additions, publish, then commit input offsets."""
    if not group_id or not group_id.strip():
        raise ValueError("group_id is required")
    if max_messages is not None and (type(max_messages) is not int or max_messages <= 0):
        raise ValueError("max_messages must be a positive integer")
    if type(idle_timeout) not in (int, float) or idle_timeout <= 0:
        raise ValueError("idle_timeout must be greater than zero")
    Producer, Consumer, _, _ = _kafka_types()
    consumer = (consumer_factory or Consumer)({
        "bootstrap.servers": bootstrap_servers, "group.id": group_id,
        "client.id": "blockpulse-enricher", "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    producer = (producer_factory or Producer)({
        "bootstrap.servers": bootstrap_servers, "client.id": "blockpulse-enrichment-output",
        "acks": "all", "enable.idempotence": True, "message.max.bytes": 8388608,
    })
    consumer.subscribe([RAW_TOPIC])
    started = time.monotonic()
    idle_since = None
    messages = events = failures = requests = enriched_count = 0
    offsets = {}
    final_lag = None
    _log("kafka_enrichment_started", group_id=group_id, topic=RAW_TOPIC, output_topic=ENRICHED_TOPIC)
    try:
        while max_messages is None or messages < max_messages:
            message = consumer.poll(0.25)
            if message is None:
                now = time.monotonic()
                if not consumer.assignment():
                    if now - started >= assignment_timeout:
                        raise RuntimeError(f"Kafka enrichment consumer was not assigned partitions within {assignment_timeout:g}s")
                    idle_since = None
                    continue
                if idle_since is None:
                    idle_since = now
                elif now - idle_since >= idle_timeout:
                    break
                continue
            error = message.error()
            if error is not None:
                raise RuntimeError(f"Kafka raw consumer error: {error}")
            try:
                row = _decode_envelope(message.value(), message.offset() + 1)
            except ValueError as error:
                _produce_wait(producer, ERROR_TOPIC, f"{message.partition()}:{message.offset()}", {
                    "schema_version": 1, "stage": "enrichment", "kind": "invalid_raw_envelope",
                    "partition": message.partition(), "offset": message.offset(), "detail": str(error),
                    "value_base64": base64.b64encode(message.value() or b"").decode("ascii"),
                })
                consumer.commit(message=message, asynchronous=False)
                messages += 1
                failures += 1
                offsets[str(message.partition())] = max(offsets.get(str(message.partition()), -1), message.offset())
                idle_since = None
                continue
            enriched, record_errors = enricher.process(row)
            key = str(row.get("session_id") or row.get("run_id") or row["message_id"])
            _produce_wait(producer, ENRICHED_TOPIC, key, enriched)
            for issue in record_errors:
                error_record = {
                    "schema_version": 1, "stage": "enrichment",
                    "source_message_id": row["message_id"], "run_id": row.get("run_id"),
                    "received_at": row.get("received_at"), **issue,
                }
                _produce_wait(producer, ERROR_TOPIC, key, error_record)
            consumer.commit(message=message, asynchronous=False)
            messages += 1
            events += len(enriched["events"])
            failures += len(record_errors)
            requests += enriched["enrichment_summary"]["requests"]
            enriched_count += enriched["enrichment_summary"]["enriched"]
            offsets[str(message.partition())] = max(offsets.get(str(message.partition()), -1), message.offset())
            idle_since = None
    finally:
        final_lag = _consumer_lag(consumer)
        consumer.close()
        producer.flush(30)
    elapsed = max(time.monotonic() - started, 0.001)
    summary = {
        "schema_version": 1, "input_topic": RAW_TOPIC, "output_topic": ENRICHED_TOPIC,
        "error_topic": ERROR_TOPIC, "group_id": group_id,
        "messages_consumed": messages, "events_emitted": events,
        "transactions_enriched": enriched_count, "enrichment_requests": requests,
        "error_records": failures, "last_offsets_by_partition": offsets,
        "consumer_lag_messages": final_lag,
        "elapsed_seconds": round(elapsed, 3), "messages_per_second": round(messages / elapsed, 3),
        "enrichment_requests_per_second": round(requests / elapsed, 3),
        "coverage_note": "Only added transactions missing required fields are fetched; request budget and provider view bound coverage.",
    }
    _log("kafka_enrichment_finished", **summary)
    return summary
