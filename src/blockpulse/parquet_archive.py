"""Restart-safe Parquet archive for enriched Kafka message envelopes."""

import hashlib
import json
import os
from pathlib import Path
import sys
import time
from datetime import datetime, timezone

from blockpulse.events import reject_nonfinite
from blockpulse.kafka_pipeline import DEFAULT_BOOTSTRAP_SERVERS, ENRICHED_TOPIC, _kafka_types
from blockpulse.storage import write_summary


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _canonical_hash(row):
    data = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _append_error(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as f:
        f.write((json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8"))
        f.flush()
        os.fsync(f.fileno())


def _schema(pa):
    return pa.schema([
        ("schema_version", pa.int16()),
        ("message_id", pa.string()),
        ("run_id", pa.string()),
        ("session_id", pa.string()),
        ("received_at", pa.string()),
        ("source_mode", pa.string()),
        ("message_type", pa.string()),
        ("raw_payload_sha256", pa.string()),
        ("events_json", pa.large_string()),
        ("parse_issues_json", pa.large_string()),
        ("enrichment_summary_json", pa.large_string()),
        ("record_sha256", pa.string()),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
    ])


def archive_enriched_to_parquet(
    output: Path,
    group_id: str,
    bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
    *,
    consumer_factory=None,
    max_messages=None,
    idle_timeout=3.0,
    assignment_timeout=30.0,
):
    """Write one deterministic Parquet file per message; fsync before offset commit."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise RuntimeError("Parquet archival requires pyarrow; install python -m pip install -e '.[m1]'") from error
    if not group_id or not group_id.strip():
        raise ValueError("group_id is required")
    if max_messages is not None and (type(max_messages) is not int or max_messages <= 0):
        raise ValueError("max_messages must be a positive integer")
    if type(idle_timeout) not in (int, float) or idle_timeout <= 0:
        raise ValueError("idle_timeout must be greater than zero")
    _, Consumer, _, _ = _kafka_types()
    consumer = (consumer_factory or Consumer)({
        "bootstrap.servers": bootstrap_servers, "group.id": group_id,
        "client.id": "blockpulse-parquet-archive", "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    output = output.resolve()
    data_dir = output / "messages"
    data_dir.mkdir(parents=True, exist_ok=True)
    errors_path = output / "errors.jsonl"
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("topic") != ENRICHED_TOPIC or manifest.get("group_id") != group_id:
            raise ValueError("Existing Parquet archive topic/group_id differs; use a new directory or original settings")
    else:
        manifest = {
            "schema_version": 1, "topic": ENRICHED_TOPIC, "group_id": group_id,
            "bootstrap_servers": bootstrap_servers, "created_at": _now(),
            "format": "parquet-message-records-v1", "compression": "zstd",
        }
        write_summary(manifest_path, manifest)
    consumer.subscribe([ENRICHED_TOPIC])
    started_wall = _now()
    started = time.monotonic()
    assigned_at = started
    idle_since = None
    consumed = archived = duplicates = conflicts = invalid = 0
    offsets = {}
    final_lag = None
    _log = {"timestamp": _now(), "level": "INFO", "event": "parquet_archive_started", "topic": ENRICHED_TOPIC, "group_id": group_id}
    print(json.dumps(_log, separators=(",", ":")), file=sys.stderr, flush=True)
    try:
        while max_messages is None or consumed < max_messages:
            message = consumer.poll(0.25)
            if message is None:
                now = time.monotonic()
                if not consumer.assignment():
                    if now - assigned_at >= assignment_timeout:
                        raise RuntimeError(f"Parquet consumer was not assigned partitions within {assignment_timeout:g}s")
                    idle_since = None
                    continue
                if idle_since is None:
                    idle_since = now
                elif now - idle_since >= idle_timeout:
                    break
                continue
            if message.error() is not None:
                raise RuntimeError(f"Kafka enriched consumer error: {message.error()}")
            consumed += 1
            idle_since = None
            partition, offset = message.partition(), message.offset()
            offsets[str(partition)] = max(offsets.get(str(partition), -1), offset)
            try:
                row = json.loads(message.value(), parse_constant=reject_nonfinite)
                if not isinstance(row, dict) or row.get("schema_version") != 1 or not isinstance(row.get("message_id"), str) or not row["message_id"]:
                    raise ValueError("Expected a schema_version 1 enriched message with message_id")
                if not isinstance(row.get("events"), list) or not isinstance(row.get("parse_issues"), list):
                    raise ValueError("Enriched message events and parse_issues must be arrays")
                digest = _canonical_hash(row)
                file_key = hashlib.sha256(row["message_id"].encode("utf-8")).hexdigest()
                target_dir = data_dir / file_key[:2]
                target_dir.mkdir(parents=True, exist_ok=True)
                target = target_dir / f"{file_key}.parquet"
                if target.exists():
                    prior = pq.read_table(target, columns=["record_sha256"]).column("record_sha256")[0].as_py()
                    if prior == digest:
                        duplicates += 1
                    else:
                        conflicts += 1
                        _append_error(errors_path, {
                            "kind": "conflicting_message_id", "message_id": row["message_id"],
                            "partition": partition, "offset": offset,
                            "detail": "Existing Parquet record retained; conflicting content skipped.",
                        })
                else:
                    table_row = {
                        "schema_version": 1,
                        "message_id": row["message_id"],
                        "run_id": row.get("run_id"),
                        "session_id": row.get("session_id"),
                        "received_at": row.get("received_at"),
                        "source_mode": row.get("source_mode"),
                        "message_type": row.get("message_type"),
                        "raw_payload_sha256": row.get("raw_payload_sha256"),
                        "events_json": json.dumps(row["events"], ensure_ascii=False, separators=(",", ":"), allow_nan=False),
                        "parse_issues_json": json.dumps(row["parse_issues"], ensure_ascii=False, separators=(",", ":"), allow_nan=False),
                        "enrichment_summary_json": json.dumps(row.get("enrichment_summary", {}), ensure_ascii=False, separators=(",", ":"), allow_nan=False),
                        "record_sha256": digest,
                        "kafka_partition": partition,
                        "kafka_offset": offset,
                    }
                    temporary = target.with_name(target.stem + ".tmp.parquet")
                    pq.write_table(pa.Table.from_pylist([table_row], schema=_schema(pa)), temporary, compression="zstd")
                    with temporary.open("r+b") as f:
                        os.fsync(f.fileno())
                    os.replace(temporary, target)
                    archived += 1
            except (ValueError, TypeError, UnicodeError) as error:
                invalid += 1
                _append_error(errors_path, {
                    "kind": "invalid_enriched_message", "partition": partition,
                    "offset": offset, "detail": str(error),
                })
            consumer.commit(message=message, asynchronous=False)
    finally:
        final_lag = _lag(consumer)
        consumer.close()
    elapsed = max(time.monotonic() - started, 0.001)
    files = sorted(data_dir.rglob("*.parquet"))
    digest = hashlib.sha256()
    total_bytes = 0
    for file in files:
        content_hash = hashlib.sha256(file.read_bytes()).hexdigest()
        digest.update(file.relative_to(output).as_posix().encode("utf-8"))
        digest.update(content_hash.encode("ascii"))
        total_bytes += file.stat().st_size
    summary = {
        **manifest, "status": "complete", "started_at": started_wall, "ended_at": _now(),
        "messages_consumed": consumed, "messages_archived": archived,
        "duplicate_messages_skipped": duplicates, "conflicting_messages": conflicts,
        "invalid_messages": invalid, "archive_messages_total": len(files),
        "last_offsets_by_partition": offsets, "archive_sha256": digest.hexdigest(),
        "archive_bytes": total_bytes, "elapsed_seconds": round(elapsed, 3),
        "messages_per_second": round(consumed / elapsed, 3),
        "consumer_lag_messages": final_lag,
    }
    write_summary(manifest_path, summary)
    print(json.dumps({"timestamp": _now(), "level": "INFO", "event": "parquet_archive_finished", **summary}, separators=(",", ":")), file=sys.stderr, flush=True)
    return summary


def _lag(consumer):
    try:
        assigned = consumer.assignment()
        positions = consumer.position(assigned)
        return sum(max(0, consumer.get_watermark_offsets(tp, timeout=1)[1] - pos.offset)
                   for tp, pos in zip(assigned, positions) if pos.offset >= 0)
    except Exception:
        return None
