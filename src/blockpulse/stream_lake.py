"""Stream the enriched Kafka topic straight into the lake: features, rule flags and lifecycle events.

This is the streaming twin of `process-pending`'s lake stage. It consumes `blockpulse.enriched.v1`, computes the
shared features and the structural rules in the stream, and writes micro-batches into the same lake tables through
the same quality gate.

Exactly-once *output* on top of at-least-once delivery:

* A micro-batch covers a contiguous offset range of one Kafka partition. Its files are named after that partition
  and the batch's first offset (`stream-p{partition}-o{first_offset}`).
* Before writing, any files left by an earlier attempt at the same batch (a crash after writing, before committing)
  are deleted, in every partition directory, so a replay can't leave stale rows behind.
* Offsets are committed only after the files and their quality report are written. A crash means the batch is
  read again from the same committed offset and rewrites the same file names: no duplicates.

Identity: the stream knows a capture by its `run_id`, so the lake's `session` column holds the run id for streamed
rows (the batch path uses the capture folder name). Capture-session coverage rows come from capture summaries, which
are not in Kafka, so the stream writes `transactions` and `lifecycle_events` only.

Known limit: within one capture run, a transaction's first observation wins. That state is held in memory, so after a
restart a transaction re-announced later in the same run could get a second row; `fct_transactions` in dbt keeps the
first observation either way.
"""

import json
import time
from collections import defaultdict
from pathlib import Path

from blockpulse.detector import DetectorConfig, screen_transaction
from blockpulse.features import FEATURE_SET_VERSION, extract_features
from blockpulse.lake import TABLES, _partition, _write_parquet, validate
from blockpulse.storage import utc_now

STREAM_TABLES = ("transactions", "lifecycle_events")


def _batch_name(partition: int, first_offset: int) -> str:
    return f"stream-p{partition}-o{first_offset:012d}"


def rows_from_enriched(record: dict, state: dict) -> tuple[list[dict], list[dict], dict]:
    """Turn one enriched message into lake rows, applying the batch path's rules.

    state carries {"messages": set, "txids_by_run": dict} across messages. Returns (transaction rows, event rows,
    counts) where counts records skipped duplicates and incomplete additions.
    """
    counts = {"duplicate_message": 0, "incomplete_added": 0, "duplicate_txid_in_run": 0}
    message_id = record.get("message_id")
    if message_id in state["messages"]:
        counts["duplicate_message"] = 1
        return [], [], counts
    state["messages"].add(message_id)
    run = record.get("run_id")
    seen = state["txids_by_run"].setdefault(run, set())
    transactions, events = [], []
    for event in record.get("events") or []:
        if event.get("txid") is None:
            continue
        events.append({"event_id": event.get("event_id"), "session": run, "run_id": event.get("run_id"),
                       "txid": event.get("txid"), "event_type": event.get("event_type"),
                       "event_time": event.get("event_time"), "source_sequence": event.get("source_sequence"),
                       "replacement_txid": event.get("replacement_txid"),
                       "source_message_id": event.get("source_message_id")})
        if event.get("event_type") != "added":
            continue
        try:
            features = extract_features(event.get("payload"))
        except (TypeError, ValueError, AttributeError):
            counts["incomplete_added"] += 1
            continue
        if event["txid"] in seen:
            counts["duplicate_txid_in_run"] += 1
            continue
        seen.add(event["txid"])
        screened = screen_transaction(features, DetectorConfig())
        transactions.append({"txid": event["txid"], "event_id": event.get("event_id"), "session": run,
                             "received_at": event.get("received_at"), "source_mode": event.get("source_mode"),
                             "feature_set_version": FEATURE_SET_VERSION, **features,
                             "ruleset_version": screened["ruleset_version"], "is_flagged": screened["is_flagged"],
                             "signal_count": screened["signal_count"],
                             "rule_ids": [s["rule_id"] for s in screened["signals"]]})
    return transactions, events, counts


def _remove_replayed(lake: Path, partition: int, first: int, last: int) -> int:
    """Delete stream files of this partition whose batch started inside [first, last].

    A crashed run may have cut the same offsets into different batches (batches also close on a timer), so a replayed
    batch must remove every earlier file that starts within its range, not just the one with its own name.
    """
    removed = 0
    for folder, pattern in ((lake, "*/**/stream-p{p}-o*.parquet"), (lake / "_quality", "stream-p{p}-o*.json"),
                            (lake / "_quarantine", "*/stream-p{p}-o*.jsonl")):
        if not folder.is_dir():
            continue
        for path in folder.glob(pattern.format(p=partition)):
            start = int(path.name.split("-o", 1)[1].split(".", 1)[0])
            if first <= start <= last:
                path.unlink()
                removed += 1
    return removed


def write_batch(lake: Path, name: str, rows_by_table: dict[str, list[dict]]) -> dict:
    """Validate and write one micro-batch (stale files from earlier attempts must already be removed)."""
    report = {"schema_version": 1, "batch": name, "written_at": utc_now(), "tables": {}}
    for table_name in STREAM_TABLES:
        table = TABLES[table_name]
        good, bad, failures = validate(table, rows_by_table.get(table_name, []))
        partitions = defaultdict(list)
        for row in good:
            partitions[_partition(table, row[table.time_column])].append(row)
        for partition, members in sorted(partitions.items()):
            members.sort(key=lambda r: (r[table.time_column], r[table.key]))
            _write_parquet(lake / table_name / partition / f"{name}.parquet", table, members)
        quarantine = lake / "_quarantine" / table_name / f"{name}.jsonl"
        if bad:
            quarantine.parent.mkdir(parents=True, exist_ok=True)
            quarantine.write_text("".join(json.dumps(i, sort_keys=True, default=str) + "\n" for i in bad),
                                  encoding="utf-8", newline="\n")
        elif quarantine.exists():
            quarantine.unlink()
        report["tables"][table_name] = {"rows_in": len(rows_by_table.get(table_name, [])), "rows_written": len(good),
                                        "rows_quarantined": len(bad), "partitions": sorted(partitions),
                                        "failed_checks": dict(sorted(failures.items()))}
    path = lake / "_quality" / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    return report


def stream_to_lake(lake: Path, group_id: str, bootstrap_servers: str, *, consumer_factory=None, batch_messages: int = 500,
                   batch_seconds: float = 5.0, idle_timeout: float = 10.0, max_messages: int | None = None,
                   assignment_timeout: float = 60.0, metrics=None) -> dict:
    """Consume the enriched topic into the lake until idle (or max_messages). Safe to stop and restart."""
    from blockpulse.kafka_pipeline import ENRICHED_TOPIC, _kafka_types
    from blockpulse.kafka_workers import _consumer_lag, _is_group_change, _log
    if consumer_factory is None:
        consumer_factory = _kafka_types()[1]
    consumer = consumer_factory({
        "bootstrap.servers": bootstrap_servers, "group.id": group_id, "client.id": "blockpulse-stream-lake",
        "enable.auto.commit": False, "auto.offset.reset": "earliest",
        "session.timeout.ms": 10000, "heartbeat.interval.ms": 3000,
    })
    consumer.subscribe([ENRICHED_TOPIC])
    state = {"messages": set(), "txids_by_run": {}}
    batches = {}  # partition -> {"first": offset, "last_message": message, "rows": {table: [...]}, "count": n}
    totals = defaultdict(int)
    started, last_flush, idle_since = time.monotonic(), time.monotonic(), None

    def flush(partition: int) -> None:
        batch = batches.pop(partition)
        totals["stale_files_removed"] += _remove_replayed(lake, partition, batch["first"], batch["last_message"].offset())
        report = write_batch(lake, _batch_name(partition, batch["first"]), batch["rows"])
        for table_name, stats in report["tables"].items():
            totals[f"{table_name}_written"] += stats["rows_written"]
            totals[f"{table_name}_quarantined"] += stats["rows_quarantined"]
        totals["batches"] += 1
        try:
            consumer.commit(message=batch["last_message"], asynchronous=False)
        except Exception as error:  # the group changed: the batch will be redelivered and rewrite the same files
            if not _is_group_change(error):
                raise
            totals["commits_skipped_group_changed"] += 1
            _log("stream_lake_commit_skipped", detail=str(error)[:200])
        if metrics:
            metrics.batch_written(report, consumer)

    def flush_all() -> None:
        nonlocal last_flush
        for partition in list(batches):
            flush(partition)
        last_flush = time.monotonic()

    _log("stream_lake_started", group_id=group_id, topic=ENRICHED_TOPIC, lake=str(lake))
    try:
        while max_messages is None or totals["messages"] < max_messages:
            if batches and (time.monotonic() - last_flush >= batch_seconds
                            or any(b["count"] >= batch_messages for b in batches.values())):
                flush_all()
            message = consumer.poll(0.25)
            if message is None:
                now = time.monotonic()
                if not consumer.assignment():
                    if now - started >= assignment_timeout:
                        raise RuntimeError(f"stream-lake consumer was not assigned partitions within {assignment_timeout:g}s")
                    continue
                idle_since = idle_since or now
                if now - idle_since >= idle_timeout:
                    break
                continue
            idle_since = None
            if message.error() is not None:
                raise RuntimeError(f"Kafka enriched consumer error: {message.error()}")
            record = json.loads(message.value())
            transactions, events, counts = rows_from_enriched(record, state)
            batch = batches.setdefault(message.partition(), {"first": message.offset(), "count": 0,
                                                             "rows": {"transactions": [], "lifecycle_events": []}})
            batch["rows"]["transactions"].extend(transactions)
            batch["rows"]["lifecycle_events"].extend(events)
            batch["last_message"] = message
            batch["count"] += 1
            totals["messages"] += 1
            for key, value in counts.items():
                totals[key] += value
            if metrics:
                metrics.message_consumed(record)
        flush_all()  # normal exit: everything read is written, then committed
    finally:
        lag = _consumer_lag(consumer)
        consumer.close()
    summary = {"group_id": group_id, "lake": str(lake), **dict(totals), "consumer_lag_messages": lag,
               "elapsed_seconds": round(time.monotonic() - started, 2)}
    _log("stream_lake_finished", **summary)
    return summary
