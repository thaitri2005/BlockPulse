"""Load tests on recorded data: how fast is each stage, and how much latency does the streaming path add?

Batch path: the per-session work `process-pending` does (process, screen, lake), timed stage by stage.
Streaming path: replay captures into Kafka while the enrichment worker runs, then read both topics back and
join raw and enriched records by message_id. Latency comes from Kafka's own record timestamps: the time the
raw record was appended versus the time its enriched record was appended.

Nothing here contacts the public source or its REST API. Results are written as JSON reports with the host
and library versions, because a throughput figure means nothing without the machine it came from.
"""

import json
import os
import platform
import shutil
import statistics
import sys
import threading
import time
from pathlib import Path

from blockpulse.storage import utc_now


def _peak_memory_bytes():
    from blockpulse.model import _peak_working_set_bytes
    return _peak_working_set_bytes()


def _environment() -> dict:
    import pyarrow
    return {"python": sys.version.split()[0], "platform": platform.platform(), "machine": platform.machine(),
            "processor": platform.processor(), "cpu_count": os.cpu_count(), "pyarrow": pyarrow.__version__}


def _percentiles(values: list[float]) -> dict:
    if not values:
        return {}
    ordered = sorted(values)

    def pick(q):
        return round(ordered[min(len(ordered) - 1, int(q * len(ordered)))], 2)
    return {"count": len(ordered), "p50": pick(0.50), "p90": pick(0.90), "p99": pick(0.99),
            "max": round(ordered[-1], 2), "mean": round(statistics.fmean(ordered), 2)}


def _link_or_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(source, target)
    except OSError:
        shutil.copyfile(source, target)


# ---------------------------------------------------------------- batch

def benchmark_batch(raw_sessions: list[Path], work: Path) -> dict:
    """Run process, screen and lake for each session into a scratch root and time every stage."""
    from blockpulse.detect import detect_features
    from blockpulse.lake import build_session
    from blockpulse.process import process_capture

    if not raw_sessions:
        raise ValueError("no sessions given")
    work.mkdir(parents=True, exist_ok=False)
    stages = {"process": 0.0, "screen": 0.0, "lake": 0.0}
    totals = {"sessions": 0, "raw_bytes": 0, "messages": 0, "events": 0, "transactions": 0, "lake_rows": 0}
    per_session = []
    for raw in raw_sessions:
        name = raw.name
        _link_or_copy(raw / "messages.jsonl", work / "raw" / name / "messages.jsonl")
        shutil.copyfile(raw / "summary.json", work / "raw" / name / "summary.json")
        timings = {}
        started = time.perf_counter()
        processed = process_capture(work / "raw" / name / "messages.jsonl", work / "processed" / name)
        timings["process"] = time.perf_counter() - started
        started = time.perf_counter()
        detect_features(work / "processed" / name / "features.jsonl", work / "screened" / name)
        timings["screen"] = time.perf_counter() - started
        started = time.perf_counter()
        report = build_session(work, name)
        timings["lake"] = time.perf_counter() - started
        for stage, seconds in timings.items():
            stages[stage] += seconds
        totals["sessions"] += 1
        totals["raw_bytes"] += (raw / "messages.jsonl").stat().st_size
        totals["messages"] += processed["lines_read"]
        totals["events"] += sum(processed["event_counts"].values())
        totals["transactions"] += processed["features_written"]
        totals["lake_rows"] += sum(t["rows_written"] for t in report["tables"].values())
        per_session.append({"session": name, **{k: round(v, 3) for k, v in timings.items()},
                            "transactions": processed["features_written"]})
    elapsed = sum(stages.values())
    return {
        "schema_version": 1, "kind": "batch", "created_at": utc_now(), "environment": _environment(),
        "input": {**totals, "first_session": raw_sessions[0].name, "last_session": raw_sessions[-1].name},
        "seconds": {k: round(v, 2) for k, v in stages.items()} | {"total": round(elapsed, 2)},
        "throughput": {
            "raw_mb_per_second": round(totals["raw_bytes"] / 2**20 / elapsed, 2),
            "messages_per_second": round(totals["messages"] / elapsed, 1),
            "events_per_second": round(totals["events"] / elapsed, 1),
            "transactions_per_second": round(totals["transactions"] / elapsed, 1),
            "process_events_per_second": round(totals["events"] / stages["process"], 1),
            "screen_transactions_per_second": round(totals["transactions"] / stages["screen"], 1),
            "lake_rows_per_second": round(totals["lake_rows"] / stages["lake"], 1),
            "capture_hours_per_wall_minute": round(totals["sessions"] / (elapsed / 60), 2),
        },
        "session_seconds": _percentiles([sum(s[k] for k in ("process", "screen", "lake")) for s in per_session]),
        "peak_process_memory_mb": round((_peak_memory_bytes() or 0) / 2**20, 1),
        "per_session": per_session,
    }


# ---------------------------------------------------------------- streaming

def _offline_fetch(txid):
    raise RuntimeError("the benchmark never calls the public API")


def _read_topic_timestamps(bootstrap: str, topic: str, *, timeout: float = 30.0) -> dict[str, int]:
    """message_id -> Kafka append timestamp (ms), reading the whole topic with a throwaway consumer."""
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"blockpulse-bench-reader-{time.time_ns()}",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    metadata = consumer.list_topics(topic, timeout=10)
    partitions = [TopicPartition(topic, p, 0) for p in metadata.topics[topic].partitions]
    ends = {p.partition: consumer.get_watermark_offsets(p, timeout=10)[1] for p in partitions}
    consumer.assign(partitions)
    found, done, deadline = {}, {p for p, end in ends.items() if end == 0}, time.monotonic() + timeout
    while len(done) < len(partitions) and time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None or message.error():
            continue
        row = json.loads(message.value())
        found[row.get("message_id")] = message.timestamp()[1]
        if message.offset() + 1 >= ends[message.partition()]:
            done.add(message.partition())
        deadline = time.monotonic() + timeout
    consumer.close()
    return found


def benchmark_streaming(raw_sessions: list[Path], bootstrap: str) -> dict:
    """Replay sessions into an empty raw topic while the enrichment worker consumes it; report throughput and latency.

    Expects a fresh broker (see docs/guides/09-load-test.md): it reads whole topics back to compute latency.
    """
    from blockpulse.enrichment import RateLimitedEnricher
    from blockpulse.kafka_pipeline import ENRICHED_TOPIC, RAW_TOPIC, create_m1_topics, publish_capture
    from blockpulse.kafka_workers import enrich_topic

    create_m1_topics(bootstrap)
    enricher = RateLimitedEnricher(fetch=_offline_fetch)
    result = {}

    def consume():
        result["enrich"] = enrich_topic(f"blockpulse-bench-{time.time_ns()}", bootstrap, enricher=enricher,
                                        idle_timeout=15.0, assignment_timeout=60.0)

    worker = threading.Thread(target=consume)
    started = time.monotonic()
    worker.start()
    published = 0
    for raw in raw_sessions:
        published += publish_capture(raw / "messages.jsonl", bootstrap)["messages_published"]
    publish_seconds = time.monotonic() - started
    worker.join()
    total_seconds = time.monotonic() - started
    raw_times = _read_topic_timestamps(bootstrap, RAW_TOPIC)
    enriched_times = _read_topic_timestamps(bootstrap, ENRICHED_TOPIC)
    latencies = [(enriched_times[mid] - raw_times[mid]) for mid in raw_times if mid in enriched_times]
    enrich = result.get("enrich", {})
    return {
        "schema_version": 1, "kind": "streaming", "created_at": utc_now(), "environment": _environment(),
        "input": {"sessions": len(raw_sessions), "first_session": raw_sessions[0].name,
                  "last_session": raw_sessions[-1].name, "messages_published": published,
                  "raw_bytes": sum((r / "messages.jsonl").stat().st_size for r in raw_sessions)},
        "seconds": {"publish": round(publish_seconds, 2), "until_enrichment_idle": round(total_seconds, 2)},
        "throughput": {
            "publish_messages_per_second": round(published / publish_seconds, 1),
            "enrich_messages_per_second": enrich.get("messages_per_second"),
            "enrich_events": enrich.get("events_emitted"),
            "capture_hours_per_wall_minute": round(len(raw_sessions) / (total_seconds / 60), 2),
        },
        "enrichment": {k: enrich.get(k) for k in ("messages_consumed", "events_emitted", "enrichment_requests",
                                                  "error_records", "consumer_lag_messages")},
        "end_to_end_latency_ms": _percentiles(latencies),
        "latency_note": "Kafka append time of the enriched record minus that of its raw record. Under a replay the "
                        "whole day arrives at once, so this measures queueing behind the backlog, not steady-state latency.",
        "matched_messages": len(latencies),
    }


def write_report(report: dict, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{report['kind']}-{report['created_at'][:19].replace(':', '').replace('-', '')}.json"
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    return path
