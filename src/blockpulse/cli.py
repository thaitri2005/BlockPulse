"""Command-line entry points for capture and offline processing."""

import argparse
import asyncio
import json
from pathlib import Path
import sys

from blockpulse.process import process_capture
from blockpulse.detect import detect_features
from blockpulse.detector import DetectorConfig
from blockpulse.lab import run_rule_lab
from blockpulse.kafka_pipeline import (
    DEFAULT_BOOTSTRAP_SERVERS,
    archive_topic,
    create_m1_topics,
    RawKafkaPublisher,
    publish_capture,
)
from blockpulse.storage import StorageError, new_run_path


def _kafka_call(label, action):
    try:
        return action()
    except (OSError, ValueError, RuntimeError):
        raise
    except Exception as error:
        raise RuntimeError(f"{label} failed: {error}") from error


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="blockpulse", description="Capture Bitcoin mempool observations and replay them locally.")
    commands = parser.add_subparsers(dest="command", required=True)
    capture_parser = commands.add_parser("capture", help="Record a bounded live WebSocket session")
    capture_parser.add_argument("--duration", type=float, default=60, help="Maximum capture time in seconds (default: 60)")
    capture_parser.add_argument("--output", type=Path, help="New output directory; existing paths are never overwritten")
    capture_parser.add_argument("--url", default="wss://mempool.space/api/v1/ws")
    capture_parser.add_argument("--subscription", choices=("track-mempool", "track-mempool-txids"), default="track-mempool")
    capture_parser.add_argument("--max-messages", type=int, default=10_000)
    capture_parser.add_argument("--max-bytes", type=int, default=50 * 1024 * 1024, help="Maximum messages.jsonl size in bytes")
    capture_parser.add_argument("--max-reconnects", type=int, default=3)
    capture_parser.add_argument("--open-timeout", type=float, default=15)
    capture_kafka = commands.add_parser("capture-kafka", help="Capture to local JSONL and publish the same envelopes to Kafka")
    capture_kafka.add_argument("--duration", type=float, default=60)
    capture_kafka.add_argument("--output", type=Path, help="New output directory; existing paths are never overwritten")
    capture_kafka.add_argument("--url", default="wss://mempool.space/api/v1/ws")
    capture_kafka.add_argument("--subscription", choices=("track-mempool", "track-mempool-txids"), default="track-mempool")
    capture_kafka.add_argument("--max-messages", type=int, default=10_000)
    capture_kafka.add_argument("--max-bytes", type=int, default=50 * 1024 * 1024)
    capture_kafka.add_argument("--max-reconnects", type=int, default=3)
    capture_kafka.add_argument("--open-timeout", type=float, default=15)
    capture_kafka.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    process_parser = commands.add_parser("process", help="Normalize a capture and write features without network access")
    process_parser.add_argument("source", type=Path, help="Path to messages.jsonl")
    process_parser.add_argument("--output", type=Path, help="New output directory")
    detect_parser = commands.add_parser("detect", help="Apply explainable, offline transaction-structure rules")
    detect_parser.add_argument("source", type=Path, help="Path to transaction-v2 features.jsonl")
    detect_parser.add_argument("--output", type=Path, help="New output directory")
    detect_parser.add_argument("--many-outputs-min", type=int, default=10, help="Output threshold for the fan-out shape rule")
    detect_parser.add_argument("--many-outputs-max-inputs", type=int, default=2)
    detect_parser.add_argument("--many-inputs-min", type=int, default=10, help="Input threshold for the fan-in shape rule")
    detect_parser.add_argument("--many-inputs-max-outputs", type=int, default=2)
    detect_parser.add_argument("--repeated-value-min", type=int, default=3, help="Equal-value output threshold")
    detect_parser.add_argument("--repeated-value-min-inputs", type=int, default=3)
    lab_parser = commands.add_parser("lab", help="Run controlled synthetic examples through the structural rules")
    lab_parser.add_argument("--output", type=Path, help="New output directory")
    kafka_init = commands.add_parser("kafka-init", help="Create the local raw, enriched, and error Kafka topics")
    kafka_init.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    kafka_replay = commands.add_parser("kafka-replay", help="Replay a saved messages.jsonl file into Kafka")
    kafka_replay.add_argument("source", type=Path)
    kafka_replay.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    kafka_archive = commands.add_parser("kafka-archive", help="Consume Kafka records into a restart-safe JSONL archive")
    kafka_archive.add_argument("--output", type=Path, required=True, help="Archive directory; reuse to resume")
    kafka_archive.add_argument("--group-id", required=True, help="Stable group ID for restart/resume")
    kafka_archive.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    kafka_archive.add_argument("--max-messages", type=int, help="Optional stop limit; otherwise stops after idle timeout")
    kafka_archive.add_argument("--idle-timeout", type=float, default=3.0)
    kafka_enrich = commands.add_parser("kafka-enrich", help="Enrich incomplete added transactions from the bounded REST API")
    kafka_enrich.add_argument("--group-id", required=True, help="Stable group ID for restart/resume")
    kafka_enrich.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    kafka_enrich.add_argument("--api-base", default="https://mempool.space/api")
    kafka_enrich.add_argument("--requests-per-second", type=float, default=1.0)
    kafka_enrich.add_argument("--max-requests", type=int, default=100)
    kafka_enrich.add_argument("--max-messages", type=int)
    kafka_enrich.add_argument("--idle-timeout", type=float, default=3.0)
    kafka_parquet = commands.add_parser("kafka-parquet", help="Archive enriched Kafka messages as restart-safe Parquet files")
    kafka_parquet.add_argument("--output", type=Path, required=True)
    kafka_parquet.add_argument("--group-id", required=True, help="Stable group ID for restart/resume")
    kafka_parquet.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    kafka_parquet.add_argument("--max-messages", type=int)
    kafka_parquet.add_argument("--idle-timeout", type=float, default=3.0)
    args = parser.parse_args(argv)
    try:
        if args.command in ("capture", "capture-kafka"):
            from blockpulse.capture import CaptureConfig, capture
            config = CaptureConfig(
                url=args.url, subscription=args.subscription, duration=args.duration,
                max_messages=args.max_messages, max_bytes=args.max_bytes,
                max_reconnects=args.max_reconnects, open_timeout=args.open_timeout,
            )
            output = args.output or new_run_path(Path("data/raw"), "capture")
            publisher = None
            if args.command == "capture-kafka":
                _kafka_call("Kafka topic initialization", lambda: create_m1_topics(args.bootstrap_servers))
                publisher = RawKafkaPublisher(args.bootstrap_servers)
            capture_error = None
            summary = None
            try:
                summary = asyncio.run(capture(config, output, on_message=publisher if publisher else None))
            except BaseException as error:
                capture_error = error
            if publisher is not None:
                try:
                    kafka_summary = publisher.close()
                except Exception as error:
                    kafka_summary = publisher.summary()
                    kafka_summary["status"] = "failed"
                    kafka_summary["error"] = str(error)
                    capture_error = capture_error or error
                if summary is None:
                    try:
                        summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
                    except (OSError, ValueError):
                        summary = None
                if summary is not None:
                    summary["kafka_publish"] = kafka_summary
                    from blockpulse.storage import write_summary
                    write_summary(output / "summary.json", summary)
            if capture_error is not None:
                raise capture_error
            code = 130 if summary["stop_reason"] == "interrupted" else (0 if sum(summary["event_counts"].values()) else 3)
        elif args.command in ("process", "detect", "lab"):
            if args.command == "process":
                output = args.output or new_run_path(Path("data/processed"), "features")
                summary = process_capture(args.source, output)
                code = 0 if summary["features_written"] else 3
            elif args.command == "detect":
                config = DetectorConfig(
                    many_outputs_min=args.many_outputs_min,
                    many_outputs_max_inputs=args.many_outputs_max_inputs,
                    many_inputs_min=args.many_inputs_min,
                    many_inputs_max_outputs=args.many_inputs_max_outputs,
                    repeated_value_min=args.repeated_value_min,
                    repeated_value_min_inputs=args.repeated_value_min_inputs,
                )
                output = args.output or new_run_path(Path("data/screened"), "detections")
                summary = detect_features(args.source, output, config)
                code = 0 if summary["rows_screened"] else 3
            else:
                output = args.output or new_run_path(Path("data/labs"), "rule-lab")
                summary = run_rule_lab(output)
                code = 0 if summary["cases_matching_expected_rules"] == summary["scenario_count"] else 4
        elif args.command == "kafka-init":
            output = None
            summary = _kafka_call("Kafka topic initialization", lambda: create_m1_topics(args.bootstrap_servers))
            code = 0
        elif args.command == "kafka-replay":
            output = None
            summary = _kafka_call(
                "Kafka capture replay", lambda: publish_capture(args.source, args.bootstrap_servers),
            )
            code = 0
        elif args.command == "kafka-enrich":
            from blockpulse.kafka_workers import enrich_topic
            from blockpulse.enrichment import RateLimitedEnricher
            output = None
            enricher = RateLimitedEnricher(
                api_base=args.api_base, requests_per_second=args.requests_per_second,
                max_requests=args.max_requests,
            )
            summary = _kafka_call("Kafka enrichment worker", lambda: enrich_topic(
                args.group_id, args.bootstrap_servers, enricher=enricher,
                max_messages=args.max_messages, idle_timeout=args.idle_timeout,
            ))
            code = 0
        elif args.command == "kafka-parquet":
            from blockpulse.parquet_archive import archive_enriched_to_parquet
            output = args.output
            summary = _kafka_call("Kafka Parquet archive", lambda: archive_enriched_to_parquet(
                args.output, args.group_id, args.bootstrap_servers,
                max_messages=args.max_messages, idle_timeout=args.idle_timeout,
            ))
            code = 0
        else:
            output = args.output
            summary = _kafka_call(
                "Kafka archive consumer",
                lambda: archive_topic(
                    args.output, args.group_id, args.bootstrap_servers,
                    max_messages=args.max_messages, idle_timeout=args.idle_timeout,
                ),
            )
            code = 0
        print(json.dumps(summary, indent=2))
        if output is not None:
            print(f"Saved to: {output.resolve()}")
        return code
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError, RuntimeError, StorageError) as error:
        print(f"BlockPulse: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
