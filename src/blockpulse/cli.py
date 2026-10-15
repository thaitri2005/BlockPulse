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
    commands.add_parser("db-init", help="Apply pending Postgres migrations (uses BLOCKPULSE_DSN or the local default)")
    load_results = commands.add_parser("load-results", help="Load screened.jsonl into Postgres (safe to repeat)")
    load_results.add_argument("source", type=Path, help="A `detect` output directory or its screened.jsonl")
    load_results.add_argument("--run-id", help="Identity of this result set; default is the directory name")
    load_events_parser = commands.add_parser("load-events", help="Load lifecycle events into Postgres and refresh transaction status (safe to repeat)")
    load_events_parser.add_argument("source", type=Path, help="A `process` output directory or its events.jsonl")
    serve = commands.add_parser("serve", help="Run the read-only API (localhost only unless --host is changed)")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    trace = commands.add_parser("trace", help="Show everything stored about one txid; with --capture, re-derive its features from the archive")
    trace.add_argument("txid")
    trace.add_argument("--capture", type=Path, help="The messages.jsonl the data was loaded from")
    trace.add_argument("--brief", action="store_true", help="Print a short human summary instead of the full JSON")
    build_dataset = commands.add_parser("build-dataset", help="Build a checksummed chronological train/calibration/test manifest from screened sessions")
    build_dataset.add_argument("--root", type=Path, default=Path("data/screened"), help="Directory of screened run folders")
    build_dataset.add_argument("--output", type=Path, help="Manifest path; default data/datasets/dataset-<id>.json")
    build_dataset.add_argument("--min-rows", type=int, default=100, help="Exclude sessions with fewer rows")
    train = commands.add_parser("train-iforest", help="Fit an Isolation Forest on a dataset manifest's train split")
    train.add_argument("manifest", type=Path, help="Manifest from build-dataset")
    train.add_argument("--root", type=Path, default=Path("data/screened"))
    train.add_argument("--output", type=Path, help="New model directory; default data/models/iforest-<manifest id>-seed<seed>")
    train.add_argument("--seed", type=int, default=7)
    train.add_argument("--trees", type=int, default=100)
    train.add_argument("--max-train-rows", type=int, default=50_000)
    evaluate = commands.add_parser("evaluate-iforest", help="Calibrate an alert threshold, score the test split, and compare with the rules")
    evaluate.add_argument("manifest", type=Path)
    evaluate.add_argument("model", type=Path, help="Model directory from train-iforest")
    evaluate.add_argument("--root", type=Path, default=Path("data/screened"))
    evaluate.add_argument("--alerts-per-1000", type=float, default=25.0, help="Alert budget on calibration traffic")
    evaluate.add_argument("--output", type=Path, help="New directory; default data/evaluations/<model name>-budget<N>")
    sweep = commands.add_parser("sweep", help="Run labeled synthetic patterns through the rules and the calibrated model")
    sweep.add_argument("manifest", type=Path)
    sweep.add_argument("model", type=Path, help="Model directory from train-iforest")
    sweep.add_argument("evaluation", type=Path, help="Directory from evaluate-iforest (supplies the calibrated threshold)")
    sweep.add_argument("--root", type=Path, default=Path("data/screened"))
    sweep.add_argument("--seed", type=int, default=11)
    sweep.add_argument("--output", type=Path, help="New directory; default data/sweeps/<evaluation name>-seed<N>")
    detectors = commands.add_parser("detectors", help="Package, register, promote, and roll back detector bundles")
    detectors.add_argument("--root", type=Path, default=Path("data/detectors"))
    actions = detectors.add_subparsers(dest="action", required=True)
    pkg_rules = actions.add_parser("package-rules", help="Bundle and register the rule baseline")
    pkg_rules.add_argument("--many-outputs-min", type=int, default=10)
    pkg_rules.add_argument("--many-inputs-min", type=int, default=10)
    pkg_model = actions.add_parser("package-model", help="Bundle and register a trained, calibrated, swept model")
    pkg_model.add_argument("manifest", type=Path)
    pkg_model.add_argument("model", type=Path)
    pkg_model.add_argument("evaluation", type=Path)
    pkg_model.add_argument("sweep", type=Path)
    pkg_model.add_argument("--screened-root", type=Path, default=Path("data/screened"))
    promote_cmd = actions.add_parser("promote", help="Make a registered detector the champion if it passes its gate")
    promote_cmd.add_argument("detector_id")
    promote_cmd.add_argument("--reason", required=True)
    rollback_cmd = actions.add_parser("rollback", help="Return to the previous champion")
    rollback_cmd.add_argument("--reason", required=True)
    actions.add_parser("status", help="Show registered detectors, the champion, and recent events")
    collect_parser = commands.add_parser("collect", help="One capture, then process (and optionally load) every finished session")
    collect_parser.add_argument("--root", type=Path, default=Path("data"))
    collect_parser.add_argument("--duration", type=float, default=3540)
    collect_parser.add_argument("--url", default="wss://node203.sv1.mempool.space/api/v1/ws")
    collect_parser.add_argument("--prefix", default="session", help="Session folder prefix; must start with 'session'")
    collect_parser.add_argument("--load-db", action="store_true", help="Also load results into Postgres (BLOCKPULSE_DSN)")
    collect_parser.add_argument("--min-free-gib", type=float, default=15.0)
    loop_parser = commands.add_parser("collect-loop", help="Capture sessions back to back until a deadline or limit")
    loop_parser.add_argument("--root", type=Path, default=Path("data"))
    loop_parser.add_argument("--session-seconds", type=float, default=3600)
    loop_parser.add_argument("--url", default="wss://node203.sv1.mempool.space/api/v1/ws")
    loop_parser.add_argument("--prefix", default="session")
    loop_parser.add_argument("--until", help="Stop at this UTC time (ISO 8601), for example 2026-10-19T04:00:00Z")
    loop_parser.add_argument("--max-sessions", type=int)
    loop_parser.add_argument("--min-free-gib", type=float, default=15.0)
    pending_parser = commands.add_parser("process-pending", help="Process, screen, compact (and optionally load) every finished capture")
    pending_parser.add_argument("--root", type=Path, default=Path("data"))
    pending_parser.add_argument("--load-db", action="store_true", help="Also load into Postgres (BLOCKPULSE_DSN); safe to repeat")
    sync_parser = commands.add_parser("sync-pending", help="Upload each processed session to S3 once, plus a status file")
    sync_parser.add_argument("--root", type=Path, default=Path("data"))
    sync_parser.add_argument("--bucket", required=True)
    sync_parser.add_argument("--prefix", default="data")
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
        elif args.command == "db-init":
            from blockpulse import db
            output = None

            def run_db_init():
                with db.connect() as connection:
                    return db.apply_migrations(connection)
            summary = _kafka_call("Database initialization", run_db_init)
            code = 0
        elif args.command == "load-results":
            from blockpulse import db
            from blockpulse.results import load_screened
            output = None
            run_id = args.run_id or (args.source if args.source.is_dir() else args.source.parent).resolve().name

            def run_load():
                with db.connect() as connection:
                    return load_screened(connection, args.source, run_id)
            summary = _kafka_call("Result load", run_load)
            code = 0 if summary["conflicts"] == 0 and summary["invalid"] == 0 else 4
        elif args.command == "load-events":
            from blockpulse import db
            from blockpulse.lifecycle import load_events
            output = None

            def run_load_events():
                with db.connect() as connection:
                    return load_events(connection, args.source)
            summary = _kafka_call("Lifecycle load", run_load_events)
            code = 0 if summary["invalid"] == 0 else 4
        elif args.command == "serve":
            try:
                import uvicorn
            except ImportError as error:
                raise RuntimeError('uvicorn is not installed; run: pip install -e ".[m2]"') from error
            uvicorn.run("blockpulse.api:create_default_app", factory=True, host=args.host, port=args.port)
            return 0
        elif args.command == "trace":
            from blockpulse import db
            from blockpulse.trace import trace_transaction
            output = None

            def run_trace():
                with db.connect() as connection:
                    return trace_transaction(connection, args.txid.lower(), args.capture)
            summary = _kafka_call("Transaction trace", run_trace)
            check = summary["replay_check"]
            if args.brief:
                flagged = [s["is_flagged"] for s in summary["scores"]]
                print(f"status: {summary['transaction']['lifecycle_status']}; scores: {len(flagged)} ({sum(flagged)} flagged); "
                      f"lifecycle events: {', '.join(e['event_type'] for e in summary['lifecycle_events']) or 'none'}")
                if check is not None:
                    print(f"archived features match stored features: {check['matches_stored_features']}")
            else:
                print(json.dumps(summary, indent=2, default=str))
            return 0 if check is None or check["matches_stored_features"] is not False else 4
        elif args.command == "build-dataset":
            from blockpulse.dataset import build_manifest, write_manifest
            output = None
            manifest = build_manifest(args.root, min_rows=args.min_rows)
            path = args.output or Path("data/datasets") / f"dataset-{manifest['manifest_id'][:12]}.json"
            created = write_manifest(manifest, path)
            summary = {"manifest": str(path), "created": created, "manifest_id": manifest["manifest_id"],
                       "splits": manifest["splits"], "excluded": manifest["excluded"]}
            code = 0
        elif args.command == "train-iforest":
            try:
                from blockpulse.model import train_isolation_forest
            except ImportError as error:
                raise RuntimeError('scikit-learn is not installed; run: pip install -e ".[ml]"') from error
            manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
            output = args.output or Path("data/models") / f"iforest-{manifest['manifest_id'][:12]}-seed{args.seed}"
            summary = train_isolation_forest(manifest, args.root, output, seed=args.seed, n_estimators=args.trees,
                                             max_train_rows=args.max_train_rows)
            code = 0
        elif args.command == "evaluate-iforest":
            try:
                from blockpulse.evaluate import evaluate_detector
            except ImportError as error:
                raise RuntimeError('scikit-learn is not installed; run: pip install -e ".[ml]"') from error
            manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
            output = args.output or Path("data/evaluations") / f"{args.model.name}-budget{args.alerts_per_1000:g}"
            summary = evaluate_detector(manifest, args.root, args.model, output, alerts_per_1000=args.alerts_per_1000)
            code = 0
        elif args.command == "sweep":
            try:
                from blockpulse.sweep import run_sweep
            except ImportError as error:
                raise RuntimeError('scikit-learn is not installed; run: pip install -e ".[ml]"') from error
            manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
            output = args.output or Path("data/sweeps") / f"{args.evaluation.name}-seed{args.seed}"
            summary = run_sweep(manifest, args.root, args.model, args.evaluation, output, seed=args.seed)
            code = 0
        elif args.command == "detectors":
            from blockpulse import registry
            output = None
            args.root.mkdir(parents=True, exist_ok=True)
            code = 0
            if args.action == "package-rules":
                config = DetectorConfig(many_outputs_min=args.many_outputs_min, many_inputs_min=args.many_inputs_min)
                directory = registry.package_rules(args.root, config)
                summary = {"bundle": str(directory), "registered": registry.register(args.root, directory)}
            elif args.action == "package-model":
                manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
                directory = registry.package_model(manifest, args.screened_root, args.model, args.evaluation, args.sweep, args.root)
                summary = {"bundle": str(directory), "registered": registry.register(args.root, directory)}
            elif args.action == "promote":
                summary = registry.promote(args.root, args.detector_id, args.reason)
                code = 0 if summary["promoted"] or summary.get("note") else 4
            elif args.action == "rollback":
                summary = registry.rollback(args.root, args.reason)
            else:
                events = registry.read_events(args.root)
                current = registry.state(events)
                summary = {"champion": current["champion"], "previous_champions": current["previous_champions"],
                           "registered": {k: v["family"] for k, v in current["registered"].items()}, "recent_events": events[-8:]}
        elif args.command in ("collect", "collect-loop", "process-pending", "sync-pending"):
            from datetime import datetime
            from blockpulse import collector
            output = None
            layout = collector.Layout(args.root)
            if args.command == "collect":
                summary = collector.collect(layout, duration=args.duration, url=args.url, prefix=args.prefix,
                                            load_db=args.load_db, min_free_gib=args.min_free_gib)
                code = 130 if summary["capture"]["stop_reason"] == "interrupted" else (4 if summary["processing"]["failed"] else 0)
            elif args.command == "collect-loop":
                until = datetime.fromisoformat(args.until.replace("Z", "+00:00")) if args.until else None
                if until is not None and until.tzinfo is None:
                    raise ValueError("--until needs a time zone, for example 2026-10-19T04:00:00Z")
                summary = collector.collect_loop(layout, session_seconds=args.session_seconds, url=args.url, prefix=args.prefix,
                                                 until=until, max_sessions=args.max_sessions, min_free_gib=args.min_free_gib)
                code = 130 if summary["stop_reason"] == "interrupted" else 0
            elif args.command == "process-pending":
                summary = collector.process_pending(layout, load_db=args.load_db)
                code = 0 if not summary["failed"] else 4
            else:
                summary = collector.sync_pending(layout, bucket=args.bucket, prefix=args.prefix)
                code = 0 if not summary["failed"] else 4
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
