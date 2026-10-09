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
from blockpulse.storage import StorageError, new_run_path


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
    args = parser.parse_args(argv)
    try:
        if args.command == "capture":
            from blockpulse.capture import CaptureConfig, capture
            config = CaptureConfig(
                url=args.url, subscription=args.subscription, duration=args.duration,
                max_messages=args.max_messages, max_bytes=args.max_bytes,
                max_reconnects=args.max_reconnects, open_timeout=args.open_timeout,
            )
            output = args.output or new_run_path(Path("data/raw"), "capture")
            summary = asyncio.run(capture(config, output))
            code = 130 if summary["stop_reason"] == "interrupted" else (0 if sum(summary["event_counts"].values()) else 3)
        else:
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
        print(json.dumps(summary, indent=2))
        print(f"Saved to: {output.resolve()}")
        return code
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError, StorageError) as error:
        print(f"BlockPulse: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
