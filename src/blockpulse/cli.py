"""Command-line entry points for capture and offline processing."""

import argparse
import asyncio
import json
from pathlib import Path
import sys

from blockpulse.process import process_capture
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
            output = args.output or new_run_path(Path("data/processed"), "features")
            summary = process_capture(args.source, output)
            code = 0 if summary["features_written"] else 3
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
