"""Bounded WebSocket capture with raw preservation and explicit connection gaps."""

import asyncio
import base64
from collections import Counter
from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
import random
import time
from urllib.parse import urlsplit
import uuid

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from blockpulse.events import EVENT_TYPES, parse_record
from blockpulse.storage import JsonlWriter, utc_now, write_summary


@dataclass(frozen=True)
class CaptureConfig:
    url: str = "wss://mempool.space/api/v1/ws"
    subscription: str = "track-mempool"
    duration: float = 60
    max_messages: int = 10_000
    max_bytes: int = 50 * 1024 * 1024
    max_message_bytes: int = 4 * 1024 * 1024
    max_reconnects: int = 3
    open_timeout: float = 15

    def __post_init__(self):
        address = urlsplit(self.url)
        if address.scheme not in ("ws", "wss") or not address.hostname:
            raise ValueError("URL must be a ws:// or wss:// endpoint")
        if self.subscription not in ("track-mempool", "track-mempool-txids"):
            raise ValueError("Unsupported subscription")
        for name in ("duration", "open_timeout"):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be finite and greater than zero")
        for name in ("max_messages", "max_bytes", "max_message_bytes"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if type(self.max_reconnects) is not int or self.max_reconnects < 0:
            raise ValueError("max_reconnects must be a nonnegative integer")


async def capture(config: CaptureConfig, output: Path, *, connector=connect, on_message=None) -> dict:
    """Create a new capture directory. Never append to or overwrite an old run."""
    output.mkdir(parents=True, exist_ok=False)
    run_id = uuid.uuid4().hex
    started_at = utc_now()
    started = time.monotonic()
    deadline = started + config.duration
    summary = {
        "schema_version": 1,
        "run_id": run_id,
        "source_mode": "ws",
        "started_at": started_at,
        "config": asdict(config),
        "connection_attempts": 0,
        "connections": 0,
        "connection_errors": 0,
        "messages_received": 0,
        "messages_saved": 0,
        "payload_bytes_saved": 0,
        "raw_file_bytes": 0,
        "messages_with_parse_issues": 0,
        "unrecognized_messages": 0,
        "sequence_discontinuities": 0,
        "event_counts": {kind: 0 for kind in EVENT_TYPES},
        "gaps": [],
        "coverage_note": "Source observations only. Reconnects do not recover missed history.",
    }
    unique_ids = set()
    gap_start = started_at
    reason = "duration"
    last_progress = started

    with JsonlWriter(output / "messages.jsonl") as raw, JsonlWriter(output / "connections.jsonl") as journal:
        def note(kind: str, **fields):
            journal.append({"record_type": kind, "at": utc_now(), **fields})

        note("capture_started", run_id=run_id, config=asdict(config))
        try:
            while time.monotonic() < deadline:
                if summary["connection_attempts"] >= config.max_reconnects + 1:
                    reason = "retry_limit"
                    break
                summary["connection_attempts"] += 1
                session_id = uuid.uuid4().hex
                sequences = {}
                note("connecting", session_id=session_id)
                print(f"Connecting: attempt {summary['connection_attempts']}", flush=True)
                try:
                    async with asyncio.timeout(max(0.001, deadline - time.monotonic())):
                        async with connector(
                            config.url,
                            open_timeout=min(config.open_timeout, max(0.001, deadline - time.monotonic())),
                            close_timeout=1,
                            ping_interval=20,
                            ping_timeout=20,
                            max_size=config.max_message_bytes,
                            max_queue=8,
                            user_agent_header="BlockPulse/0.1 source-observation",
                        ) as websocket:
                            await websocket.send(json.dumps({config.subscription: True}))
                            connected_at = utc_now()
                            summary["connections"] += 1
                            if gap_start is not None:
                                summary["gaps"].append({"start": gap_start, "end": connected_at, "history_recovered": False})
                                gap_start = None
                            note("subscribed", session_id=session_id, subscription=config.subscription)
                            print(f"Subscribed to {config.subscription}; saving original messages.", flush=True)
                            while time.monotonic() < deadline:
                                message = await websocket.recv()
                                summary["messages_received"] += 1
                                record = {
                                    "schema_version": 1,
                                    "run_id": run_id,
                                    "session_id": session_id,
                                    "message_id": f"{run_id}:{summary['messages_received']}",
                                    "received_at": utc_now(),
                                    "source_mode": "ws",
                                    "message_type": "text" if isinstance(message, str) else "binary",
                                }
                                if isinstance(message, str):
                                    record["raw_text"] = message
                                    payload_size = len(message.encode("utf-8"))
                                else:
                                    record["raw_base64"] = base64.b64encode(message).decode("ascii")
                                    payload_size = len(message)
                                if not raw.append(record, max_bytes=config.max_bytes):
                                    reason = "byte_limit"
                                    note("message_not_saved", message_id=record["message_id"], reason=reason)
                                    break
                                summary["messages_saved"] += 1
                                summary["payload_bytes_saved"] += payload_size
                                # The raw envelope is flushed locally before an optional stream sink sees it.
                                if on_message is not None:
                                    on_message(record)
                                parsed = parse_record(record)
                                summary["messages_with_parse_issues"] += bool(parsed.issues)
                                summary["unrecognized_messages"] += not parsed.recognized
                                counts = Counter(event["event_type"] for event in parsed.events)
                                for kind, count in counts.items():
                                    summary["event_counts"][kind] += count
                                unique_ids.update(event["txid"] for event in parsed.events if event["txid"])
                                for channel, sequence in parsed.sequences.items():
                                    previous = sequences.get(channel)
                                    if previous is not None and sequence != previous + 1:
                                        summary["sequence_discontinuities"] += 1
                                        note("sequence_discontinuity", session_id=session_id, channel=channel,
                                             previous=previous, observed=sequence,
                                             interpretation="Possible gap, repeat, or reset; history not reconstructed")
                                    sequences[channel] = sequence
                                now = time.monotonic()
                                if summary["messages_saved"] == 1 or now - last_progress >= 5:
                                    print(f"Saved {summary['messages_saved']} messages; events={summary['event_counts']}", flush=True)
                                    last_progress = now
                                if summary["messages_saved"] >= config.max_messages:
                                    reason = "message_limit"
                                    break
                        if reason != "duration":
                            break
                except (OSError, TimeoutError, WebSocketException) as error:
                    if time.monotonic() >= deadline:
                        break
                    summary["connection_errors"] += 1
                    gap_start = gap_start or utc_now()
                    note("connection_error", session_id=session_id, error_type=type(error).__name__, error=str(error))
                    print(f"Connection ended: {type(error).__name__}: {error}", flush=True)
                    if summary["connection_attempts"] <= config.max_reconnects:
                        delay = min(2 ** (summary["connection_attempts"] - 1) + random.uniform(0, 0.25), 10)
                        await asyncio.sleep(min(delay, max(0, deadline - time.monotonic())))
        except asyncio.CancelledError:
            reason = "interrupted"
        except Exception:
            reason = "failed"
            raise
        finally:
            ended_at = utc_now()
            if gap_start is not None:
                summary["gaps"].append({"start": gap_start, "end": ended_at, "history_recovered": False})
            summary.update({
                "ended_at": ended_at,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "stop_reason": reason,
                "unique_transaction_ids": len(unique_ids),
                "raw_file_bytes": raw.bytes_written,
            })
            elapsed = max(summary["elapsed_seconds"], 0.001)
            summary["messages_per_minute"] = round(summary["messages_saved"] * 60 / elapsed, 3)
            summary["events_per_minute"] = round(sum(summary["event_counts"].values()) * 60 / elapsed, 3)
            note("capture_stopped", reason=reason)
            write_summary(output / "summary.json", summary)
    return summary
