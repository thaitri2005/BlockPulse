"""Normalize captured messages without guessing unsupported provider shapes."""

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import re

EVENT_TYPES = ("added", "removed", "mined", "replaced")
CHANNELS = ("mempool-transactions", "mempool-txids")
TXID = re.compile(r"^[0-9a-fA-F]{64}$")


@dataclass
class ParsedMessage:
    events: list[dict] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
    sequences: dict[str, int] = field(default_factory=dict)
    recognized: bool = False


def stable_id(*parts: object) -> str:
    encoded = json.dumps(parts, separators=(",", ":"), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def reject_nonfinite(value: str):
    raise ValueError(f"Non-standard JSON constant: {value}")


def parse_record(record: dict) -> ParsedMessage:
    """A message can contain several lifecycle arrays, or just control data.

    Raw payloads remain in the capture even when their schema is unsupported.
    All normalized event times use receipt time, never a guessed first-seen time.
    """
    parsed = ParsedMessage()
    if not isinstance(record, dict) or record.get("schema_version") != 1:
        parsed.issues.append("Unsupported capture envelope/schema_version")
        return parsed
    for name in ("run_id", "session_id", "message_id", "received_at"):
        if not isinstance(record.get(name), str) or not record[name]:
            parsed.issues.append(f"Missing envelope field: {name}")
    if parsed.issues:
        return parsed
    try:
        timestamp = datetime.fromisoformat(record["received_at"].replace("Z", "+00:00"))
        if timestamp.utcoffset() is None:
            raise ValueError("timezone is required")
    except ValueError:
        parsed.issues.append("received_at must be an ISO timestamp with a timezone")
        return parsed
    if record.get("message_type", "text") != "text":
        parsed.issues.append("Binary message preserved; text JSON was expected")
        return parsed
    try:
        payload = json.loads(record["raw_text"], parse_constant=reject_nonfinite)
    except (KeyError, TypeError, ValueError):
        parsed.issues.append("Message is not valid JSON text")
        return parsed
    if not isinstance(payload, dict):
        parsed.issues.append("Message JSON must be an object")
        return parsed

    for channel in CHANNELS:
        if channel not in payload:
            continue
        parsed.recognized = True
        body = payload[channel]
        if not isinstance(body, dict):
            parsed.issues.append(f"{channel} must be an object")
            continue
        sequence = body.get("sequence")
        if type(sequence) is int and sequence >= 0:
            parsed.sequences[channel] = sequence
        elif sequence is not None:
            parsed.issues.append(f"{channel}.sequence is not a nonnegative integer")
        for kind in EVENT_TYPES:
            entries = body.get(kind, [])
            if not isinstance(entries, list):
                parsed.issues.append(f"{channel}.{kind} is not a list")
                continue
            for index, entry in enumerate(entries):
                candidate = entry.get("txid") if isinstance(entry, dict) else entry
                txid = candidate.lower() if isinstance(candidate, str) and TXID.fullmatch(candidate) else None
                if txid is None:
                    parsed.issues.append(f"{channel}.{kind}[{index}] has no supported txid; payload retained")
                parsed.events.append({
                    "schema_version": 1,
                    "event_id": stable_id(record["message_id"], channel, kind, index),
                    "run_id": record["run_id"],
                    "source_session_id": record["session_id"],
                    "source_message_id": record["message_id"],
                    "source_sequence": parsed.sequences.get(channel),
                    "source_mode": record.get("source_mode", "ws"),
                    "event_type": kind,
                    "txid": txid,
                    "event_time": record["received_at"],
                    "event_time_basis": "received_at",
                    "received_at": record["received_at"],
                    "payload": entry,
                })
    return parsed
