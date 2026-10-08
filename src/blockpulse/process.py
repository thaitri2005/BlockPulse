"""Offline normalization and feature extraction; this module performs no HTTP."""

from collections import Counter
import csv
import hashlib
import json
from pathlib import Path

from blockpulse.events import EVENT_TYPES, parse_record, reject_nonfinite, stable_id
from blockpulse.features import FEATURE_NAMES, FEATURE_SET_VERSION, extract_features
from blockpulse.storage import JsonlWriter, write_summary


def process_capture(source: Path, output: Path) -> dict:
    """Preserve event observations, but write one feature row per unique txid.

    Invalid/ID-only records are reported and do not prevent later full records
    from producing features. Conflicting feature inputs are reported explicitly.
    """
    source = source.resolve()
    if not source.is_file():
        raise ValueError(f"Capture file does not exist: {source}")
    output.mkdir(parents=True, exist_ok=False)
    summary = {
        "schema_version": 1,
        "source_file": str(source),
        "feature_set_version": FEATURE_SET_VERSION,
        "lines_read": 0,
        "duplicate_messages": 0,
        "conflicting_messages": 0,
        "event_counts": {kind: 0 for kind in EVENT_TYPES},
        "unrecognized_messages": 0,
        "features_written": 0,
        "duplicate_added_transactions": 0,
        "conflicting_feature_rows": 0,
        "incomplete_added_transactions": 0,
        "issue_count": 0,
    }
    input_hash = hashlib.sha256()
    seen_messages = {}
    seen_features = {}
    unique_ids = set()
    field_presence = Counter()
    required = ("vin", "vout", "weight", "fee")
    csv_fields = ("txid", "event_id", "received_at", "source_mode", "feature_set_version", *FEATURE_NAMES)
    with (
        source.open("rb") as captured,
        JsonlWriter(output / "events.jsonl") as events_file,
        JsonlWriter(output / "features.jsonl") as features_file,
        JsonlWriter(output / "errors.jsonl") as errors_file,
        (output / "features.csv").open("x", encoding="utf-8", newline="") as csv_file,
    ):
        table = csv.DictWriter(csv_file, fieldnames=csv_fields)
        table.writeheader()

        def issue(kind: str, detail: str, **fields):
            summary["issue_count"] += 1
            errors_file.append({"line": summary["lines_read"], "kind": kind, "detail": detail, **fields})

        for raw_line in captured:
            summary["lines_read"] += 1
            input_hash.update(raw_line)
            try:
                record = json.loads(raw_line, parse_constant=reject_nonfinite)
            except (ValueError, UnicodeError):
                issue("invalid_jsonl", "Line is not valid UTF-8 JSON; source file preserved")
                continue
            parsed = parse_record(record)
            for detail in parsed.issues:
                issue("schema", detail)
            if not isinstance(record, dict):
                continue
            message_id = record.get("message_id")
            if isinstance(message_id, str) and message_id:
                content_hash = stable_id(record)
                previous = seen_messages.get(message_id)
                if previous is not None:
                    if previous == content_hash:
                        summary["duplicate_messages"] += 1
                    else:
                        summary["conflicting_messages"] += 1
                        issue("message_conflict", "Different content under the same message ID; first record kept", message_id=message_id)
                    continue
                seen_messages[message_id] = content_hash
            summary["unrecognized_messages"] += not parsed.recognized
            for event in parsed.events:
                events_file.append(event)
                summary["event_counts"][event["event_type"]] += 1
                if event["txid"]:
                    unique_ids.add(event["txid"])
                if event["event_type"] != "added":
                    continue
                payload = event["payload"]
                for name in required:
                    if isinstance(payload, dict) and name in payload:
                        field_presence[name] += 1
                if event["txid"] is None:
                    summary["incomplete_added_transactions"] += 1
                    continue
                try:
                    features = extract_features(payload)
                except ValueError as error:
                    summary["incomplete_added_transactions"] += 1
                    issue("missing_or_invalid_feature_input", str(error), event_id=event["event_id"], txid=event["txid"])
                    continue
                previous = seen_features.get(event["txid"])
                if previous is not None:
                    if previous == features:
                        summary["duplicate_added_transactions"] += 1
                    else:
                        summary["conflicting_feature_rows"] += 1
                        issue("feature_conflict", "Different features for one txid; first valid row kept", txid=event["txid"])
                    continue
                seen_features[event["txid"]] = features
                row = {
                    "txid": event["txid"],
                    "event_id": event["event_id"],
                    "received_at": event["received_at"],
                    "source_mode": event["source_mode"],
                    "feature_set_version": FEATURE_SET_VERSION,
                    **features,
                }
                features_file.append(row)
                table.writerow(row)
                summary["features_written"] += 1
    summary["input_sha256"] = input_hash.hexdigest()
    summary["unique_transaction_ids"] = len(unique_ids)
    summary["added_field_presence"] = {name: field_presence[name] for name in required}
    summary["coverage_note"] = "Counts describe this file only; they do not establish full mempool coverage."
    write_summary(output / "summary.json", summary)
    return summary
