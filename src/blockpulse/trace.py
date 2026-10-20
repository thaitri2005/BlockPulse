"""Follow one transaction from stored results back to its archived observation and re-derive its features."""

import json
from pathlib import Path

from blockpulse.events import parse_record, reject_nonfinite
from blockpulse.features import FEATURE_NAMES, extract_features
from blockpulse.storage import capture_path, open_capture


def find_archived_features(capture: Path, message_ids: set[str], txid: str) -> dict:
    """Locate the raw capture lines for these messages and recompute the txid's features from them.

    Scans the capture file once. Returns where the evidence was found and the recomputed features;
    nothing is fetched from the network.
    """
    capture = capture_path(capture)
    found = {"capture_file": str(capture.resolve()), "messages": {}, "recomputed_features": None, "problem": None}
    with open_capture(capture) as lines:
        for line_number, raw in enumerate(lines, start=1):
            try:
                record = json.loads(raw, parse_constant=reject_nonfinite)
            except ValueError:
                continue
            message_id = record.get("message_id") if isinstance(record, dict) else None
            if message_id not in message_ids:
                continue
            found["messages"][message_id] = {"line": line_number, "received_at": record.get("received_at")}
            for event in parse_record(record).events:
                if event["event_type"] == "added" and event["txid"] == txid and found["recomputed_features"] is None:
                    try:
                        found["recomputed_features"] = extract_features(event["payload"])
                    except ValueError as error:
                        found["problem"] = f"archived payload cannot produce features: {error}"
    missing = sorted(message_ids - set(found["messages"]))
    if missing:
        found["problem"] = f"{len(missing)} source message(s) not found in this capture file"
    return found


def trace_transaction(connection, txid: str, capture: Path | None = None) -> dict:
    """Everything stored about one txid, plus (with a capture file) a check that replay reproduces it."""
    from psycopg.rows import dict_row
    cursor = connection.cursor(row_factory=dict_row)
    transaction = cursor.execute("SELECT * FROM transactions WHERE txid = %s", (txid,)).fetchone()
    if transaction is None:
        raise ValueError("No stored record for this txid (never observed, not loaded, or expired)")
    scores = cursor.execute(
        "SELECT id, run_id, feature_set_version, ruleset_version, features, signals, is_flagged, source_event_id "
        "FROM scores WHERE txid = %s ORDER BY received_at, id", (txid,)).fetchall()
    events = cursor.execute(
        "SELECT event_id, event_type, event_time, source_message_id, replacement_txid "
        "FROM lifecycle_events WHERE txid = %s ORDER BY event_time, event_id", (txid,)).fetchall()
    result = {"transaction": transaction, "scores": scores, "lifecycle_events": events, "replay_check": None}
    if capture is not None:
        archived = find_archived_features(capture, {e["source_message_id"] for e in events}, txid)
        stored = [s["features"] for s in scores if s["feature_set_version"]]
        recomputed = archived["recomputed_features"]
        archived["matches_stored_features"] = (
            None if recomputed is None or not stored
            else all({k: f.get(k) for k in FEATURE_NAMES} == recomputed for f in stored)
        )
        result["replay_check"] = archived
    return result
