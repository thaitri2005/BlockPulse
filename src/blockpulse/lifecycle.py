"""Store lifecycle evidence and project it onto transactions.lifecycle_status.

The projection is computed from stored evidence, never from load order: the latest event time wins,
and at an identical time a stronger kind wins (mined > replaced > removed > added). Loading old events
after newer ones therefore cannot overwrite newer evidence, and reloading is harmless.
"""

import gzip
import json
from pathlib import Path

from blockpulse.events import reject_nonfinite

STATUS_BY_EVENT = {
    "added": "observed_unconfirmed",
    "removed": "removed_unknown_reason",
    "mined": "confirmed",
    "replaced": "superseded",
}

INSERT_EVENT = """
INSERT INTO lifecycle_events (event_id, run_id, txid, event_type, event_time, source_message_id,
                              source_sequence, replacement_txid)
VALUES (%(event_id)s, %(run_id)s, %(txid)s, %(event_type)s, %(event_time)s, %(source_message_id)s,
        %(source_sequence)s, %(replacement_txid)s)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id
"""

UPSERT_SEEN = """
INSERT INTO transactions (txid, first_seen_at, last_seen_at) VALUES (%(txid)s, %(event_time)s, %(event_time)s)
ON CONFLICT (txid) DO UPDATE SET
    first_seen_at = LEAST(transactions.first_seen_at, EXCLUDED.first_seen_at),
    last_seen_at  = GREATEST(transactions.last_seen_at, EXCLUDED.last_seen_at)
"""

# Rebuild the status of the given transactions from all of their stored evidence.
PROJECT = """
UPDATE transactions t SET lifecycle_status = latest.status
FROM (
    SELECT DISTINCT ON (txid) txid,
        CASE event_type WHEN 'added' THEN 'observed_unconfirmed' WHEN 'removed' THEN 'removed_unknown_reason'
                        WHEN 'mined' THEN 'confirmed' ELSE 'superseded' END AS status
    FROM lifecycle_events
    WHERE txid = ANY(%s)
    ORDER BY txid, event_time DESC,
             CASE event_type WHEN 'mined' THEN 4 WHEN 'replaced' THEN 3 WHEN 'removed' THEN 2 ELSE 1 END DESC,
             event_id
) latest
WHERE t.txid = latest.txid AND t.lifecycle_status IS DISTINCT FROM latest.status
"""

BATCH = 500


def _event_params(event: dict) -> dict:
    for name in ("event_id", "run_id", "txid", "event_time", "source_message_id"):
        if not isinstance(event.get(name), str) or not event[name]:
            raise ValueError(f"{name} is required")
    if event.get("event_type") not in STATUS_BY_EVENT:
        raise ValueError(f"unsupported event_type {event.get('event_type')!r}")
    sequence = event.get("source_sequence")
    if sequence is not None and type(sequence) is not int:
        raise ValueError("source_sequence must be an integer or null")
    return {
        "event_id": event["event_id"], "run_id": event["run_id"], "txid": event["txid"],
        "event_type": event["event_type"], "event_time": event["event_time"],
        "source_message_id": event["source_message_id"], "source_sequence": sequence,
        "replacement_txid": event.get("replacement_txid"),
    }


def load_events(connection, source: Path) -> dict:
    """Load a `process` output's events.jsonl (or its directory) in one transaction.

    A directory whose events file was compacted to events.jsonl.gz is read transparently.
    """
    if source.is_dir():
        plain, packed = source / "events.jsonl", source / "events.jsonl.gz"
        source = plain if plain.is_file() or not packed.is_file() else packed
    if not source.is_file():
        raise ValueError(f"Events file not found: {source}")
    summary = {"source_file": str(source.resolve()), "rows_read": 0, "inserted": 0, "duplicates": 0,
               "skipped_without_txid": 0, "invalid": 0, "invalid_examples": [], "status_changes": 0}
    touched: set[str] = set()

    def project():
        if touched:
            summary["status_changes"] += connection.execute(PROJECT, (sorted(touched),)).rowcount
            touched.clear()

    opener = gzip.open if source.suffix == ".gz" else open
    with connection.transaction(), opener(source, "rb") as lines:
        for line_number, raw in enumerate(lines, start=1):
            summary["rows_read"] += 1
            try:
                event = json.loads(raw, parse_constant=reject_nonfinite)
                if not isinstance(event, dict):
                    raise ValueError("row must be a JSON object")
                if event.get("txid") is None:
                    summary["skipped_without_txid"] += 1
                    continue
                params = _event_params(event)
            except ValueError as error:
                summary["invalid"] += 1
                if len(summary["invalid_examples"]) < 5:
                    summary["invalid_examples"].append({"line": line_number, "detail": str(error)})
                continue
            connection.execute(UPSERT_SEEN, params)
            if connection.execute(INSERT_EVENT, params).fetchone():
                summary["inserted"] += 1
            else:
                summary["duplicates"] += 1
            touched.add(params["txid"])
            if len(touched) >= BATCH:
                project()
        project()
    return summary
