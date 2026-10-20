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

STAGE_COLUMNS = ("event_id", "run_id", "txid", "event_type", "event_time", "source_message_id",
                 "source_sequence", "replacement_txid")

CREATE_STAGE = """
CREATE TEMP TABLE stage_events (
    event_id text, run_id text, txid text, event_type text, event_time timestamptz,
    source_message_id text, source_sequence bigint, replacement_txid text
) ON COMMIT DROP
"""

UPSERT_SEEN = """
INSERT INTO transactions (txid, first_seen_at, last_seen_at)
SELECT txid, min(event_time), max(event_time) FROM stage_events GROUP BY txid
ON CONFLICT (txid) DO UPDATE SET
    first_seen_at = LEAST(transactions.first_seen_at, EXCLUDED.first_seen_at),
    last_seen_at  = GREATEST(transactions.last_seen_at, EXCLUDED.last_seen_at)
"""

INSERT_EVENTS = """
INSERT INTO lifecycle_events (event_id, run_id, txid, event_type, event_time, source_message_id,
                              source_sequence, replacement_txid)
SELECT event_id, run_id, txid, event_type, event_time, source_message_id, source_sequence, replacement_txid
FROM stage_events
ON CONFLICT (event_id) DO NOTHING
"""

# Rebuild the status of transactions from all of their stored evidence: those this load touched, or (for a
# backfill) every transaction at once with a single scan.
_PROJECT_BODY = """
UPDATE transactions t SET lifecycle_status = latest.status
FROM (
    SELECT DISTINCT ON (e.txid) e.txid,
        CASE e.event_type WHEN 'added' THEN 'observed_unconfirmed' WHEN 'removed' THEN 'removed_unknown_reason'
                          WHEN 'mined' THEN 'confirmed' ELSE 'superseded' END AS status
    FROM lifecycle_events e
    {where}
    ORDER BY e.txid, e.event_time DESC,
             CASE e.event_type WHEN 'mined' THEN 4 WHEN 'replaced' THEN 3 WHEN 'removed' THEN 2 ELSE 1 END DESC,
             e.event_id
) latest
WHERE t.txid = latest.txid AND t.lifecycle_status IS DISTINCT FROM latest.status
"""
PROJECT = _PROJECT_BODY.format(where="WHERE e.txid IN (SELECT DISTINCT txid FROM stage_events)")
PROJECT_ALL = _PROJECT_BODY.format(where="")

# Bulk statements sort and hash hundreds of thousands of rows; the 4 MB default spills to disk.
LOAD_WORK_MEM = "SET LOCAL work_mem = '128MB'"


def project_all(connection) -> int:
    """Recompute every transaction's status in one pass (after a backfill loaded with project=False)."""
    with connection.transaction():
        connection.execute(LOAD_WORK_MEM)
        return connection.execute(PROJECT_ALL).rowcount


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


def load_events(connection, source: Path, *, project: bool = True) -> dict:
    """Load a `process` output's events.jsonl (or its directory) in one transaction.

    A directory whose events file was compacted to events.jsonl.gz is read transparently. Events are validated
    in Python, streamed into a staging table with COPY, and merged with set-based statements.
    project=False skips the status projection; a backfill then calls project_all() once at the end, because a
    per-session projection rescans the growing events table every time.
    """
    if source.is_dir():
        plain, packed = source / "events.jsonl", source / "events.jsonl.gz"
        source = plain if plain.is_file() or not packed.is_file() else packed
    if not source.is_file():
        raise ValueError(f"Events file not found: {source}")
    summary = {"source_file": str(source.resolve()), "rows_read": 0, "inserted": 0, "duplicates": 0,
               "skipped_without_txid": 0, "invalid": 0, "invalid_examples": [], "status_changes": 0}
    valid = []
    opener = gzip.open if source.suffix == ".gz" else open
    with opener(source, "rb") as lines:
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
            valid.append(tuple(params[name] for name in STAGE_COLUMNS))
    with connection.transaction():
        # Inside a caller's transaction this block is a savepoint and ON COMMIT DROP hasn't fired yet.
        connection.execute("DROP TABLE IF EXISTS pg_temp.stage_events")
        connection.execute(CREATE_STAGE)
        with connection.cursor().copy(f"COPY stage_events ({', '.join(STAGE_COLUMNS)}) FROM STDIN") as copy:
            for record in valid:
                copy.write_row(record)
        connection.execute(LOAD_WORK_MEM)
        connection.execute("ANALYZE stage_events")  # without statistics the planner assumes 200 rows
        connection.execute(UPSERT_SEEN)
        inserted = connection.execute(INSERT_EVENTS).rowcount
        if project:
            summary["status_changes"] = connection.execute(PROJECT).rowcount
    summary.update({"inserted": inserted, "duplicates": len(valid) - inserted})
    return summary
