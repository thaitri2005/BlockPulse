"""Load screened results into Postgres. Safe to repeat: the unique key absorbs retries."""

import json
from pathlib import Path

from blockpulse.events import reject_nonfinite

UPSERT_TRANSACTION = """
INSERT INTO transactions (txid, first_seen_at, last_seen_at) VALUES (%(txid)s, %(seen)s, %(seen)s)
ON CONFLICT (txid) DO UPDATE SET
    first_seen_at = LEAST(transactions.first_seen_at, EXCLUDED.first_seen_at),
    last_seen_at  = GREATEST(transactions.last_seen_at, EXCLUDED.last_seen_at)
"""

INSERT_SCORE = """
INSERT INTO scores (run_id, txid, feature_set_version, ruleset_version, features, signals,
                    signal_count, is_flagged, source_event_id, received_at)
VALUES (%(run_id)s, %(txid)s, %(feature_set_version)s, %(ruleset_version)s, %(features)s, %(signals)s,
        %(signal_count)s, %(is_flagged)s, %(source_event_id)s, %(received_at)s)
ON CONFLICT (run_id, txid, feature_set_version, ruleset_version) DO NOTHING
RETURNING id
"""

SELECT_EXISTING = """
SELECT features, signals, signal_count, is_flagged FROM scores
WHERE run_id = %s AND txid = %s AND feature_set_version = %s AND ruleset_version = %s
"""


def _score_params(run_id: str, row: dict) -> dict:
    """Validate one screened.jsonl row and shape it for the scores table."""
    from psycopg.types.json import Jsonb
    for name in ("txid", "event_id", "received_at", "feature_set_version", "ruleset_version"):
        if not isinstance(row.get(name), str) or not row[name]:
            raise ValueError(f"{name} is required")
    if not isinstance(row.get("features"), dict) or not isinstance(row.get("signals"), list):
        raise ValueError("features must be an object and signals a list")
    if type(row.get("is_flagged")) is not bool or type(row.get("signal_count")) is not int:
        raise ValueError("is_flagged must be a boolean and signal_count an integer")
    return {
        "run_id": run_id, "txid": row["txid"], "feature_set_version": row["feature_set_version"],
        "ruleset_version": row["ruleset_version"], "features": Jsonb(row["features"]),
        "signals": Jsonb(row["signals"]), "signal_count": row["signal_count"],
        "is_flagged": row["is_flagged"], "source_event_id": row["event_id"], "received_at": row["received_at"],
        "seen": row["received_at"],
    }


def load_screened(connection, source: Path, run_id: str) -> dict:
    """Load `screened.jsonl` (or the directory holding it) in one transaction.

    inserted: new rows. duplicates: identical row already stored. conflicts: same key but different
    content, which is reported and never overwritten. invalid: rows that failed validation.
    """
    source = source / "screened.jsonl" if source.is_dir() else source
    if not source.is_file():
        raise ValueError(f"Screened results not found: {source}")
    if not run_id:
        raise ValueError("run_id must not be empty")
    summary = {"run_id": run_id, "source_file": str(source.resolve()), "rows_read": 0,
               "inserted": 0, "duplicates": 0, "conflicts": 0, "invalid": 0, "invalid_examples": []}
    with connection.transaction(), source.open("rb") as lines:
        for line_number, raw in enumerate(lines, start=1):
            summary["rows_read"] += 1
            try:
                row = json.loads(raw, parse_constant=reject_nonfinite)
                if not isinstance(row, dict):
                    raise ValueError("row must be a JSON object")
                params = _score_params(run_id, row)
            except ValueError as error:
                summary["invalid"] += 1
                if len(summary["invalid_examples"]) < 5:
                    summary["invalid_examples"].append({"line": line_number, "detail": str(error)})
                continue
            connection.execute(UPSERT_TRANSACTION, params)
            if connection.execute(INSERT_SCORE, params).fetchone():
                summary["inserted"] += 1
                continue
            key = (run_id, params["txid"], params["feature_set_version"], params["ruleset_version"])
            stored = connection.execute(SELECT_EXISTING, key).fetchone()
            same = stored == (row["features"], row["signals"], row["signal_count"], row["is_flagged"])
            summary["duplicates" if same else "conflicts"] += 1
    return summary
