"""Load screened results into Postgres. Safe to repeat: the unique key absorbs retries.

Rows are validated in Python, streamed into a temporary staging table with COPY, then merged with a few
set-based statements in one transaction. Row-by-row inserts cost several round trips per row; this costs a
handful per file (measured in docs/guides/04-postgres-api-dashboard.md).
"""

import json
from pathlib import Path

from blockpulse.events import reject_nonfinite

KEY = ("run_id", "txid", "feature_set_version", "ruleset_version")
STAGE_COLUMNS = ("run_id", "txid", "feature_set_version", "ruleset_version", "features", "signals",
                 "signal_count", "is_flagged", "source_event_id", "received_at")

CREATE_STAGE = """
CREATE TEMP TABLE stage_scores (
    run_id text, txid text, feature_set_version text, ruleset_version text, features jsonb, signals jsonb,
    signal_count integer, is_flagged boolean, source_event_id text, received_at timestamptz
) ON COMMIT DROP
"""

UPSERT_TRANSACTIONS = """
INSERT INTO transactions (txid, first_seen_at, last_seen_at)
SELECT txid, min(received_at), max(received_at) FROM stage_scores GROUP BY txid
ON CONFLICT (txid) DO UPDATE SET
    first_seen_at = LEAST(transactions.first_seen_at, EXCLUDED.first_seen_at),
    last_seen_at  = GREATEST(transactions.last_seen_at, EXCLUDED.last_seen_at)
"""

# Rows whose key is already stored: identical content is a duplicate, different content is a conflict.
CLASSIFY_EXISTING = """
SELECT count(*) FILTER (WHERE s.features = x.features AND s.signals = x.signals
                          AND s.signal_count = x.signal_count AND s.is_flagged = x.is_flagged),
       count(*) FILTER (WHERE NOT (s.features = x.features AND s.signals = x.signals
                          AND s.signal_count = x.signal_count AND s.is_flagged = x.is_flagged))
FROM stage_scores s
JOIN scores x USING (run_id, txid, feature_set_version, ruleset_version)
"""

INSERT_NEW = """
INSERT INTO scores (run_id, txid, feature_set_version, ruleset_version, features, signals,
                    signal_count, is_flagged, source_event_id, received_at)
SELECT DISTINCT ON (run_id, txid, feature_set_version, ruleset_version) *
FROM stage_scores
ON CONFLICT (run_id, txid, feature_set_version, ruleset_version) DO NOTHING
"""


def _score_row(run_id: str, row: dict) -> tuple:
    """Validate one screened.jsonl row and shape it as a staging-table row."""
    for name in ("txid", "event_id", "received_at", "feature_set_version", "ruleset_version"):
        if not isinstance(row.get(name), str) or not row[name]:
            raise ValueError(f"{name} is required")
    if not isinstance(row.get("features"), dict) or not isinstance(row.get("signals"), list):
        raise ValueError("features must be an object and signals a list")
    if type(row.get("is_flagged")) is not bool or type(row.get("signal_count")) is not int:
        raise ValueError("is_flagged must be a boolean and signal_count an integer")
    return (run_id, row["txid"], row["feature_set_version"], row["ruleset_version"],
            json.dumps(row["features"]), json.dumps(row["signals"]), row["signal_count"], row["is_flagged"],
            row["event_id"], row["received_at"])


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
    valid = []
    with source.open("rb") as lines:
        for line_number, raw in enumerate(lines, start=1):
            summary["rows_read"] += 1
            try:
                row = json.loads(raw, parse_constant=reject_nonfinite)
                if not isinstance(row, dict):
                    raise ValueError("row must be a JSON object")
                valid.append(_score_row(run_id, row))
            except ValueError as error:
                summary["invalid"] += 1
                if len(summary["invalid_examples"]) < 5:
                    summary["invalid_examples"].append({"line": line_number, "detail": str(error)})
    with connection.transaction():
        # Inside a caller's transaction this block is a savepoint and ON COMMIT DROP hasn't fired yet.
        connection.execute("DROP TABLE IF EXISTS pg_temp.stage_scores")
        connection.execute(CREATE_STAGE)
        with connection.cursor().copy(f"COPY stage_scores ({', '.join(STAGE_COLUMNS)}) FROM STDIN") as copy:
            for record in valid:
                copy.write_row(record)
        connection.execute("SET LOCAL work_mem = '128MB'")  # bulk sorts and hashes; the 4 MB default spills
        connection.execute("ANALYZE stage_scores")  # without statistics the planner assumes 200 rows
        connection.execute(UPSERT_TRANSACTIONS)
        duplicates, conflicts = connection.execute(CLASSIFY_EXISTING).fetchone()
        inserted = connection.execute(INSERT_NEW).rowcount
    repeated_in_file = len(valid) - inserted - duplicates - conflicts  # same key twice in one file
    summary.update({"inserted": inserted, "duplicates": duplicates + repeated_in_file, "conflicts": conflicts})
    return summary
