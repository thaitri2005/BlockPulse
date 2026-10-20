"""Rebuild the Postgres serving tables from the lake in bulk, then swap them in atomically.

Inserting millions of rows one session at a time into indexed tables is slow once the indexes outgrow memory:
lifecycle_events is keyed by a random hash, so every insert lands on a random index page and waits for a disk
read (measured in docs/guides/04-postgres-api-dashboard.md). A rebuild avoids that entirely:

1. Read the lake with DuckDB (columnar, fast) and compute each transaction's lifecycle status there.
2. COPY everything into new tables that have no indexes yet.
3. Build the indexes once, as sorted bulk builds.
4. In one short transaction, drop the old tables and rename the new ones into place. Readers see the old data
   or the new data, never a mix.

Sessions without lake tables (older runs) are read from their screened and processed files instead. Signal
explanations aren't stored in the lake; they are regenerated from the features with the same rules, and the
rebuild refuses to continue if the regenerated rule IDs differ from the lake's for any row.
"""

import gzip
import json
import time
from pathlib import Path

from blockpulse import db
from blockpulse.detector import RULESET_VERSION, DetectorConfig, screen_transaction
from blockpulse.features import FEATURE_NAMES

TABLES = ("scores", "transactions", "lifecycle_events")
SCORE_COLUMNS = ("run_id", "txid", "feature_set_version", "ruleset_version", "features", "signals",
                 "signal_count", "is_flagged", "source_event_id", "received_at")
EVENT_COLUMNS = ("event_id", "run_id", "txid", "event_type", "event_time", "source_message_id",
                 "source_sequence", "replacement_txid")
STATUS_SQL = """CASE event_type WHEN 'added' THEN 'observed_unconfirmed' WHEN 'removed' THEN 'removed_unknown_reason'
                 WHEN 'mined' THEN 'confirmed' ELSE 'superseded' END"""
EVENT_RANK_SQL = "CASE event_type WHEN 'mined' THEN 4 WHEN 'replaced' THEN 3 WHEN 'removed' THEN 2 ELSE 1 END"

# Indexes and constraints of the live tables (migrations 001-002), rebuilt on the new tables after loading.
INDEXES = [
    "ALTER TABLE scores_rebuild ADD CONSTRAINT scores_rebuild_pkey PRIMARY KEY (id)",
    "ALTER TABLE scores_rebuild ADD CONSTRAINT scores_rebuild_key UNIQUE (run_id, txid, feature_set_version, ruleset_version)",
    "CREATE INDEX scores_rebuild_txid_idx ON scores_rebuild (txid)",
    "CREATE INDEX scores_rebuild_received_at_idx ON scores_rebuild (received_at DESC)",
    "CREATE INDEX scores_rebuild_flagged_received_idx ON scores_rebuild (received_at DESC) WHERE is_flagged",
    "ALTER TABLE transactions_rebuild ADD CONSTRAINT transactions_rebuild_pkey PRIMARY KEY (txid)",
    "ALTER TABLE lifecycle_events_rebuild ADD CONSTRAINT lifecycle_events_rebuild_pkey PRIMARY KEY (event_id)",
    "CREATE INDEX lifecycle_events_rebuild_txid_idx ON lifecycle_events_rebuild (txid, event_time)",
]
RENAMES = [
    "ALTER TABLE scores RENAME CONSTRAINT scores_rebuild_pkey TO scores_pkey",
    "ALTER TABLE scores RENAME CONSTRAINT scores_rebuild_key TO scores_run_id_txid_feature_set_version_ruleset_version_key",
    "ALTER INDEX scores_rebuild_txid_idx RENAME TO scores_txid_idx",
    "ALTER INDEX scores_rebuild_received_at_idx RENAME TO scores_received_at_idx",
    "ALTER INDEX scores_rebuild_flagged_received_idx RENAME TO scores_flagged_received_idx",
    "ALTER TABLE transactions RENAME CONSTRAINT transactions_rebuild_pkey TO transactions_pkey",
    "ALTER TABLE lifecycle_events RENAME CONSTRAINT lifecycle_events_rebuild_pkey TO lifecycle_events_pkey",
    "ALTER INDEX lifecycle_events_rebuild_txid_idx RENAME TO lifecycle_events_txid_idx",
]


def _jsonl(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as lines:
        for line in lines:
            if line.strip():
                yield json.loads(line)


def _signals(features: dict, expected_rule_ids: list[str], where: str) -> tuple[str, int, bool]:
    result = screen_transaction(features, DetectorConfig())
    rule_ids = [s["rule_id"] for s in result["signals"]]
    if rule_ids != list(expected_rule_ids):
        raise ValueError(f"{where}: regenerated rule IDs {rule_ids} differ from the stored {list(expected_rule_ids)}")
    return json.dumps(result["signals"]), result["signal_count"], result["is_flagged"]


def _fallback_sessions(root: Path, lake_sessions: set[str]) -> list[Path]:
    """Screened folders that have no lake tables (runs from before the lake existed)."""
    return sorted(p for p in (root / "screened").iterdir()
                  if p.is_dir() and (p / "summary.json").is_file() and p.name not in lake_sessions)


def _fallback_events_file(screened: Path) -> Path | None:
    try:
        source = json.loads((screened / "summary.json").read_text(encoding="utf-8"))["source_file"]
        processed = Path(source).parent
    except (KeyError, ValueError, OSError):
        return None
    for name in ("events.jsonl", "events.jsonl.gz"):
        if (processed / name).is_file():
            return processed / name
    return None


def rebuild_serving(root: Path, dsn: str | None = None) -> dict:
    """Rebuild scores, transactions and lifecycle_events from the lake (plus older sessions' files) and swap them in."""
    from blockpulse.analytics import connect as lake_connect
    started = time.monotonic()
    timings = {}
    lake = lake_connect(root)
    lake_sessions = {row[0] for row in lake.execute("SELECT DISTINCT session FROM transactions").fetchall()}
    fallback = _fallback_sessions(root, lake_sessions)
    counts = {"scores": 0, "lifecycle_events": 0, "fallback_sessions": [p.name for p in fallback]}

    with db.connect(dsn) as pg:
        with pg.transaction():
            pg.execute("SET LOCAL maintenance_work_mem = '512MB'")
            pg.execute("SET LOCAL work_mem = '256MB'")
            for table in TABLES:
                pg.execute(f"DROP TABLE IF EXISTS {table}_rebuild")
                pg.execute(f"CREATE TABLE {table}_rebuild (LIKE {table} INCLUDING DEFAULTS INCLUDING CONSTRAINTS)")

            # 1. Scores: lake rows, then older sessions from their screened files.
            step = time.monotonic()
            names = ", ".join(SCORE_COLUMNS)
            with pg.cursor().copy(f"COPY scores_rebuild ({names}) FROM STDIN") as copy:
                cursor = lake.execute(
                    f"SELECT session, txid, feature_set_version, ruleset_version, {', '.join(FEATURE_NAMES)}, "
                    "rule_ids, event_id, received_at FROM transactions")
                while batch := cursor.fetchmany(50_000):
                    for row in batch:
                        features = dict(zip(FEATURE_NAMES, row[4:13]))
                        if row[3] != RULESET_VERSION:
                            raise ValueError(f"{row[0]}: ruleset {row[3]} is not {RULESET_VERSION}")
                        signals, count, flagged = _signals(features, row[13], row[0])
                        copy.write_row((row[0], row[1], row[2], row[3], json.dumps(features), signals, count, flagged,
                                        row[14], row[15]))
                        counts["scores"] += 1
                for folder in fallback:
                    for r in _jsonl(folder / "screened.jsonl"):
                        copy.write_row((folder.name, r["txid"], r["feature_set_version"], r["ruleset_version"],
                                        json.dumps(r["features"]), json.dumps(r["signals"]), r["signal_count"],
                                        r["is_flagged"], r["event_id"], r["received_at"]))
                        counts["scores"] += 1
            timings["copy_scores"] = time.monotonic() - step

            # 2. Lifecycle events: lake rows, then older sessions' processed events.
            step = time.monotonic()
            with pg.cursor().copy(f"COPY lifecycle_events_rebuild ({', '.join(EVENT_COLUMNS)}) FROM STDIN") as copy:
                cursor = lake.execute(f"SELECT {', '.join(EVENT_COLUMNS)} FROM lifecycle_events")
                while batch := cursor.fetchmany(100_000):
                    for row in batch:
                        copy.write_row(row)
                    counts["lifecycle_events"] += len(batch)
                seen = set()
                for folder in fallback:
                    events = _fallback_events_file(folder)
                    for e in _jsonl(events) if events else ():
                        if e.get("txid") is None or e["event_id"] in seen:
                            continue
                        seen.add(e["event_id"])
                        copy.write_row(tuple(e.get(c) for c in EVENT_COLUMNS))
                        counts["lifecycle_events"] += 1
            timings["copy_events"] = time.monotonic() - step

            # 3. Transactions: first/last seen and lifecycle status, computed in Postgres from the new tables.
            step = time.monotonic()
            pg.execute("ANALYZE scores_rebuild")
            pg.execute("ANALYZE lifecycle_events_rebuild")
            pg.execute(f"""
                INSERT INTO transactions_rebuild (txid, first_seen_at, last_seen_at, lifecycle_status)
                SELECT seen.txid, seen.first_seen, seen.last_seen, coalesce(latest.status, 'observed_unconfirmed')
                FROM (SELECT txid, min(t) AS first_seen, max(t) AS last_seen FROM (
                        SELECT txid, event_time AS t FROM lifecycle_events_rebuild
                        UNION ALL SELECT txid, received_at FROM scores_rebuild) AS observed GROUP BY txid) AS seen
                LEFT JOIN (SELECT DISTINCT ON (txid) txid, {STATUS_SQL} AS status FROM lifecycle_events_rebuild
                           ORDER BY txid, event_time DESC, {EVENT_RANK_SQL} DESC, event_id) AS latest USING (txid)
            """)
            timings["build_transactions"] = time.monotonic() - step

            # 4. Indexes and constraints, built once over the loaded data.
            step = time.monotonic()
            for statement in INDEXES:
                pg.execute(statement)
            timings["build_indexes"] = time.monotonic() - step

            # 5. Swap. Nothing above touched the live tables, so until here a failure leaves them unchanged.
            step = time.monotonic()
            pg.execute("ALTER SEQUENCE scores_id_seq OWNED BY scores_rebuild.id")
            for table in TABLES:
                pg.execute(f"DROP TABLE {table}")
                pg.execute(f"ALTER TABLE {table}_rebuild RENAME TO {table}")
            for statement in RENAMES:
                pg.execute(statement)
            pg.execute("SELECT setval('scores_id_seq', coalesce((SELECT max(id) FROM scores), 1))")
            timings["swap"] = time.monotonic() - step
        for table in TABLES:  # statistics for the planner on the live tables
            pg.execute(f"ANALYZE {table}")
        counts["transactions"] = pg.execute("SELECT count(*) FROM transactions").fetchone()[0]
        counts["flagged"] = pg.execute("SELECT count(*) FROM scores WHERE is_flagged").fetchone()[0]
    lake.close()
    return {**counts, "seconds": {k: round(v, 1) for k, v in timings.items()} | {"total": round(time.monotonic() - started, 1)}}
