import os

import pytest

pytest.importorskip("duckdb")

from blockpulse import collector, db  # noqa: E402
from blockpulse.serving import rebuild_serving  # noqa: E402
from tests.test_collector import captured_layout  # noqa: E402

PG_DSN = os.environ.get("BLOCKPULSE_TEST_DSN")
needs_postgres = pytest.mark.skipif(not PG_DSN, reason="Set BLOCKPULSE_TEST_DSN to a throwaway Postgres database")

SNAPSHOT = {
    "scores": "SELECT run_id, txid, feature_set_version, ruleset_version, features, signals, signal_count, is_flagged, "
              "source_event_id, received_at FROM scores ORDER BY run_id, txid",
    "lifecycle_events": "SELECT event_id, run_id, txid, event_type, event_time, source_message_id, source_sequence, "
                        "replacement_txid FROM lifecycle_events ORDER BY event_id",
    "transactions": "SELECT txid, first_seen_at, last_seen_at, lifecycle_status FROM transactions ORDER BY txid",
}


def fresh(connection):
    connection.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    connection.commit()
    db.apply_migrations(connection)
    connection.commit()


def snapshot(connection):
    return {name: connection.execute(sql).fetchall() for name, sql in SNAPSHOT.items()}


@needs_postgres
def test_rebuild_matches_incremental_loading_and_swaps_cleanly(tmp_path, transaction):
    layout = captured_layout(tmp_path, transaction, sessions=3)
    assert collector.process_pending(layout)["failed"] == []  # builds the lake
    with db.connect(PG_DSN) as connection:
        fresh(connection)
    for name in collector.finished_captures(layout):  # the incremental path, one session at a time
        collector.process_session(layout, name, load_db=True, dsn=PG_DSN)
    with db.connect(PG_DSN) as connection:
        incremental = snapshot(connection)
        fresh(connection)
    result = rebuild_serving(layout.root, PG_DSN)
    with db.connect(PG_DSN) as connection:
        rebuilt = snapshot(connection)
        indexes = {r[0] for r in connection.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")}
        leftovers = connection.execute("SELECT count(*) FROM pg_tables WHERE tablename LIKE '%_rebuild'").fetchone()[0]
        next_id = connection.execute("SELECT nextval('scores_id_seq')").fetchone()[0]
    assert rebuilt == incremental and incremental["scores"]
    assert result["scores"] == len(incremental["scores"])
    assert {"scores_pkey", "scores_txid_idx", "transactions_pkey", "lifecycle_events_pkey",
            "lifecycle_events_txid_idx", "scores_run_id_txid_feature_set_version_ruleset_version_key"} <= indexes
    assert leftovers == 0 and next_id == len(rebuilt["scores"]) + 1
    rebuild_serving(layout.root, PG_DSN)  # a second rebuild works and gives the same tables
    with db.connect(PG_DSN) as connection:
        assert snapshot(connection) == incremental
