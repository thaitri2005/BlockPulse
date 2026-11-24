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


@needs_postgres
def test_serving_can_be_rebuilt_from_the_streamed_lake(tmp_path, transaction):
    """The stream sink's lake serves the same transactions and scores as the batch lake; only the run id differs."""
    from blockpulse.stream_lake import stream_to_lake
    from tests.test_stream_lake import FakeConsumer, enriched_messages
    layout = captured_layout(tmp_path, transaction, sessions=3)
    assert collector.process_pending(layout)["failed"] == []
    FakeConsumer.committed = {}
    messages = enriched_messages(layout)
    stream_lake = tmp_path / "lake-stream"
    stream_to_lake(stream_lake, "g", "unused", consumer_factory=lambda _c: FakeConsumer(messages), idle_timeout=0.1, batch_messages=2)

    with db.connect(PG_DSN) as connection:
        fresh(connection)
    rebuild_serving(layout.root, PG_DSN)
    with db.connect(PG_DSN) as connection:
        from_batch = snapshot(connection)
        fresh(connection)
    result = rebuild_serving(layout.root, PG_DSN, lake_dir=stream_lake)
    with db.connect(PG_DSN) as connection:
        from_stream = snapshot(connection)

    assert result["source_lake"] == str(stream_lake) and result["fallback_sessions"] == []
    assert from_stream["transactions"] == from_batch["transactions"] and from_batch["transactions"]
    without_run = lambda rows: sorted(row[1:] for row in rows)  # noqa: E731  (column 0 is run_id in both queries)
    assert without_run(from_stream["scores"]) == without_run(from_batch["scores"])
    assert [(r[0], *r[2:]) for r in from_stream["lifecycle_events"]] == [(r[0], *r[2:]) for r in from_batch["lifecycle_events"]]
