import json
import os

import pytest

from blockpulse import db
from blockpulse.lifecycle import load_events

PG_DSN = os.environ.get("BLOCKPULSE_TEST_DSN")
needs_postgres = pytest.mark.skipif(not PG_DSN, reason="Set BLOCKPULSE_TEST_DSN to a throwaway Postgres database")
TX = "a" * 64


@pytest.fixture
def conn():
    connection = db.connect(PG_DSN)
    connection.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    connection.commit()
    db.apply_migrations(connection)
    yield connection
    connection.close()


def event(kind, second, txid=TX, **extra):
    return {
        "event_id": f"{kind}-{txid[:4]}-{second}", "run_id": "r", "txid": txid, "event_type": kind,
        "event_time": f"2026-10-09T02:40:{second:02d}.000+00:00", "source_message_id": f"r:{second}",
        "source_sequence": second, **extra,
    }


def write(path, events, extra=""):
    path.write_text("".join(json.dumps(e) + "\n" for e in events) + extra, encoding="utf-8")


def status(conn, txid=TX):
    return conn.execute("SELECT lifecycle_status FROM transactions WHERE txid = %s", (txid,)).fetchone()[0]


@needs_postgres
def test_latest_evidence_wins_regardless_of_load_order(conn, tmp_path):
    path = tmp_path / "events.jsonl"
    # Newer evidence arrives first; the older 'added' must not roll the status back.
    write(path, [event("mined", 30)])
    load_events(conn, path)
    assert status(conn) == "confirmed"
    write(path, [event("added", 10)])
    summary = load_events(conn, path)
    assert summary["inserted"] == 1
    assert status(conn) == "confirmed"
    row = conn.execute("SELECT first_seen_at FROM transactions WHERE txid = %s", (TX,)).fetchone()
    assert row[0].second == 10  # first_seen moves earlier; status does not move backwards


@needs_postgres
def test_same_time_precedence_and_replacement_link(conn, tmp_path):
    path = tmp_path / "events.jsonl"
    write(path, [event("removed", 5), event("replaced", 5, replacement_txid="b" * 64), event("added", 1)])
    load_events(conn, path)
    assert status(conn) == "superseded"
    link = conn.execute("SELECT replacement_txid FROM lifecycle_events WHERE event_type = 'replaced'").fetchone()
    assert link[0] == "b" * 64


@needs_postgres
def test_reload_is_a_no_op_and_bad_rows_are_reported(conn, tmp_path):
    path = tmp_path / "events.jsonl"
    write(path, [event("added", 1), event("removed", 2), {**event("added", 3), "txid": None}, {"event_type": "added"}], "nope\n")
    first = load_events(conn, path)
    assert (first["inserted"], first["skipped_without_txid"], first["invalid"]) == (2, 2, 1)
    again = load_events(conn, path)
    assert (again["inserted"], again["duplicates"], again["status_changes"]) == (0, 2, 0)
    assert status(conn) == "removed_unknown_reason"
    assert conn.execute("SELECT count(*) FROM lifecycle_events").fetchone()[0] == 2


@needs_postgres
def test_backfill_with_deferred_projection_matches_incremental(conn, tmp_path):
    from blockpulse.lifecycle import project_all
    path = tmp_path / "events.jsonl"
    write(path, [event("added", 1), event("mined", 2)])
    deferred = load_events(conn, path, project=False)
    assert deferred["inserted"] == 2 and deferred["status_changes"] == 0
    assert status(conn) == "observed_unconfirmed"  # not projected yet
    assert project_all(conn) == 1
    assert status(conn) == "confirmed"  # same answer the incremental load gives
    assert project_all(conn) == 0  # nothing left to change
