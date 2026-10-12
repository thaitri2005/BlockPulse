import json
import os

import pytest

from blockpulse import db
from blockpulse.results import load_screened

PG_DSN = os.environ.get("BLOCKPULSE_TEST_DSN")
needs_postgres = pytest.mark.skipif(not PG_DSN, reason="Set BLOCKPULSE_TEST_DSN to a throwaway Postgres database")


@pytest.fixture
def conn():
    connection = db.connect(PG_DSN)
    connection.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    connection.commit()
    db.apply_migrations(connection)
    yield connection
    connection.close()


def screened_row(txid, flagged=False, n_out=2):
    return {
        "schema_version": 1, "txid": txid, "event_id": "e-" + txid, "received_at": "2026-10-09T02:40:00.000+00:00",
        "source_mode": "ws", "feature_set_version": "transaction-v2",
        "features": {"n_in": 1, "n_out": n_out}, "ruleset_version": "structural-rules-v1",
        "signal_count": int(flagged), "is_flagged": flagged,
        "signals": [{"rule_id": "many_outputs", "explanation": "x"}] if flagged else [],
    }


def write_rows(path, rows, extra=""):
    path.write_text("".join(json.dumps(r) + "\n" for r in rows) + extra, encoding="utf-8")


@needs_postgres
def test_load_is_repeatable_and_reports_conflicts(conn, tmp_path):
    source = tmp_path / "screened.jsonl"
    write_rows(source, [screened_row("a" * 64), screened_row("b" * 64, flagged=True)], extra="not json\n")
    first = load_screened(conn, source, "run-1")
    assert (first["inserted"], first["duplicates"], first["invalid"]) == (2, 0, 1)

    second = load_screened(conn, source, "run-1")
    assert (second["inserted"], second["duplicates"]) == (0, 2)
    assert conn.execute("SELECT count(*) FROM scores").fetchone()[0] == 2
    assert conn.execute("SELECT count(*) FROM transactions").fetchone()[0] == 2

    # Same key, different content: reported, never overwritten.
    write_rows(source, [screened_row("a" * 64, n_out=99)])
    assert load_screened(conn, source, "run-1")["conflicts"] == 1
    stored = conn.execute("SELECT features FROM scores WHERE txid = %s", ("a" * 64,)).fetchone()[0]
    assert stored["n_out"] == 2

    # A different run id is a separate, legitimate result set.
    assert load_screened(conn, source, "run-2")["inserted"] == 1


@needs_postgres
def test_directory_source_and_missing_file(conn, tmp_path):
    write_rows(tmp_path / "screened.jsonl", [screened_row("c" * 64)])
    assert load_screened(conn, tmp_path, "run-1")["inserted"] == 1
    with pytest.raises(ValueError, match="not found"):
        load_screened(conn, tmp_path / "missing.jsonl", "run-1")
