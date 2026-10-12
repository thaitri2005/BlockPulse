"""Capture file -> process -> detect -> Postgres -> API/trace, then repeat to prove replay changes nothing."""

import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from blockpulse import db
from blockpulse.api import create_app
from blockpulse.detect import detect_features
from blockpulse.lifecycle import load_events
from blockpulse.process import process_capture
from blockpulse.results import load_screened
from blockpulse.trace import trace_transaction

PG_DSN = os.environ.get("BLOCKPULSE_TEST_DSN")
FIXTURE = Path(__file__).resolve().parents[1] / "examples" / "fixtures" / "mempool_messages.jsonl"


def table_counts(conn):
    return [conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("transactions", "scores", "lifecycle_events")]


@pytest.mark.skipif(not PG_DSN, reason="Set BLOCKPULSE_TEST_DSN to a throwaway Postgres database")
def test_one_transaction_end_to_end_and_replay_is_stable(tmp_path):
    conn = db.connect(PG_DSN)
    conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    conn.commit()
    db.apply_migrations(conn)

    process_capture(FIXTURE, tmp_path / "processed")
    detect_features(tmp_path / "processed" / "features.jsonl", tmp_path / "screened")
    first_features = [json.loads(line) for line in (tmp_path / "processed" / "features.jsonl").open()]
    assert first_features, "fixture should yield features"

    def load_everything():
        load_screened(conn, tmp_path / "screened", "demo")
        load_events(conn, tmp_path / "processed")
        conn.commit()

    load_everything()
    counts = table_counts(conn)
    assert counts[1] == len(first_features)

    # Replay: re-running every stage on the same capture adds nothing and changes nothing.
    process_capture(FIXTURE, tmp_path / "processed2")
    detect_features(tmp_path / "processed2" / "features.jsonl", tmp_path / "screened2")
    load_screened(conn, tmp_path / "screened2", "demo")
    load_events(conn, tmp_path / "processed2")
    conn.commit()
    assert table_counts(conn) == counts

    # Observation -> archive -> features -> stored score -> API, for one transaction.
    txid = first_features[0]["txid"]
    traced = trace_transaction(conn, txid, FIXTURE)
    assert traced["replay_check"]["matches_stored_features"] is True
    assert traced["scores"][0]["features"]["n_out"] == first_features[0]["n_out"]
    body = TestClient(create_app(PG_DSN)).get(f"/tx/{txid}").json()
    assert body["scores"][0]["source_event_id"] == first_features[0]["event_id"]
    conn.close()
