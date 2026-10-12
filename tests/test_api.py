import os
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from blockpulse import db
from blockpulse.api import create_app, decode_cursor, encode_cursor
from blockpulse.lifecycle import load_events
from blockpulse.results import load_screened

from tests.test_lifecycle import event, write as write_events
from tests.test_results import screened_row, write_rows

PG_DSN = os.environ.get("BLOCKPULSE_TEST_DSN")
needs_postgres = pytest.mark.skipif(not PG_DSN, reason="Set BLOCKPULSE_TEST_DSN to a throwaway Postgres database")


def txid(n):
    return f"{n:064x}"


def test_cursor_round_trip_and_rejects_garbage():
    moment = datetime(2026, 10, 9, 2, 40, tzinfo=timezone.utc)
    assert decode_cursor(encode_cursor(moment, 7)) == (moment, 7)
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as caught:
        decode_cursor("not-a-cursor")
    assert caught.value.status_code == 422


def test_healthz_needs_no_database_and_bad_txid_is_rejected():
    client = TestClient(create_app("postgresql://nobody@127.0.0.1:1/none"))
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/tx/not-a-txid").status_code == 422
    assert client.get("/readyz").status_code == 503
    assert client.get("/alerts?limit=0").status_code == 422


@pytest.fixture
def client(tmp_path):
    conn = db.connect(PG_DSN)
    conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    conn.commit()
    db.apply_migrations(conn)
    rows = []
    for n in range(1, 6):  # five flagged rows at distinct, increasing times
        row = screened_row(txid(n), flagged=True)
        row["received_at"] = f"2026-10-09T02:40:{n:02d}.000+00:00"
        rows.append(row)
    rows.append(screened_row(txid(9)))  # one unflagged row
    write_rows(tmp_path / "screened.jsonl", rows)
    load_screened(conn, tmp_path / "screened.jsonl", "run-1")
    write_events(tmp_path / "events.jsonl", [event("added", 1, txid(1)), event("mined", 20, txid(1))])
    load_events(conn, tmp_path / "events.jsonl")
    conn.commit()
    conn.close()
    yield TestClient(create_app(PG_DSN))


@needs_postgres
def test_transaction_view_has_scores_and_lifecycle(client):
    body = client.get(f"/tx/{txid(1)}").json()
    assert body["transaction"]["lifecycle_status"] == "confirmed"
    assert body["scores"][0]["is_flagged"] is True
    assert [e["event_type"] for e in body["lifecycle_events"]] == ["added", "mined"]
    missing = client.get(f"/tx/{txid(77)}")
    assert missing.status_code == 404 and "never have been observed" in missing.json()["detail"]


@needs_postgres
def test_alerts_paginate_newest_first_without_gaps(client):
    first = client.get("/alerts?limit=2").json()
    assert [a["txid"] for a in first["alerts"]] == [txid(5), txid(4)]
    second = client.get("/alerts", params={"limit": 2, "cursor": first["next_cursor"]}).json()
    assert [a["txid"] for a in second["alerts"]] == [txid(3), txid(2)]
    third = client.get("/alerts", params={"limit": 2, "cursor": second["next_cursor"]}).json()
    assert [a["txid"] for a in third["alerts"]] == [txid(1)] and third["next_cursor"] is None
    since = client.get("/alerts", params={"since": "2026-10-09T02:40:04+00:00"}).json()
    assert len(since["alerts"]) == 2
    assert client.get("/alerts?detector=other").json()["alerts"] == []


@needs_postgres
def test_alert_detail_models_and_status(client):
    alert_id = client.get("/alerts?limit=1").json()["alerts"][0]["id"]
    detail = client.get(f"/alerts/{alert_id}").json()
    assert detail["alert"]["signals"][0]["rule_id"] == "many_outputs"
    assert "not evidence of wrongdoing" in detail["interpretation"]
    unflagged = client.get("/tx/" + txid(9)).json()["scores"][0]["id"]
    assert client.get(f"/alerts/{unflagged}").status_code == 404
    models = client.get("/models/current").json()["detectors"]
    assert models[0]["ruleset_version"] == "structural-rules-v1" and models[0]["flagged"] == 5
    status = client.get("/pipeline/status").json()
    assert (status["scores"], status["flagged"], status["lifecycle_events"]) == (6, 5, 2)
    assert client.get("/readyz").json() == {"status": "ready"}


@needs_postgres
def test_api_connection_is_read_only(client):
    conn = db.connect(PG_DSN)
    conn.read_only = True
    import psycopg
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        conn.execute("DELETE FROM scores")
    conn.close()
