"""Read-only HTTP API over the Postgres query store. It never writes and holds no feature or model logic."""

import base64
from datetime import datetime
import json
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Path, Query
from psycopg.rows import dict_row

from blockpulse import db

TXID_PATTERN = r"^[0-9a-f]{64}$"
INTERPRETATION = "Unusual transaction structure is not evidence of wrongdoing. A flag is a rule match, not a fraud probability."
NOT_FOUND = (
    "No stored record for this txid. It may never have been observed, may not be loaded yet, "
    "or may have expired from the query store."
)

SCORE_COLUMNS = (
    "id, run_id, txid, feature_set_version, ruleset_version, features, signals, signal_count, "
    "is_flagged, source_event_id, received_at"
)


def encode_cursor(received_at: datetime, score_id: int) -> str:
    raw = json.dumps([received_at.isoformat(), score_id]).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        received_at, score_id = json.loads(base64.urlsafe_b64decode(cursor.encode("ascii")))
        if type(score_id) is not int:
            raise ValueError
        return datetime.fromisoformat(received_at), score_id
    except (ValueError, TypeError, UnicodeError):
        raise HTTPException(status_code=422, detail="cursor is not valid; use the next_cursor value from a previous response")


def create_app(dsn: str | None = None) -> FastAPI:
    app = FastAPI(
        title="BlockPulse", version="0.1.0",
        description="Read-only view of stored transaction scores. " + INTERPRETATION,
    )

    class LazyConnection:
        """Opens the read-only connection on first query, so bad input is rejected before touching the database."""

        def __init__(self):
            self._conn = None

        def execute(self, *args):
            if self._conn is None:
                try:
                    self._conn = db.connect(dsn)
                except Exception as error:
                    raise HTTPException(status_code=503, detail=f"database unavailable: {type(error).__name__}")
                self._conn.row_factory = dict_row
                self._conn.read_only = True
            return self._conn.execute(*args)

        def close(self):
            if self._conn is not None:
                self._conn.close()

    def connection():
        lazy = LazyConnection()
        try:
            yield lazy
        finally:
            lazy.close()

    Conn = Annotated[object, Depends(connection)]

    def lifecycle_for(conn, txid: str) -> list[dict]:
        return conn.execute(
            "SELECT event_id, event_type, event_time, source_message_id, source_sequence, replacement_txid, run_id "
            "FROM lifecycle_events WHERE txid = %s ORDER BY event_time, event_id", (txid,),
        ).fetchall()

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz(conn: Conn):
        conn.execute("SELECT 1")
        return {"status": "ready"}

    @app.get("/tx/{txid}")
    def get_transaction(conn: Conn, txid: Annotated[str, Path(pattern=TXID_PATTERN)]):
        transaction = conn.execute("SELECT * FROM transactions WHERE txid = %s", (txid,)).fetchone()
        if transaction is None:
            raise HTTPException(status_code=404, detail=NOT_FOUND)
        scores = conn.execute(
            f"SELECT {SCORE_COLUMNS} FROM scores WHERE txid = %s ORDER BY received_at, id", (txid,),
        ).fetchall()
        return {
            "transaction": transaction, "scores": scores, "lifecycle_events": lifecycle_for(conn, txid),
            "interpretation": INTERPRETATION,
        }

    @app.get("/alerts")
    def list_alerts(
        conn: Conn,
        since: datetime | None = None,
        detector: Annotated[str | None, Query(max_length=100)] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        cursor: Annotated[str | None, Query(max_length=300)] = None,
    ):
        """Flagged scores, newest first. Pass `next_cursor` back as `cursor` for the next page."""
        clauses, params = ["is_flagged"], []
        if since is not None:
            clauses.append("received_at >= %s")
            params.append(since)
        if detector is not None:
            clauses.append("ruleset_version = %s")
            params.append(detector)
        if cursor is not None:
            cursor_time, cursor_id = decode_cursor(cursor)
            clauses.append("(received_at, id) < (%s, %s)")
            params += [cursor_time, cursor_id]
        rows = conn.execute(
            f"SELECT {SCORE_COLUMNS} FROM scores WHERE {' AND '.join(clauses)} "
            "ORDER BY received_at DESC, id DESC LIMIT %s", (*params, limit + 1),
        ).fetchall()
        page = rows[:limit]
        next_cursor = encode_cursor(page[-1]["received_at"], page[-1]["id"]) if len(rows) > limit else None
        return {"alerts": page, "next_cursor": next_cursor, "interpretation": INTERPRETATION}

    @app.get("/alerts/{score_id}")
    def get_alert(conn: Conn, score_id: Annotated[int, Path(ge=1)]):
        score = conn.execute(f"SELECT {SCORE_COLUMNS} FROM scores WHERE id = %s AND is_flagged", (score_id,)).fetchone()
        if score is None:
            raise HTTPException(status_code=404, detail="No flagged score with this id.")
        transaction = conn.execute("SELECT * FROM transactions WHERE txid = %s", (score["txid"],)).fetchone()
        return {
            "alert": score, "transaction": transaction, "lifecycle_events": lifecycle_for(conn, score["txid"]),
            "interpretation": INTERPRETATION,
        }

    @app.get("/models/current")
    def current_models(conn: Conn):
        """Detector and feature versions that have produced stored scores."""
        rows = conn.execute(
            "SELECT ruleset_version, feature_set_version, count(*) AS scores, "
            "count(*) FILTER (WHERE is_flagged) AS flagged, max(received_at) AS latest_received_at "
            "FROM scores GROUP BY 1, 2 ORDER BY latest_received_at DESC"
        ).fetchall()
        return {"detectors": rows, "note": "Rules are uncalibrated and have no independent labels."}

    @app.get("/pipeline/status")
    def pipeline_status(conn: Conn):
        counts = conn.execute(
            "SELECT (SELECT count(*) FROM transactions) AS transactions, "
            "(SELECT count(*) FROM scores) AS scores, (SELECT count(*) FROM scores WHERE is_flagged) AS flagged, "
            "(SELECT count(*) FROM lifecycle_events) AS lifecycle_events, "
            "(SELECT max(received_at) FROM scores) AS latest_score_received_at, "
            "(SELECT max(event_time) FROM lifecycle_events) AS latest_event_time"
        ).fetchone()
        statuses = conn.execute(
            "SELECT lifecycle_status, count(*) AS count FROM transactions GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        return {
            **counts, "transactions_by_status": statuses,
            "note": "Counts describe loaded data only; they say nothing about live freshness or full mempool coverage.",
        }

    return app


def create_default_app() -> FastAPI:
    """Entry point for `uvicorn --factory blockpulse.api:create_default_app`."""
    return create_app()

