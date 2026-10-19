# 04 · Postgres, API, and dashboard

Milestone M2 turns the offline results into something you can query. Data flows into Postgres from the files the earlier stages already write; an API and a Grafana dashboard read it.

```text
capture messages.jsonl
   |-- process --> events.jsonl ---- load-events --> lifecycle_events --+
   |            \-> features.jsonl -- detect --> screened.jsonl         |--> transactions (status projection)
   |                                     load-results --> scores -------+
   '-- trace TXID --capture ... (re-derives features from the raw line and compares)

Postgres --> read-only API (blockpulse serve)      Postgres --> Grafana dashboard
```

The files remain the source of truth. Postgres is a rebuildable query copy: dropping the database and re-running the load commands restores it.

## Run it

```powershell
pip install -e ".[m2,dev]"
docker compose --profile m2 up -d --wait          # Postgres + Grafana (Kafka is the separate m1 profile)
.\.venv\Scripts\python.exe -m blockpulse db-init  # create tables; safe to repeat

.\.venv\Scripts\python.exe -m blockpulse load-results data/screened/phase0-10min-20261009
.\.venv\Scripts\python.exe -m blockpulse load-events  data/processed/phase0-10min-20261009-fixed
.\.venv\Scripts\python.exe -m blockpulse serve        # http://127.0.0.1:8000/docs
```

Grafana is at http://127.0.0.1:3000 (user `admin`, password `blockpulse_dev`), dashboard "BlockPulse overview". The time range defaults to the last 30 days because loaded captures keep their original receive times; widen it if the panels are empty. Memory limits: Postgres 512 MB, Grafana 384 MB. Everything binds to localhost. The passwords are local-development defaults; override with `POSTGRES_PASSWORD` and `GRAFANA_PASSWORD`, and `BLOCKPULSE_DSN` for the app.

Offline demo with synthetic data and no live source: `powershell -File scripts/demo_m2.ps1`. It uses its own database, `blockpulse_demo`, so fake rows never mix with real ones.

Stop services when you're done: `docker compose --profile m2 stop`. Data lives in the `postgres-data` volume and survives.

## Tables

| Table | One row per | Notes |
| --- | --- | --- |
| `transactions` | txid | First/last seen time and `lifecycle_status`, a projection (see below) |
| `scores` | run, txid, feature version, rule version | Features, rule signals, flag decision. UNIQUE on that key |
| `lifecycle_events` | observation | Added/removed/mined/replaced exactly as recorded; replacement link; source message ID |
| `schema_migrations` | applied SQL file | Filename and SHA-256 |

Design choices worth knowing:

- **Idempotent loads.** Re-loading the same file hits a unique key (`scores`) or primary key (`lifecycle_events`), so it adds nothing. A row with the same key but *different* content is counted as a conflict and never overwritten; `load-results` then exits with code 4.
- **No foreign key from `scores` to `transactions`**, so scores stay inspectable if transaction rows expire.
- **Lifecycle status is derived, not stored by hand.** After events load, each touched transaction's status is recomputed from all its stored evidence: the latest event time wins, and at the same time `mined` beats `replaced` beats `removed` beats `added`. Loading older events after newer ones therefore cannot roll a status back. `removed_unknown_reason` is deliberate: the feed says a transaction left the mempool, not why. A `confirmed` status is the latest evidence, not a permanent fact, since a reorganization could undo it.
- **Migrations are forward-only SQL files** in `src/blockpulse/migrations/`, applied in filename order and recorded with a checksum. Editing an already-applied file makes `db-init` fail; add a new numbered file instead.

## API

Read-only: each request uses a connection set to read-only, and the API contains no feature or model logic. Interactive docs at `/docs`.

| Route | Purpose |
| --- | --- |
| `GET /healthz`, `GET /readyz` | Process is up; database reachable (503 if not) |
| `GET /tx/{txid}` | Transaction, its scores, and lifecycle events. 404 explains it may be unobserved, unloaded, or expired |
| `GET /alerts?since=&detector=&limit=&cursor=` | Flagged scores, newest first, keyset pagination (`next_cursor`), limit 1-200 |
| `GET /alerts/{id}` | One flagged score with transaction and lifecycle evidence |
| `GET /models/current` | Detector/feature versions that produced stored scores |
| `GET /pipeline/status` | Row counts, newest loaded times, status breakdown |

Malformed input (a txid that isn't 64 hex characters, a bad cursor, `limit=0`) is rejected with 422 before the database is touched. Every alert response repeats that a flag is not evidence of wrongdoing.

## Trace one transaction

```powershell
.\.venv\Scripts\python.exe -m blockpulse trace <txid> --capture data/raw/<run>/messages.jsonl --brief
```

It prints what Postgres holds, finds the original raw message in the capture file, recomputes the features from that archived payload (no network), and reports whether they match the stored ones. This is the M2 "observation to stored result and back" check. Exit code 4 means a mismatch.

## Tests and verification

Postgres tests are skipped unless `BLOCKPULSE_TEST_DSN` points at a throwaway database (the tests drop its `public` schema):

```powershell
docker compose --profile m2 exec postgres psql -U blockpulse -d blockpulse -c "CREATE DATABASE blockpulse_test"
$env:BLOCKPULSE_TEST_DSN = "postgresql://blockpulse:blockpulse_dev@127.0.0.1:5432/blockpulse_test"
.\.venv\Scripts\python.exe -m pytest -q
```

Verified 2026-10-12: with the database, 78 passed and 1 skipped (the optional loopback WebSocket test); without it, 65 passed and 14 skipped.

Results on the real ten-minute capture (402 messages):

| Check | Result |
| --- | --- |
| `load-results` | 2,732 inserted; a second run: 0 inserted, 2,732 duplicates |
| Flagged rows in Postgres | 73, matching the Phase 1 report |
| `load-events` | 6,983 inserted; a second run: 0 inserted, 6,983 duplicates, 0 status changes |
| Transactions | 6,259, equal to the capture's unique transaction IDs |
| Lifecycle status | 3,468 confirmed, 2,211 observed, 421 removed (reason unknown), 159 superseded |
| `trace` on a flagged 50-input transaction | Found at line 401 of the capture; recomputed features matched |
| Grafana | Datasource health OK; all 9 panel queries returned the figures above through Grafana's query API |
| End-to-end test | Fixture through process, detect, load, trace, and API; a full second pass left all three table counts unchanged |

Limitations: the dashboard was verified through its API, not by eye; loading is one row at a time (about 28 seconds for 6,983 events), fine for these captures but not for bulk history; the status projection ignores `block_disconnected` evidence because the source doesn't supply it; and the rules remain uncalibrated, so none of this says anything about real-world detection quality.
