# M2.1: Postgres query store

Step M2.1 of [milestone M2](PROJECT_PLAN.md#m2--features-baseline-and-inspectable-results). It adds a database and the first two tables. **Nothing writes to them yet**; M2.2 loads the existing screened results.

## Why a database now

The JSONL and Parquet files are good for replay and analysis, but an API needs to answer questions like "the 20 newest flagged transactions" or "everything about this txid" quickly. Postgres gives us indexes for that, and constraints that make retries safe.

## Run it

```powershell
pip install -e ".[m2,dev]"
docker compose --profile m2 up -d --wait postgres
.\.venv\Scripts\python.exe -m blockpulse db-init
```

`db-init` applies any migration that hasn't run. Running it again is a no-op. Stop the database with `docker compose --profile m2 stop postgres`; its data lives in the `postgres-data` volume and survives.

The default connection is `postgresql://blockpulse:blockpulse_dev@127.0.0.1:5432/blockpulse`, bound to localhost only. The password is a local-development default; override it with `POSTGRES_PASSWORD` in Compose and `BLOCKPULSE_DSN` for the app. The container is limited to 512 MB.

## Tables

| Table | One row per | Purpose |
| --- | --- | --- |
| `transactions` | txid | First/last seen times and a lifecycle status (`observed_unconfirmed` by default; revised in M2.4) |
| `scores` | run, txid, feature version, rule version | Features, rule signals, and the flag decision for one screened transaction |
| `schema_migrations` | applied migration | Records which SQL files ran, with a checksum |

Two design choices worth knowing:

- **`scores` has a UNIQUE key** on `(run_id, txid, feature_set_version, ruleset_version)`. Loading the same result twice hits the constraint instead of creating a duplicate. This is the "idempotent sink" idea from the plan (section 7.2); M2.2 uses it.
- **No foreign key from `scores` to `transactions`.** Transaction rows may expire later, and scores must stay inspectable when they do.

## Migrations

Schema changes are SQL files in `src/blockpulse/migrations/`, applied in filename order and recorded with a SHA-256. If an already-applied file is edited, `db-init` fails instead of letting databases drift apart. To change the schema, add a new numbered file; never edit an applied one.

## Tests

```powershell
# one-time: a throwaway database (the tests DROP its public schema)
docker compose --profile m2 exec postgres psql -U blockpulse -d blockpulse -c "CREATE DATABASE blockpulse_test"
$env:BLOCKPULSE_TEST_DSN = "postgresql://blockpulse:blockpulse_dev@127.0.0.1:5432/blockpulse_test"
.\.venv\Scripts\python.exe -m pytest -q
```

Without `BLOCKPULSE_TEST_DSN` the four Postgres tests are skipped. Verified 2026-10-12: 66 passed, 1 skipped (the optional loopback WebSocket test). Applying migrations twice, rejecting a modified applied migration, and rejecting a duplicate score row are all covered.
