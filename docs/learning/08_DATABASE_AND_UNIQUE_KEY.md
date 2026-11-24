# Exercise 8: why a database, and what a unique key buys us

The goal is to understand how screened results get into Postgres and why loading the same file twice is harmless. A **table** is a set of rows with fixed, typed columns. A **constraint** is a rule the database enforces on every row, whoever writes it. A **unique key** is a constraint saying no two rows may share the same values in certain columns. A **migration** is a numbered SQL file that changes the database structure, applied once and recorded. An **idempotent write** is one you can repeat without changing the result.

A database adds two things to the files: readers can query by transaction or time without scanning a file, and the rules about what may be stored live with the data.

## Run it

**Without a database.** Produce the file that will be loaded. This reuses the generated capture from [Exercise 2](02_STREAM_TO_FILE.md) and the feature table from [Exercise 3](03_FILE_TO_FEATURES.md); skip any step whose folder already exists.

```powershell
.\.venv\Scripts\python.exe -m blockpulse synth-capture --output data/learning/synth-200 --messages 200
.\.venv\Scripts\python.exe -m blockpulse process data/learning/synth-200/messages.jsonl --output data/learning/ex03-a
.\.venv\Scripts\python.exe -m blockpulse detect data/learning/ex03-a/features.jsonl --output data/learning/ex08-screened
```

List the migrations and their checksums:

```powershell
@'
from blockpulse.db import list_migrations
for version, sql, digest in list_migrations():
    print(version, digest[:16], len(sql.splitlines()), "lines")
'@ | .\.venv\Scripts\python.exe -
```

**With Postgres.** Start the container (512 MB limit) and point this shell at a separate learning database, so generated rows never sit beside recorded ones:

```powershell
docker compose --profile m2 up -d --wait postgres
$env:BLOCKPULSE_DSN = "postgresql://blockpulse:blockpulse_dev@127.0.0.1:5432/blockpulse_learning"
.\.venv\Scripts\python.exe -m blockpulse db-init --create-database
.\.venv\Scripts\python.exe -m blockpulse db-init
```

Then load the screened results twice under the same run ID:

```powershell
.\.venv\Scripts\python.exe -m blockpulse load-results data/learning/ex08-screened --run-id learn-ex08
.\.venv\Scripts\python.exe -m blockpulse load-results data/learning/ex08-screened --run-id learn-ex08
```

When you finish: `Remove-Item Env:BLOCKPULSE_DSN`, then `docker compose --profile m2 stop`.

## Open the output

The migration listing printed, on my run:

```text
001_initial 33d0731aab0e4001 33 lines
002_lifecycle_events fecd997c1688caa5 16 lines
```

Open `src/blockpulse/migrations/001_initial.sql`. It creates `transactions` and `scores`. The line this exercise is about is in `scores`:

```sql
UNIQUE (run_id, txid, feature_set_version, ruleset_version)
```

One row per run, transaction, feature version and rule version. Also read the `CHECK` and `NOT NULL` constraints: the database will refuse a negative `signal_count` or a missing `received_at`.

The file being loaded is `data/learning/ex08-screened/screened.jsonl`. Its first line (shortened) from my run:

```text
{"schema_version":1,"txid":"9aa3f488...","event_id":"4481bb75...","received_at":"2026-01-01T00:00:00.250+00:00",
 "source_mode":"synthetic","feature_set_version":"transaction-v2","features":{"n_in":1,"n_out":2,...},
 "ruleset_version":"structural-rules-v1","signal_count":0,"is_flagged":false,"signals":[],...}
```

Three of the four key columns are in that line. The fourth, `run_id`, comes from `--run-id`.

I could not run Postgres in this session, so there is no database output to show. On your run, compare `applied` and `already_applied` in the two `db-init` summaries, and `inserted` and `duplicates` in the two `load-results` summaries. `tests/test_results.py` states what to expect on a two-row file: `(inserted, duplicates)` is `(2, 0)` on the first load and `(0, 2)` on the second, with two rows in `scores` after both.

## Read one function

Open `src/blockpulse/db.py` and read `apply_migrations()`. Trace `digest`, the SHA-256 checksum of one migration file:

```python
for version, sql, digest in list_migrations(directory):
    if version in recorded:
        if recorded[version] != digest:
            raise RuntimeError(f"Migration {version} was already applied but its file has changed")
        already.append(version)
        continue
    connection.execute(sql)
    connection.execute("INSERT INTO schema_migrations (version, sha256) VALUES (%s, %s)", (version, digest))
    applied_now.append(version)
```

`list_migrations()` reads each `.sql` file in filename order and hashes its text. `recorded` is what the `schema_migrations` table says was applied before. A new version is executed and its checksum stored. A known version is skipped, unless its checksum differs from the stored one, which stops everything. Hence the project rule: never edit an applied migration; add a new numbered file.

The whole loop runs inside `connection.transaction()`. A **transaction** is a group of statements that either all take effect or none do, so a migration that fails halfway leaves no half-built tables and no record of itself.

Then read `INSERT_NEW` in `src/blockpulse/results.py`. It ends with `ON CONFLICT (run_id, txid, feature_set_version, ruleset_version) DO NOTHING`. The loader inserts and lets the unique key reject repeats. `CLASSIFY_EXISTING` runs just before it and counts repeats with equal content as duplicates and repeats with different content as conflicts. A conflict is reported and the stored row is never overwritten.

## Change one small thing

See what an edited migration looks like to the checksum, on a copy. Do not touch the real folder.

```powershell
Copy-Item -Recurse src/blockpulse/migrations data/learning/ex08-migrations
Add-Content data/learning/ex08-migrations/001_initial.sql "-- a comment added after the fact"
@'
from pathlib import Path
from blockpulse.db import list_migrations
for folder in (None, Path("data/learning/ex08-migrations")):
    found = list_migrations(folder) if folder else list_migrations()
    print([(version, digest[:16]) for version, sql, digest in found])
'@ | .\.venv\Scripts\python.exe -
```

My run printed:

```text
[('001_initial', '33d0731aab0e4001'), ('002_lifecycle_events', 'fecd997c1688caa5')]
[('001_initial', '96bba5e7e5a1404e'), ('002_lifecycle_events', 'fecd997c1688caa5')]
```

A comment changes no table, yet the checksum differs. A database that had recorded the first value would refuse the second; `test_changed_applied_migration_is_rejected` in `tests/test_db.py` checks that.

Then rerun the tests. Without `BLOCKPULSE_TEST_DSN` the database tests skip. To run them, follow the [Postgres guide](../guides/04-postgres-api-dashboard.md) and point that variable only at `blockpulse_test`, because those tests drop the whole `public` schema.

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_db.py tests/test_results.py
```

## Question to carry forward

The unique key has four columns. What would go wrong if it were `txid` alone, and what would go wrong if it had no `run_id`? Then: `load-results` reports a conflict instead of overwriting. Who would be misled if it silently kept the newer row?

## What was run

Verified on Linux on 2026-11-24: `synth-capture`, `process`, `detect`, both migration-listing scripts (run from a shell here-document, with `cp -r` and shell append in place of `Copy-Item` and `Add-Content`), and `pytest tests/test_db.py tests/test_results.py` without a test database (1 passed, 7 skipped). Not run here, because no Docker or Postgres was available to this session: `docker compose --profile m2 up`, both `db-init` commands, both `load-results` commands, and the seven database tests. Those steps are written from the code, the tests and `scripts/demo_m2.ps1`.
