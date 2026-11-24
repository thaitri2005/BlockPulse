# Exercise 9: evidence versus projection

The goal is to see why BlockPulse stores two different things about a transaction's life: every observation exactly as it arrived, and one status computed from those observations. A **lifecycle event** is one recorded observation ("added to the mempool", "removed", "mined", "replaced"). A **projection** is a value derived from stored records that can be thrown away and recomputed at any time. Here the events are the evidence and `transactions.lifecycle_status` is the projection. By the end you should be able to explain why loading old events after new ones cannot roll a status back.

## Run it

The first step is offline. It turns the bundled synthetic fixture into events:

```powershell
$py = ".\.venv\Scripts\python.exe"
& $py -m blockpulse process examples/fixtures/mempool_messages.jsonl --output data/learning/ex09/processed
```

The rest needs Postgres. Prerequisite, from [guide 04](../guides/04-postgres-api-dashboard.md): `pip install -e ".[m2,dev]"` and the `m2` Compose profile (about 1.1 GB). Use a scratch database so nothing mixes with real results:

```powershell
docker compose --profile m2 up -d --wait
docker compose --profile m2 exec -T postgres psql -U blockpulse -d blockpulse -c "CREATE DATABASE blockpulse_ex09"
$env:BLOCKPULSE_DSN = "postgresql://blockpulse:blockpulse_dev@127.0.0.1:5432/blockpulse_ex09"
& $py -m blockpulse db-init
& $py -m blockpulse load-events data/learning/ex09/processed
& $py -m blockpulse load-events data/learning/ex09/processed    # the same file again
docker compose --profile m2 exec -T postgres psql -U blockpulse -d blockpulse_ex09 -c "SELECT left(txid, 4) AS tx, lifecycle_status, first_seen_at, last_seen_at FROM transactions ORDER BY txid"
```

## Open the output

Open `data/learning/ex09/processed/events.jsonl`. These are the seven events `process` wrote from the fixture (txids shortened, other fields left out):

| txid | event_type | event_time | source_message_id |
| --- | --- | --- | --- |
| `aaaa…` | added | 08:00:02 | synthetic-demo:2 |
| `bbbb…` | added | 08:00:02 | synthetic-demo:2 |
| `aaaa…` | added | 08:00:03 | synthetic-demo:3 |
| `bbbb…` | mined | 08:00:03 | synthetic-demo:3 |
| `aaaa…` | removed | 08:00:04 | synthetic-demo:4 |
| `bbbb…` | replaced | 08:00:04 | synthetic-demo:4 |
| `cccc…` | added | 08:00:05 | synthetic-demo:5 |

Before running the `SELECT`, write down the status you expect for each of the three transactions. Then compare. Reading the code, I expect `removed_unknown_reason` for `aaaa`, `superseded` for `bbbb` and `observed_unconfirmed` for `cccc`; I could not run Postgres in the session that wrote this page, so treat that as a prediction to check, not a recorded result.

Also read the two `load-events` summaries. The second run should report `inserted: 0`, every row as a duplicate, and `status_changes: 0`. A load is **idempotent** when repeating it changes nothing; here the primary key on `event_id` does that work (`ON CONFLICT (event_id) DO NOTHING`).

## Read one function

Open [lifecycle.py](../../src/blockpulse/lifecycle.py) and find `_PROJECT_BODY`, the SQL behind `PROJECT`. Trace `bbbb` through it:

```sql
SELECT DISTINCT ON (e.txid) e.txid,
    CASE e.event_type WHEN 'added' THEN 'observed_unconfirmed' ... END AS status
FROM lifecycle_events e
ORDER BY e.txid, e.event_time DESC,
         CASE e.event_type WHEN 'mined' THEN 4 WHEN 'replaced' THEN 3 WHEN 'removed' THEN 2 ELSE 1 END DESC,
         e.event_id
```

`DISTINCT ON (txid)` is a Postgres feature that keeps the first row of each group after sorting. The sort puts the latest `event_time` first, and at an identical time the stronger kind first. For `bbbb` the rows are added (:02), mined (:03), replaced (:04). The latest is `replaced`, so the status becomes `superseded`.

Notice what the query does not read: the order in which rows were inserted, or the status the transaction had before. The answer depends only on the set of stored events. That is why `tests/test_lifecycle.py` can load a `mined` event first and an older `added` event second and still require `confirmed`: the second load adds evidence, and the projection is recomputed from all of it. `first_seen_at` moves earlier; the status does not move backwards.

Two smaller things to find in `load_events()`:

- Rows go into `stage_events` first. A **staging table** is a temporary table that holds incoming rows so they can be merged with a few set-based statements instead of one statement per row.
- `removed` maps to `removed_unknown_reason` on purpose. The feed says a transaction left the mempool, not why.

## Change one small thing

Add a test to `tests/test_lifecycle.py`, using its `event()` and `write()` helpers: one transaction with `event("mined", 10)` and `event("replaced", 20)`, loaded together, must end as `superseded`. This is the `bbbb` case from the fixture.

Then, temporarily, delete `e.event_time DESC,` from `_PROJECT_BODY` and run:

```powershell
$env:BLOCKPULSE_TEST_DSN = "postgresql://blockpulse:blockpulse_dev@127.0.0.1:5432/blockpulse_test"
& $py -m pytest -q tests/test_lifecycle.py
```

Your new test should fail, because `mined` now wins on strength alone. Look at whether the four existing tests in that file still pass: by my reading each of them happens to have the stronger kind at the later time, so they might not notice the missing line. That is unverified here; if it is true, your test closes a real gap and is worth keeping. Restore the line and rerun until everything passes. Only ever point `BLOCKPULSE_TEST_DSN` at `blockpulse_test`: the tests drop its `public` schema.

When done: `docker compose --profile m2 exec -T postgres psql -U blockpulse -d blockpulse -c "DROP DATABASE blockpulse_ex09"` and `docker compose --profile m2 stop`.

## Question to carry forward

`bbbb` was mined at :03 and "replaced" at :04, and the projection says `superseded`. Guide 04 says a `confirmed` status is the latest evidence, not a permanent fact. Should a later `replaced` or `removed` be able to override `mined`, or should some evidence be final? What would the events table need to record (for example a block being disconnected) to decide that properly?

## What was run for this page

- Ran (Linux, 2026-11-24): `blockpulse process` on the fixture; the event table above is copied from its `events.jsonl`. `pytest tests/test_lifecycle.py` without a database: all four tests skip.
- Not run (no Postgres in that session): `docker compose`, `db-init`, `load-events`, the `SELECT`, and the test experiment. Those steps are written from `lifecycle.py`, `tests/test_lifecycle.py` and guide 04.
