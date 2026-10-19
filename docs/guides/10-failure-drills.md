# 10 · Failure drills

Break a component on purpose in the middle of real work, recover, and prove the invariant: **after a crash and a restart, nothing is lost, and every input is represented exactly once where it matters.** Each drill uses recorded traffic, isolated infrastructure, and processes killed without warning. A drill passes only if every check passes; reports go to `data/drills/`.

```powershell
docker compose -p blockpulse-bench --profile m1 up -d --wait kafka     # throwaway broker (stop the normal one first: same port)
.\.venv\Scripts\python.exe -m blockpulse drill enrich-crash             # 2 hours of traffic by default
docker compose -p blockpulse-bench --profile m1 down -v; docker compose -p blockpulse-bench --profile m1 up -d --wait kafka
.\.venv\Scripts\python.exe -m blockpulse drill broker-restart
docker compose -p blockpulse-bench --profile m1 down -v
docker compose --profile m2 up -d --wait postgres
.\.venv\Scripts\python.exe -m blockpulse drill postgres-crash           # uses a separate blockpulse_drill database
```

Each Kafka drill needs a fresh broker, because it reads the whole topics back. The enrichment worker points its REST fallback at an unreachable local address, so a drill can never call the public API.

## Results (2026-10-20)

### 1. Enrichment worker killed mid-stream: passed

4,641 messages (2 hours of Saturday traffic) published; the worker was hard-killed after 6 seconds (no clean shutdown, no final checkpoint), then restarted with the same consumer group.

| Check | Result |
| --- | --- |
| Restarted worker resumed from its committed offsets | Processed the remaining 3,395 messages, final lag 0 |
| No message lost | 0 of 4,641 missing from the enriched topic |
| Duplicates bounded | 44 messages enriched twice: those processed after the last checkpoint but before the kill |
| Idempotent sink | The Parquet archive read 4,685 records and holds exactly 4,641 files |

This is at-least-once delivery working as designed: the cost of a crash is a small, bounded replay, and the sink deduplicates by `message_id`.

**Bug found by the first run.** The restart failed: Kafka keeps a dead member in its consumer group until the session timeout (45 s by default), and the restarted worker gave up waiting for partitions after 30 s. So a restart soon after a crash, which is exactly when you'd restart, failed every time. All three consumers now use a 10 s session timeout with 3 s heartbeats and wait up to 60 s for assignment.

### 2. Broker restarted mid-stream: passed

Same 4,641 messages; the Kafka container was restarted 5 seconds into enrichment.

| Check | Result |
| --- | --- |
| Consumption completes after the broker is back | Yes, after a rerun; final lag 0 |
| No message lost | 0 of 4,641 missing |
| Duplicates bounded | 233 messages enriched twice |
| Idempotent sink | 4,874 records read, exactly 4,641 files |

The worker **did not survive** the restart: the broker forgot the consumer group, so the next offset commit was rejected (`UNKNOWN_MEMBER_ID`), and the worker stopped rather than committing anything uncertain. That is the safe failure, and rerunning it recovered everything. A long-running deployment should run the worker under a supervisor that restarts it (systemd `Restart=on-failure`, as the AWS collector services already do); making the worker rejoin by itself is a possible improvement.

### 3. Postgres load crashed mid-transaction: passed

One hourly session (57,338 scored transactions) loaded into a separate `blockpulse_drill` database, crashed two ways.

| Check | Result |
| --- | --- |
| Loader process hard-killed after 8 s | Nothing from the killed load visible (0 rows) |
| Rerun | 57,338 inserted, exactly the session's rows |
| Second rerun | 0 inserted, 57,338 recognized as duplicates |
| Postgres container restarted mid-load | Load failed (exit 1); 0 rows visible |
| Rerun after the restart | 57,338 inserted, exactly once |

Each session loads in one transaction, so a crash leaves nothing behind, and the unique key on `(run_id, txid, feature version, rule version)` makes reruns harmless.

## Limits

- One machine and one broker: the drills test correctness under crashes, not high availability.
- The worker doesn't rejoin a group by itself after a broker restart; it relies on being restarted.
- Postgres loading is row by row (about 4 ms a row), which is why this drill takes about 18 minutes. Correctness, not speed, was the point.
