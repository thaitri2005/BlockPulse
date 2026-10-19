# 07 · Unattended collection

Collect live traffic for hours or days without anyone running commands. The feed carries about 480,000 added transactions a day, so a few days is plenty: the AWS run collected 2.65 million in 89 hours.

## One collector, two ways to run it

Every runner calls the same tested commands in [`src/blockpulse/collector.py`](../../src/blockpulse/collector.py):

| Command | What it does |
| --- | --- |
| `blockpulse collect-loop` | Captures sessions back to back into `data/raw/session-*` until a deadline, a session limit, or low disk (default: stop below 15 GiB free). Each session survives up to 300 reconnects. Stops cleanly on Ctrl+C or SIGINT. |
| `blockpulse process-pending` | For every finished capture not yet handled: process, rule-screen, write the [lake](05-lake-and-athena.md) tables through the quality gate, gzip the bulky events file, add a row to `data/capture_log.csv`. `--load-db` also loads Postgres once per session. Safe to repeat: finished stages are skipped, an interrupted stage is moved aside (never deleted) and redone, and a lock stops two processors colliding. |
| `blockpulse collect` | One `collect-loop` session, then `process-pending`. |
| `blockpulse sync-pending` | Uploads each processed session and its lake files to S3 once, plus `status/status.json` and the capture log. |

| Runner | Best for | Status |
| --- | --- | --- |
| **AWS instance** ([08](08-aws.md)) | Round-the-clock collection for days, laptop off | **Ran** 2026-10-15 to 19: 89 hourly sessions, 0 failures, about $0.80 |
| **Windows Task Scheduler** (`scripts/install_collector.ps1`) | Collecting on the laptop with no cloud account | Ready; the collector is unit-tested, the scheduled task itself has not been installed |

An Airflow runner was written and removed on 2026-10-20 without ever running (its image wouldn't download here). Airflow returns in roadmap step DE5 as a batch DAG with backfills.

## Laptop (Windows Task Scheduler)

```powershell
cd D:\BlockPulse
docker compose --profile m2 up -d --wait postgres      # the hourly task loads into Postgres
powershell -File scripts/install_collector.ps1          # installs an hourly task (59-minute sessions)
Get-ScheduledTask BlockPulseCollector | Get-ScheduledTaskInfo
powershell -File scripts/install_collector.ps1 -Remove  # uninstall; collected data stays
```

Each run calls `scripts/capture_session.ps1`, which is `blockpulse collect --load-db`. Processing overlaps the next hour's capture, so the task allows parallel instances. **The laptop must stay awake**: set sleep to "Never" while plugged in.

## Disk

Measured on the AWS run: about 200 MB an hour after compaction (raw 123 MB, processed 47 MB with gzipped events, screened 26 MB, lake 7 MB), so 4 to 5 GB a day. The raw captures are the source of truth and the largest part.

## After collecting

1. If the data was collected on AWS, download it ([08](08-aws.md#download-the-data-and-use-it)).
2. `blockpulse process-pending` builds anything missing, including lake tables for older sessions.
3. Rerun the [ML evaluation](06-ml-evaluation.md) on a new manifest and compare with the previous one.
4. Optional: `blockpulse process-pending --load-db` for the Postgres dashboard. Loading is row by row (about 4 ms a row), so millions of rows take hours; the lake and DuckDB answer the same questions in seconds.
