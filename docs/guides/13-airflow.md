# 13 · Airflow: scheduling, retries and backfills

Airflow runs the batch tier as one DAG run per UTC day of captures. A *DAG* (directed acyclic graph) is the list of tasks and the order they depend on each other; a *DAG run* is one execution of it for one data interval, here one day. Airflow decides when a run starts, in what order its tasks go, and what happens when one fails. What each task does stays in the `blockpulse` package: every task is one CLI command, the same code the AWS instance and the laptop task run (decision D15).

```powershell
docker compose --profile m2 up -d --wait                 # Postgres (the load target)
docker compose --profile airflow up -d --wait --build    # Airflow at http://127.0.0.1:8080 (about 1.0-1.3 GB of memory)
docker compose exec airflow airflow dags unpause blockpulse_daily
docker compose exec airflow airflow backfill create --dag-id blockpulse_daily --from-date 2026-10-13 --to-date 2026-10-15 --max-active-runs 1
docker compose --profile airflow stop                     # when done
```

The DAG owns one data root, `data/orchestrated/` (mounted at `/opt/blockpulse/data`). Captures to process go in its `raw/` folder; nothing else under `data/` is visible to the container, so the DAG can't touch the evidence there. It loads into a separate database, `blockpulse_orchestrated`, which it creates on first use.

## The DAG (`orchestration/airflow/dags/blockpulse_daily.py`)

```text
screen ---> lake ---+--> load (Postgres, then compact) <--- prepare_database
                    +--> dbt_build (models and their 28 data tests)
```

| Task | Command | What it does for the run's date |
| --- | --- | --- |
| `screen` | `process-pending --date D --no-lake --no-compact` | Process and screen every capture that started on D. Refuses the date while a capture from it is still recording |
| `lake` | `process-pending --date D --no-compact` | Write the lake tables through the quality gate |
| `prepare_database` | `db-init --create-database` | Create the database if missing, apply migrations |
| `load` | `process-pending --date D --load-db` | Bulk-load into Postgres, then gzip events and raw |
| `dbt_build` | `dbt build` | Rebuild the marts and run the data tests over the whole lake |

Why it is safe to run any task again: `process-pending` does only the stages whose output is missing for that date (it checks each stage's summary file), Postgres loads are duplicate-safe, and dbt rebuilds its tables. That property is called *idempotence*, and it is what makes retries and backfills boring. A task that fails halfway leaves a folder without a summary; the next attempt sets it aside and redoes the stage.

### Concepts in the file

- **Data interval.** The run for day D starts after D ends and gets D as `data_interval_start`. In Airflow 3 a plain cron schedule has no interval (the logical date is the moment the run starts), so the DAG uses `CronDataIntervalTimetable` explicitly. With the plain cron default, `{{ ds }}` would have pointed at the wrong day.
- **Retries with backoff.** Two retries by default, 1 minute then 2, up to 30 minutes. `screen` gets six: just after midnight the last capture of the day can still be recording, and `process-pending --date` refuses the day until it finishes, so retrying is the wait.
- **Catchup off; backfills on request.** Turning the DAG on doesn't replay every day since `start_date`; `airflow backfill create` replays a chosen range.
- **A pool for the shared resource.** A pool is a named limit on how many tasks may run at once. `blockpulse_data_root` has one slot, and every task that writes the data root uses it. `max_active_runs=1` only limits scheduled runs, not backfill runs, which is how the collisions below happened.
- **Orchestrator and workload apart.** The image is `apache/airflow:slim-3.3.2-python3.11` plus BlockPulse installed from the hashed lock into its own virtual environment (`/opt/blockpulse/venv`). Airflow's dependencies and BlockPulse's pins never mix; the DAG calls a `blockpulse` wrapper script. The source is mounted read-only, so code changes need no rebuild.
- **Metadata on SQLite.** Airflow's own state (runs, task attempts) is in SQLite on a volume, with the LocalExecutor. That is enough for one machine, and it keeps Airflow independent of the Postgres it loads into, so stopping Postgres can't take the scheduler down.

`tests/test_airflow_dag.py` loads the DAG file with stand-in Airflow classes (Airflow isn't a project dependency) and checks that every task's command parses with the real CLI, plus the order, pool and retry settings. Airflow itself parses the DAG in the image: `docker compose exec airflow airflow dags list-import-errors`.

## Results (2026-10-20)

### Backfill of three past days equals the direct run

9 captures (107 MiB of raw, 2026-10-13 to 2026-10-15) were copied into two fresh data roots. One was processed directly (`process-pending`, then `dbt build`) and one by `airflow backfill create` for the three days.

| Table | Rows | Rows that differ (`EXCEPT ALL`, both ways) |
| --- | ---: | ---: |
| lake `transactions` | 117,212 | 0 |
| lake `lifecycle_events` | 270,945 | 0 |
| lake `capture_sessions` | 9 | 0 |
| `fct_transactions` | 117,212 | 0 |
| `int_transaction_lifecycle` | 146,862 | 0 |
| `fct_hourly_activity`, `fct_outcomes_by_flag`, `dim_capture_sessions` | 11, 8, 9 | 0 |

Postgres (`blockpulse_orchestrated`) ended with 117,212 scores, 270,945 lifecycle events and 146,862 transactions across 9 runs: the same counts as the lake and the lifecycle model. All 9 sessions were compacted after loading.

| Task, day 2026-10-15 (7 captures, 100 MiB) | Time |
| --- | ---: |
| `screen` | 404 s |
| `lake` | 67 s |
| `load` | 433 s |
| `dbt_build` (all three days) | 15 s |

The direct run processed all 9 captures in 179 s natively. Tasks in the container read and write `data/` through a Docker bind mount from the Windows drive, which makes them roughly 2-3x slower; on Linux (or the AWS instance) there is no such penalty.

### What went wrong, and what it fixed

The first backfill met four real failures. Each was recovered by a retry or fixed in code, and each is now covered by a test.

| What happened | Why | Outcome |
| --- | --- | --- |
| `screen` of one run refused: "another processor holds the lock" | Unpausing the DAG also started the scheduled run for the latest day, at the same moment as the backfill; `max_active_runs` doesn't cover backfill runs | Retried and succeeded. Fix: the one-slot pool `blockpulse_data_root` |
| `dbt_build` failed in two runs | Two builds opened the same DuckDB file | Retried and succeeded. Same fix (the pool) |
| `prepare_database` failed | Two runs both saw the database missing and both ran `CREATE DATABASE`; one got a unique-key violation | Retried and succeeded. Fix: "already exists" now counts as success; a test runs four creators at once and fails without the fix |
| `load` of 2026-10-15 killed after 48 min, then refused twice | The laptop went into standby for 54 min mid-load (Windows logged it). On wake Airflow found no heartbeat and sent SIGTERM. Python's default SIGTERM handling exits without cleanup, so the lock file stayed, and both retries were refused because the lock was under 3 hours old | Lock removed by hand after checking its process was gone, task cleared, load succeeded (433 s). Fixes: SIGTERM now exits through cleanup code, and a lock whose process no longer exists on the same host is taken over. Both are tested on Linux in CI |

The last one is decision D21 in practice: a scheduler on a laptop that sleeps loses work. Long or unattended schedules belong on a machine that stays awake (the AWS instance).

### Drill: Postgres down during a run

Postgres was stopped, then 2026-10-12 (2 captures) was backfilled. `screen`, `lake` and `dbt_build` don't need Postgres and went ahead. `prepare_database` failed after 6.5 s and waited to retry; `load` waited behind it. Postgres was started again 25 s later; the retry a minute after the failure succeeded (5.7 s), then `load` (208 s). The run finished as a success in 333 s with no manual step.

The host-side script that watched this drill crashed with "cannot allocate memory" in the Docker CLI while Airflow, Postgres and the load were all running. Airflow wasn't affected, but it's a reminder that the full stack (about 3 GB of containers) is close to what this laptop runs comfortably.

## Limits

- Runs on one machine. LocalExecutor and SQLite metadata don't scale out; the next step would be Postgres metadata and a Celery or Kubernetes executor (DE7).
- `dbt_build` rebuilds every mart from the whole lake on every run (15-30 s here). Incremental models would rebuild only the changed days.
- Airflow 3 removed task SLAs; their replacement (deadline alerts) isn't configured. `dagrun_timeout` (3 h) and `execution_timeout` (1 h) bound a stuck run.
- The image is 2.4 GB, about 0.9 GB of it the BlockPulse environment (the lock installs every extra, including scikit-learn and dbt).
- The DAG covers the batch tier. The Kafka streaming sink (`kafka-lake`) is a long-running consumer, not a scheduled job, so Airflow doesn't run it.
