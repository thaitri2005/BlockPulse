# Exercise 24: orchestration with DAGs, retries and backfills

The goal is to understand what a scheduler adds to commands you can already run by hand. Airflow is an **orchestrator**: it decides when a job starts, in what order its steps go, and what happens when one fails. A **DAG** (directed acyclic graph) is the list of steps, called **tasks**, and which must finish before which. You will run one DAG for a few past days, read how a single setting keeps two runs from colliding, and see why that setting was added after a real failure.

## Run it

The DAG reads captures from its own data root, `data/orchestrated/raw/`. Copy two or three finished capture folders there first (copy, never move: `data/` is evidence). Then, from the [Airflow guide](../guides/13-airflow.md):

```powershell
docker compose --profile m2 up -d --wait                 # Postgres (the load target)
docker compose --profile airflow up -d --wait --build    # Airflow at http://127.0.0.1:8080 (about 1.0-1.3 GB of memory)
docker compose exec airflow airflow dags unpause blockpulse_daily
docker compose exec airflow airflow backfill create --dag-id blockpulse_daily --from-date 2026-10-13 --to-date 2026-10-15 --max-active-runs 1
docker compose --profile airflow stop                     # when done
```

Use the dates your copied captures started on. A **backfill** is a set of runs for past days, started on request. Without Docker you can still do the reading and the experiment below; the DAG's test needs neither Airflow nor a container:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_airflow_dag.py
```

## Open the output

In the Airflow UI at `http://127.0.0.1:8080`, open `blockpulse_daily` and look at the grid: one column per **DAG run** (one execution for one day), one row per task. Click a `screen` task and read its log; the first lines show the exact `blockpulse process-pending --date ...` command it ran. Check that the date is the day the run *covers*, not the day you started it.

On disk, look under `data/orchestrated/`: each stage leaves a summary file per session, and the lake tables appear as in Exercise 19.

For comparison, the recorded backfill of 2026-10-13 to 2026-10-15 (guide 13, run on 2026-10-20, 9 recorded captures) produced 117,212 lake transactions and 270,945 lifecycle events, with 0 rows different from processing the same captures directly. That first backfill also met four failures; keep the guide's table "What went wrong, and what it fixed" open for the next section.

## Read one function

Open `orchestration/airflow/dags/blockpulse_daily.py`. It is 62 lines. Trace one setting, the pool:

```python
DATA_ROOT_POOL = "blockpulse_data_root"

def blockpulse(task_id: str, arguments: str, *, pool: str | None = DATA_ROOT_POOL, **kwargs) -> BashOperator:
    if pool:
        kwargs["pool"] = pool
    return BashOperator(task_id=task_id, bash_command=f"blockpulse {arguments}", **kwargs)
```

A **pool** is a named limit on how many tasks may run at once, across every run. This one has a single slot (created in `compose.yaml` with `airflow pools set blockpulse_data_root 1`). Every task built by the helper gets it by default; only `prepare_database` passes `pool=None`, because it touches Postgres and not the data root.

Why not rely on `max_active_runs=1`, a few lines below? Because that limits scheduled runs only. In the first backfill, unpausing the DAG started the scheduled run for the latest day at the same moment as the backfill, and `screen` was refused with "another processor holds the lock" (guide 13). The pool is the fix.

Now read the other names in the file against these definitions:

- **Data interval**: the span of time a run covers. `DAY` is built from `data_interval_start`, so the run for day D processes captures that started on D.
- **Retries with backoff**: a failed task is tried again after a delay that grows each time (`retry_delay` of 1 minute, `retry_exponential_backoff`, capped by `max_retry_delay`).
- **Idempotent task**: one that can run again and leave the same result. `process-pending` does only the stages whose output is missing, which is what makes a retry safe.

Then open `src/blockpulse/collector.py` and read `_Lock`, the second guard. The lock is a file holding `<pid> <host> <time>`. `_stale()` decides when a leftover lock may be taken over: when it is older than three hours, or when it was written on this host by a process that no longer exists. The second rule exists because of the fourth failure in the guide: the laptop slept, Airflow killed the `load` task, the lock file stayed, and both retries were refused.

## Change one small thing

In the DAG file, take the pool away from one task:

```python
dbt_build = blockpulse("dbt_build", f"dbt --root {ROOT} build", pool=None)
```

Run `tests/test_airflow_dag.py` again. `test_dag_order_retries_and_pool` fails, because it requires the four tasks that write the data root to share the pool. Read the assertion, then restore the line and rerun until both tests pass. If Airflow is running, also check that it still parses the file:

```powershell
docker compose exec airflow airflow dags list-import-errors
```

A second, paper experiment: `screen` has `retries=6`. Write down the six delays you expect from the settings in `default_args` and add them up. Compare your total with the comment above the `screen` line, and bring any difference to the next session.

## Question to carry forward

The lock's takeover rule compares the host name and asks whether the process still exists. Inside Docker, what is the host name of the Airflow container, and what happens to it when the container is recreated? If a task dies holding the lock and the container is then rebuilt, which of the two rules frees the lock, and how long are retries refused?

## Verified

Run here on 2026-11-24 (Linux, `.venv/bin/python`): `python -m pytest -q tests/test_airflow_dag.py` (2 passed); the experiment, applied to a scratch copy of the DAG file with the test pointed at the copy (`test_dag_order_retries_and_pool` raised `AssertionError`, the command test still passed); `python -m blockpulse --help`. The DAG file, `_Lock` and `compose.yaml` were read. Not run here: every `docker compose` and `airflow` command, the UI, and the backfill. Row counts and the failure descriptions are quoted from guide 13 (results dated 2026-10-20).
