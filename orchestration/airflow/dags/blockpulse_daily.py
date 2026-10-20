"""BlockPulse batch tier: one DAG run per UTC day of captures.

    screen ---> lake ---+--> load (Postgres, then compact) <--- prepare_database
                        +--> dbt_build (models and their data tests)

Every task is one `blockpulse` command, and every command is idempotent: for the run's date it does only the stages
whose output is missing. A retry, a rerun or a backfill of past days is therefore safe and produces the same files.
Airflow decides when, in what order and how often to retry; what each step does stays in the blockpulse package,
the same code the AWS instance and the laptop task run (decision D15).

A run covers the captures that *started* on its day (data interval start). A day that still has a capture
recording is refused by `process-pending --date`, and the screen task retries until the capture has finished.
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

DAY = "{{ data_interval_start.strftime('%Y-%m-%d') }}"
ROOT = '"$BLOCKPULSE_PIPELINE_ROOT"'  # set in compose.yaml; the data root this DAG owns
# A pool is a named limit on concurrent tasks. One slot: tasks that write the data root run one at a time across every
# run, scheduled or backfill (max_active_runs only limits scheduled runs). Created when the container starts.
DATA_ROOT_POOL = "blockpulse_data_root"


def blockpulse(task_id: str, arguments: str, *, pool: str | None = DATA_ROOT_POOL, **kwargs) -> BashOperator:
    if pool:
        kwargs["pool"] = pool
    return BashOperator(task_id=task_id, bash_command=f"blockpulse {arguments}", **kwargs)


with DAG(
    dag_id="blockpulse_daily",
    description="Process, screen, lake, load and model one UTC day of mempool captures",
    # Data-interval schedule: the run for day D starts after D ends and sees D as data_interval_start.
    # (Airflow 3's plain cron schedules have no interval, so "{{ ds }}" would be the day the run starts.)
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 12, tz="UTC"),
    catchup=False,  # past days run only when asked: `airflow backfill create`
    max_active_runs=1,  # for scheduled runs; the pool below also covers backfills (process-pending holds a lock too)
    dagrun_timeout=timedelta(hours=3),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(hours=1),
    },
    tags=["blockpulse", "batch"],
) as dag:
    # The last capture of a day can still be recording just after midnight: keep retrying for about 1.5 hours.
    screen = blockpulse("screen", f"process-pending --root {ROOT} --date {DAY} --no-lake --no-compact", retries=6)
    lake = blockpulse("lake", f"process-pending --root {ROOT} --date {DAY} --no-compact")
    prepare_database = blockpulse("prepare_database", "db-init --create-database", pool=None)  # Postgres only
    load = blockpulse("load", f"process-pending --root {ROOT} --date {DAY} --load-db")
    dbt_build = blockpulse("dbt_build", f"dbt --root {ROOT} build")

    screen >> lake >> [load, dbt_build]
    prepare_database >> load
