"""Hourly collection with Airflow: one capture session, then processing of every finished session.

The work itself lives in the tested `blockpulse` commands (`collect-loop`, `process-pending`); this DAG only
schedules them, so the laptop task, this DAG and the AWS instance all behave the same. Two runs may overlap:
one capturing while the previous one processes. Both commands are safe to repeat, so retries are harmless.
"""

from datetime import datetime, timedelta
import os
import subprocess
import sys

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param

ROOT = os.environ.get("BLOCKPULSE_ROOT", "/opt/blockpulse")
DATA = os.path.join(ROOT, "data")


def blockpulse(*args: str) -> str:
    result = subprocess.run([sys.executable, "-m", "blockpulse", *args], cwd=ROOT, capture_output=True, text=True)
    if result.returncode not in (0,):
        raise AirflowException(f"blockpulse {args[0]} exited {result.returncode}: {(result.stderr or result.stdout)[-800:]}")
    return result.stdout


@dag(
    dag_id="blockpulse_collect",
    description="Capture the mempool feed for about an hour, then process, screen, and load finished sessions",
    schedule="0 * * * *", start_date=datetime(2026, 10, 15), catchup=False, max_active_runs=2,
    default_args={"owner": "blockpulse", "retries": 1, "retry_delay": timedelta(minutes=1)}, tags=["blockpulse"],
    params={
        "session_seconds": Param(3540, type="integer", minimum=10, maximum=3600),
        "url": Param("wss://node203.sv1.mempool.space/api/v1/ws", type="string"),
    },
    doc_md=__doc__,
)
def blockpulse_collect():
    @task(retries=0)  # a retried capture would just be a new, shorter session
    def capture(params=None) -> str:
        return blockpulse("collect-loop", "--root", DATA, "--max-sessions", "1",
                          "--session-seconds", str(params["session_seconds"]), "--url", params["url"])

    @task
    def process(_: str) -> str:
        return blockpulse("process-pending", "--root", DATA, "--load-db")

    process(capture())


blockpulse_collect()
