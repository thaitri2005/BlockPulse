"""Hourly data collection: capture the live feed, then process, screen, and load it.

Each run captures for just under an hour (so the next run starts right after), then runs the same CLI
commands used by hand: process -> detect -> load-results -> load-events. Two runs may overlap: one capturing
while the previous one finishes loading.

Every step is safe to repeat. A step whose output already exists is skipped, so a retry never overwrites data
and a run that died halfway can be cleared and re-run.
"""

import csv
from datetime import datetime, timedelta
import gzip
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.param import Param
from airflow.operators.python import get_current_context

ROOT = Path(os.environ.get("BLOCKPULSE_ROOT", "/opt/blockpulse"))
MIN_FREE_GIB = float(os.environ.get("BLOCKPULSE_MIN_FREE_GIB", "15"))
DEFAULT_URL = "wss://node203.sv1.mempool.space/api/v1/ws"

DEFAULT_ARGS = {"owner": "blockpulse", "retries": 1, "retry_delay": timedelta(minutes=1)}


def cli(*args: str) -> str:
    """Run one blockpulse command from the project root; raise with its error text on failure."""
    result = subprocess.run([sys.executable, "-m", "blockpulse", *args], cwd=ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        raise AirflowException(f"blockpulse {' '.join(args[:2])} exited {result.returncode}: {(result.stderr or result.stdout)[-800:]}")
    return result.stdout


def session_name() -> str:
    return f"session-af-{get_current_context()['ts_nodash']}Z"


@dag(
    dag_id="blockpulse_collect",
    description="Capture the mempool feed for ~an hour, then process, screen, and load it",
    schedule="0 * * * *", start_date=datetime(2026, 10, 15), catchup=False, max_active_runs=2,
    default_args=DEFAULT_ARGS, tags=["blockpulse"],
    params={
        "duration_seconds": Param(3540, type="integer", minimum=10, maximum=3600, description="Capture length"),
        "replay_from": Param("", type="string", description="Testing only: path under the project to an existing messages.jsonl, used instead of a live capture"),
        "url": Param(DEFAULT_URL, type="string"),
    },
    doc_md=__doc__,
)
def blockpulse_collect():
    @task
    def capture(params=None) -> str:
        """Return the path (relative to the project root) of the messages.jsonl to process."""
        if params["replay_from"]:
            if not (ROOT / params["replay_from"]).is_file():
                raise AirflowException(f"replay_from not found: {params['replay_from']}")
            return params["replay_from"]
        free_gib = shutil.disk_usage(ROOT).free / 2**30
        if free_gib < MIN_FREE_GIB:  # plan section 12.2: pause ingestion before the disk fills
            raise AirflowException(f"Only {free_gib:.1f} GiB free (limit {MIN_FREE_GIB}); not starting a capture")
        directory = f"data/raw/{session_name()}"
        if (ROOT / directory / "summary.json").is_file():  # retry after a completed capture
            return f"{directory}/messages.jsonl"
        if (ROOT / directory).exists():
            raise AirflowException(f"{directory} exists without a summary; inspect it, do not overwrite")
        # Reconnect limits are cumulative per capture and the feed drops every few minutes, so allow many.
        cli("capture", "--duration", str(params["duration_seconds"]), "--url", params["url"], "--output", directory,
            "--max-reconnects", "300", "--max-messages", "200000", "--max-bytes", str(400 * 2**20))
        return f"{directory}/messages.jsonl"

    @task
    def process(capture_file: str) -> str:
        output = f"data/processed/{session_name()}"
        if not (ROOT / output / "summary.json").is_file():
            cli("process", capture_file, "--output", output)
        return output

    @task
    def detect(processed: str) -> str:
        output = f"data/screened/{session_name()}"
        if not (ROOT / output / "summary.json").is_file():
            cli("detect", f"{processed}/features.jsonl", "--output", output)
        return output

    @task
    def load(processed: str, screened: str) -> str:
        cli("load-results", screened, "--run-id", Path(screened).name)
        cli("load-events", processed)
        return processed

    @task
    def compact_and_log(processed: str, screened: str, capture_file: str):
        """Gzip the bulky events file (raw captures stay as the source of truth) and append a log row."""
        events = ROOT / processed / "events.jsonl"
        if events.is_file():
            with events.open("rb") as source, gzip.open(str(events) + ".gz", "wb", compresslevel=6) as target:
                shutil.copyfileobj(source, target)
            with gzip.open(str(events) + ".gz", "rb") as check:  # confirm it reads back before removing the original
                while check.read(1 << 20):
                    pass
            events.unlink()
        processed_summary = json.loads((ROOT / processed / "summary.json").read_text(encoding="utf-8"))
        screened_summary = json.loads((ROOT / screened / "summary.json").read_text(encoding="utf-8"))
        capture_summary_path = (ROOT / capture_file).parent / "summary.json"
        capture_summary = json.loads(capture_summary_path.read_text(encoding="utf-8")) if capture_summary_path.is_file() else {}
        row = {"session": Path(screened).name, "seconds": capture_summary.get("elapsed_seconds", "replay"),
               "messages": processed_summary["lines_read"], "features": processed_summary["features_written"],
               "flagged": screened_summary["flagged_rows"], "connection_errors": capture_summary.get("connection_errors", ""),
               "stop_reason": capture_summary.get("stop_reason", "")}
        log = ROOT / "data" / "capture_log.csv"
        new_file = not log.exists()
        with log.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row), quoting=csv.QUOTE_ALL)
            if new_file:
                writer.writeheader()
            writer.writerow(row)

    captured = capture()
    processed = process(captured)
    screened = detect(processed)
    loaded = load(processed, screened)
    compact_and_log(loaded, screened, captured)


blockpulse_collect()
