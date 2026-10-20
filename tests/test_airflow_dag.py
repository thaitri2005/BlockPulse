"""The Airflow DAG calls the blockpulse CLI; check every command it runs still parses, without installing Airflow.

Airflow isn't a project dependency (it runs in its own image), so the DAG file is loaded with stand-in Airflow
classes that record each task. The real DAG is parsed by Airflow itself in the image (see docs/guides/13-airflow.md).
"""

import importlib.util
import shlex
import sys
import types
from pathlib import Path

from blockpulse.cli import build_parser

DAG_FILE = Path(__file__).resolve().parents[1] / "orchestration" / "airflow" / "dags" / "blockpulse_daily.py"


class _Task:
    def __init__(self, task_id, bash_command, **kwargs):
        self.task_id, self.bash_command, self.kwargs, self.downstream = task_id, bash_command, kwargs, set()
        _Dag.current.tasks[task_id] = self

    def __rshift__(self, other):
        for task in other if isinstance(other, list) else [other]:
            self.downstream.add(task.task_id)
        return other


class _Dag:
    current = None

    def __init__(self, dag_id, **kwargs):
        self.dag_id, self.kwargs, self.tasks = dag_id, kwargs, {}

    def __enter__(self):
        _Dag.current = self
        return self

    def __exit__(self, *args):
        pass


def load_dag(monkeypatch):
    fakes = {
        "airflow": types.ModuleType("airflow"),
        "airflow.sdk": types.SimpleNamespace(DAG=_Dag),
        "airflow.providers.standard.operators.bash": types.SimpleNamespace(BashOperator=_Task),
        "airflow.timetables.interval": types.SimpleNamespace(CronDataIntervalTimetable=lambda cron, timezone: (cron, timezone)),
        "pendulum": types.SimpleNamespace(datetime=lambda *a, **k: a),
    }
    for name, module in fakes.items():
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("blockpulse_daily", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def test_every_dag_task_runs_a_valid_blockpulse_command(monkeypatch):
    dag = load_dag(monkeypatch)
    assert set(dag.tasks) == {"screen", "lake", "prepare_database", "load", "dbt_build"}
    parser = build_parser()
    for task in dag.tasks.values():
        command = (task.bash_command.replace("{{ data_interval_start.strftime('%Y-%m-%d') }}", "2026-10-13")
                   .replace('"$BLOCKPULSE_PIPELINE_ROOT"', "/opt/blockpulse/data"))
        program, *arguments = shlex.split(command)
        assert program == "blockpulse", task.task_id
        args = parser.parse_args(arguments)  # exits (fails the test) on an unknown command or flag
        if args.command == "process-pending":
            assert str(args.date) == "2026-10-13" and str(args.root).replace("\\", "/") == "/opt/blockpulse/data"


def test_dag_order_retries_and_pool(monkeypatch):
    dag = load_dag(monkeypatch)
    tasks = dag.tasks
    assert tasks["screen"].downstream == {"lake"} and tasks["lake"].downstream == {"load", "dbt_build"}
    assert tasks["prepare_database"].downstream == {"load"}
    # Stages of one date must not be compacted before the load stage has read them.
    assert "--no-compact" in tasks["screen"].bash_command and "--no-compact" in tasks["lake"].bash_command
    assert "--no-compact" not in tasks["load"].bash_command
    # Everything that writes the data root shares a one-slot pool; the database step doesn't need it.
    assert {t.task_id for t in tasks.values() if t.kwargs.get("pool") == "blockpulse_data_root"} == {"screen", "lake", "load", "dbt_build"}
    assert dag.kwargs["catchup"] is False and dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["default_args"]["retries"] >= 1 and tasks["screen"].kwargs["retries"] > dag.kwargs["default_args"]["retries"]
