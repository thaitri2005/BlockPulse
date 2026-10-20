"""Run the dbt project in analytics/dbt against a lake, with absolute paths.

DuckDB resolves a view's file path when the view is queried, so the lake path baked into the staging views must
be absolute; otherwise the warehouse only works from dbt's own folder. This wrapper sets every path from the data
root and runs dbt through its Python API.
"""

import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2] / "analytics" / "dbt"


def dbt_environment(root: Path) -> dict:
    root = root.resolve()
    return {
        "BLOCKPULSE_LAKE": (root / "lake").as_posix(),
        "BLOCKPULSE_DBT_DB": (root / "dbt" / "blockpulse.duckdb").as_posix(),
        "DBT_TARGET_PATH": (root / "dbt" / "target").as_posix(),
        "DBT_LOG_PATH": (root / "dbt" / "logs").as_posix(),
        "DBT_PROFILES_DIR": PROJECT_DIR.as_posix(),
        "DBT_PROJECT_DIR": PROJECT_DIR.as_posix(),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }


def run_dbt(root: Path, args: list[str]) -> bool:
    """Invoke dbt (for example ["build"]) for the lake under root. Returns True on success."""
    from dbt.cli.main import dbtRunner
    environment = dbt_environment(root)
    (root / "dbt").mkdir(parents=True, exist_ok=True)
    previous = {key: os.environ.get(key) for key in environment}
    os.environ.update(environment)
    try:
        result = dbtRunner().invoke([*args, "--project-dir", environment["DBT_PROJECT_DIR"],
                                     "--profiles-dir", environment["DBT_PROFILES_DIR"]])
    finally:
        # dbt-duckdb keeps its database open in this process; release it so others can open the warehouse.
        from dbt.adapters.duckdb.connections import DuckDBConnectionManager
        DuckDBConnectionManager.close_all_connections()
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    return bool(result.success)
