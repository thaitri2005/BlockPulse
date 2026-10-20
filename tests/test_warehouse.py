import pytest

pytest.importorskip("dbt.cli.main")
pytest.importorskip("duckdb")

from blockpulse import analytics, collector  # noqa: E402
from blockpulse.warehouse import run_dbt  # noqa: E402
from tests.test_collector import captured_layout  # noqa: E402

SAMPLE_TRANSACTION = {  # same shape as the `transaction` fixture in conftest.py (module scope can't use it)
    "txid": "a" * 64,
    "vin": [{"txid": "b" * 64, "vout": 0, "prevout": {"value": 100603}, "sequence": 4294967293}],
    "vout": [{"value": 60000}, {"value": 40000}],
    "weight": 801,
    "fee": 603,
    "status": {"confirmed": False},
}


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("warehouse")
    layout = captured_layout(tmp, dict(SAMPLE_TRANSACTION), sessions=3)
    assert collector.process_pending(layout)["failed"] == []
    assert run_dbt(layout.root, ["build"]), "dbt build (models and tests) failed"
    return layout.root


def test_dbt_build_passes_and_marts_match_the_shared_queries(warehouse):
    import duckdb
    connection = duckdb.connect(str(warehouse / "dbt" / "blockpulse.duckdb"), read_only=True)
    hourly = connection.execute("SELECT dt, hour, transactions, flagged, flag_rate_pct FROM main.fct_hourly_activity "
                                "ORDER BY dt, hour").fetchall()
    assert [tuple(r) for r in hourly] == [tuple(r) for r in analytics.run_query(warehouse, "hourly_flag_rate")["rows"]]
    latest = dict(connection.execute("SELECT latest_event_type, count(*) FROM main.int_transaction_lifecycle GROUP BY 1").fetchall())
    assert latest == dict((r[0], r[1]) for r in analytics.run_query(warehouse, "lifecycle_outcomes")["rows"])
    transactions = connection.execute("SELECT count(*), count(DISTINCT txid) FROM main.fct_transactions").fetchone()
    assert transactions[0] == transactions[1] > 0  # one row per transaction


def test_views_use_absolute_paths_and_work_from_any_directory(warehouse, monkeypatch, tmp_path):
    import duckdb
    monkeypatch.chdir(tmp_path)  # somewhere unrelated to the dbt project
    connection = duckdb.connect(str(warehouse / "dbt" / "blockpulse.duckdb"), read_only=True)
    assert connection.execute("SELECT count(*) FROM main.stg_transactions").fetchone()[0] > 0
