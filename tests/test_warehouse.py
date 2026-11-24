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


def test_hourly_mart_can_be_refreshed_for_named_days_only(warehouse):
    """With `dts`, the hourly mart replaces just those days; without it, it is rebuilt in full as before."""
    import json

    import duckdb
    database = str(warehouse / "dbt" / "blockpulse.duckdb")

    def table():
        with duckdb.connect(database, read_only=True) as connection:
            return connection.execute("SELECT * FROM main.fct_hourly_activity ORDER BY dt, hour").fetchall()

    def spoil():
        with duckdb.connect(database) as connection:
            connection.execute("UPDATE main.fct_hourly_activity SET transactions = -1")

    def refresh(days):
        return run_dbt(warehouse, ["run", "--select", "fct_hourly_activity", "--vars", json.dumps({"dts": days})])

    correct = table()
    days = sorted({row[0] for row in correct})
    assert correct and days

    spoil()
    assert refresh(["1999-01-01"])           # a day that isn't in the lake: nothing may be touched
    assert {row[2] for row in table()} == {-1}
    assert refresh(days)                     # the real days: recomputed and replaced
    assert table() == correct
    assert refresh(days) and table() == correct  # repeating it changes nothing (delete+insert, not append)

    spoil()
    assert run_dbt(warehouse, ["build"])     # the normal full build still rebuilds everything and passes its tests
    assert table() == correct
