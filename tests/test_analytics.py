import pytest

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

from blockpulse import analytics, collector  # noqa: E402
from tests.test_collector import captured_layout  # noqa: E402


@pytest.fixture
def lake_root(tmp_path, transaction):
    layout = captured_layout(tmp_path, transaction, sessions=2)
    assert collector.process_pending(layout)["failed"] == []
    return layout.root


def test_views_type_partitions_like_the_glue_tables_and_use_utc(lake_root):
    connection = analytics.connect(lake_root)
    types = dict(connection.execute("SELECT column_name, data_type FROM information_schema.columns "
                                    "WHERE table_name = 'transactions'").fetchall())
    assert types["dt"] == "VARCHAR" and types["hour"] == "INTEGER"
    offset = connection.execute("SELECT strftime(received_at, '%z') FROM transactions LIMIT 1").fetchone()[0]
    assert offset in ("+0000", "+00")
    connection.close()


@pytest.mark.parametrize("name", sorted(p.stem for p in analytics.QUERIES_DIR.glob("*.sql")))
def test_every_shared_query_runs_locally(lake_root, name):
    result = analytics.run_query(lake_root, name)
    assert result["columns"] and result["rows"] is not None


def test_query_results_match_the_data(lake_root):
    hourly = analytics.run_query(lake_root, "hourly_flag_rate")
    assert sum(row[2] for row in hourly["rows"]) == 4  # 2 sessions x 2 transactions
    coverage = analytics.run_query(lake_root, "capture_coverage")
    assert len(coverage["rows"]) == 2
    with pytest.raises(ValueError, match="Named queries"):
        analytics.resolve_query("no_such_query")
