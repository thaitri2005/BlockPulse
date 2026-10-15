"""Query the local Parquet lake with DuckDB, using the same table names and SQL as the Athena tables.

`connect()` registers one view per lake table over its Hive-partitioned files, typing the partition keys the way
the Glue tables declare them (dt as string, hour as int). The queries in analytics/queries/ therefore run
unchanged here and in Athena, so they are tested offline before anyone pays for a scan.
"""

from pathlib import Path

from blockpulse.lake import TABLES

QUERIES_DIR = Path(__file__).resolve().parents[2] / "analytics" / "queries"


def connect(root: Path):
    import duckdb
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")  # match Athena, which works in UTC
    for name, table in TABLES.items():
        directory = root / "lake" / name
        if not any(directory.rglob("*.parquet")):
            continue
        pattern = (directory / ("*/*/*.parquet" if table.hourly else "*/*.parquet")).as_posix()
        types = "{'dt': 'VARCHAR', 'hour': 'INTEGER'}" if table.hourly else "{'dt': 'VARCHAR'}"
        connection.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{pattern}', "
                           f"hive_partitioning = true, hive_types = {types}, union_by_name = true)")
    return connection


def resolve_query(name_or_path: str) -> Path:
    path = Path(name_or_path)
    if path.is_file():
        return path
    named = QUERIES_DIR / f"{name_or_path.removesuffix('.sql')}.sql"
    if named.is_file():
        return named
    known = ", ".join(sorted(p.stem for p in QUERIES_DIR.glob("*.sql")))
    raise ValueError(f"No query file or named query {name_or_path!r}. Named queries: {known}")


def run_query(root: Path, name_or_path: str) -> dict:
    sql = resolve_query(name_or_path).read_text(encoding="utf-8")
    connection = connect(root)
    try:
        cursor = connection.execute(sql)
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
    finally:
        connection.close()
    return {"columns": columns, "rows": [[v.isoformat() if hasattr(v, "isoformat") else v for v in row] for row in rows]}
