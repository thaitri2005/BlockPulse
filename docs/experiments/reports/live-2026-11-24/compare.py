"""Compare lake folders row for row with DuckDB. Usage: compare.py <lake A> <lake B>. `session` is ignored:
the batch path names a capture by its folder, the stream by its run id."""
import sys

import duckdb

a, b = sys.argv[1], sys.argv[2]
for table in ("transactions", "lifecycle_events"):
    def rel(lake):
        return f"(SELECT * EXCLUDE (session) FROM read_parquet('{lake}/{table}/*/*/*.parquet', hive_partitioning = true))"
    count_a, count_b = (duckdb.sql(f"SELECT count(*) FROM {rel(x)}").fetchone()[0] for x in (a, b))
    only_a = duckdb.sql(f"SELECT count(*) FROM (SELECT * FROM {rel(a)} EXCEPT ALL SELECT * FROM {rel(b)})").fetchone()[0]
    only_b = duckdb.sql(f"SELECT count(*) FROM (SELECT * FROM {rel(b)} EXCEPT ALL SELECT * FROM {rel(a)})").fetchone()[0]
    files = lambda lake: len(duckdb.sql(f"SELECT DISTINCT filename FROM read_parquet('{lake}/{table}/*/*/*.parquet', filename = true)").fetchall())  # noqa: E731
    print(f"{table}: rows {count_a} vs {count_b}; only in first {only_a}; only in second {only_b}; files {files(a)} vs {files(b)}")
