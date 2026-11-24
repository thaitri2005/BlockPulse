# Exercise 20: one SQL, two engines

The goal is to understand how the same query file can run on your laptop for free and in AWS for a fraction of a cent, and what has to be true for the two to give the same answer. You will run the shared queries against the scratch lake from exercise 19, follow one partition column from a folder name into both engines, and write a query of your own.

A **query engine** is a program that reads data files and answers SQL. **DuckDB** is an engine that runs inside the Python process and reads local files. **Athena** is an AWS service (built on the Trino engine) that reads files in S3 and charges by the amount of data it scans. The lake has no database server: both engines read the same Parquet layout directly.

## Run it

This uses the scratch root from exercise 19 (two generated sessions, labelled synthetic). From PowerShell in the project folder:

```powershell
$py = ".\.venv\Scripts\python.exe"
$root = "$env:TEMP\bp-learn"
& $py -m blockpulse lake-query hourly_flag_rate --root $root
& $py -m blockpulse lake-query signals_by_rule --root $root
& $py -m blockpulse lake-query lifecycle_outcomes --root $root
```

Each name refers to a file in [`analytics/queries/`](../../analytics/queries). Open `hourly_flag_rate.sql` next to its output.

## Open the output

```json
{"columns": ["dt", "hour", "transactions", "flagged", "flag_rate_pct"],
 "rows": [["2026-01-01", 0, 16000, 2505, 15.66]]}
```

(shown compacted). One row, because the generator starts both sessions at the same time. `signals_by_rule` returned `many_outputs` 1,579 and `many_inputs` 926, which add up to the 2,505 flagged rows. `lifecycle_outcomes` returned a single state, `added`, for all 16,000, because generated captures contain no removed, mined or replaced events.

These numbers describe the generator's mix of shapes and nothing else. About one generated transaction in five is deliberately a batch or a consolidation, so the flag rate here is far above the recorded data's. A flag is a structural rule match, never a statement about wrongdoing. The recorded results are in the [lake guide](../guides/05-lake-and-athena.md).

The fourth shared query reads the third table:

```powershell
& $py -m blockpulse lake-query capture_coverage --root $root
```

It returns one row per session: 500.0 elapsed seconds, stop reason `generated`, 2,000 messages saved, 0 connection errors, and a `gap_pct` of 0.0. A generated capture has no disconnects, so its coverage is trivially complete. On recorded data this is the query that shows how much capture time each session lost.

## Read one function

Open [`src/blockpulse/analytics.py`](../../src/blockpulse/analytics.py) and read `connect()`. It creates one **view** (a named query that looks like a table) per lake table:

```python
pattern = (directory / ("*/*/*.parquet" if table.hourly else "*/*.parquet")).as_posix()
types = "{'dt': 'VARCHAR', 'hour': 'INTEGER'}" if table.hourly else "{'dt': 'VARCHAR'}"
connection.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{pattern}', "
                   f"hive_partitioning = true, hive_types = {types}, union_by_name = true)")
```

Trace the column `hour`. It is not stored inside any Parquet file. It exists only as the folder name `hour=00`. `hive_partitioning = true` tells DuckDB to turn folder names into columns, and `hive_types` says `hour` is an integer and `dt` is text.

Now find the same column on the AWS side, in [`infra/aws/terraform/analytics.tf`](../../infra/aws/terraform/analytics.tf):

```hcl
"projection.hour.type"   = "integer"
"projection.hour.range"  = "0,23"
"projection.hour.digits" = "2"
```

and a `partition_keys` block declaring `hour` as `int` and `dt` as `string`. **Partition projection** means Athena computes which folders exist from a pattern (`dt=${dt}/hour=${hour}/`, dates from a start date to now, hours 00 to 23) instead of keeping a list that something must update. A new hour is queryable as soon as its file is uploaded.

So the same fact is stated twice, once per engine: `hour` is an integer taken from a two-digit folder name. If the two statements disagree, `WHERE hour = 0` means different things in the two places. A test, `test_views_type_partitions_like_the_glue_tables_and_use_utc`, pins the DuckDB side. The other columns cannot drift: they are generated from `lake.py` into `lake_schema.json`, which Terraform reads.

`connect()` also runs `SET TimeZone = 'UTC'`, because Athena works in UTC and a query that groups by hour would otherwise shift on a laptop in another time zone.

**Scan cost** is the reason partitions matter in Athena: you pay for bytes read. A filter on `dt` and `hour` lets the engine skip whole folders, and the workgroup in the same Terraform file cancels any query that would scan more than 1 GiB.

## Change one small thing

Write your own query. Save this as `$env:TEMP\bp-learn\one_hour.sql`:

```sql
SELECT session, COUNT(*) AS transactions, SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS flagged
FROM transactions
WHERE dt = '2026-01-01' AND hour = 0
GROUP BY session
ORDER BY session
```

```powershell
& $py -m blockpulse lake-query "$env:TEMP\bp-learn\one_hour.sql" --root $root
```

It returns one row per session: 8,000 transactions and 1,268 flagged for the first, 8,000 and 1,237 for the second. The query uses only standard SQL (`CASE` instead of DuckDB's `count(*) FILTER`), which is the rule for anything in `analytics/queries/`.

Then see what the type declaration buys. In `connect()`, temporarily remove `hive_types = {types}, ` from the `read_parquet(...)` call, so DuckDB guesses. It will type `hour` as text (`'00'`) and `dt` as a date. Run the tests:

```powershell
& $py -m pytest -q tests/test_analytics.py
```

Read the failure, restore the line, and run them again.

## Question to carry forward

Partitions follow `received_at` in UTC, and `capture_sessions` is partitioned by day only. A session that starts at 23:50 and ends at 00:40 writes transaction files into two `dt` folders but one session row. If you wanted "transactions per session-hour of coverage" for one day, which filters would prune correctly, and where could you silently lose the session row?

## What was verified

Run on 2026-11-24 on Linux against a fresh scratch lake, after `synth.py` began writing a full capture summary: all four `lake-query` commands (all succeeded), the custom `one_hour.sql`, and `pytest tests/test_analytics.py` (passed, unmodified). The guessed types without `hive_types` (`hour` VARCHAR, `dt` DATE) were observed by calling DuckDB's `read_parquet` directly on the scratch lake, not by editing `analytics.py`, so the exact test failure was not seen. **Not run:** anything in Athena or AWS. The Glue settings were read from the Terraform file; the AWS guide records that the saved queries ran against the real upload in October.
