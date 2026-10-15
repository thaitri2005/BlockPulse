# Data lake: partitioned Parquet, a quality gate, and SQL with DuckDB or Athena

Steps DE1 and DE2 of the [data engineering roadmap](PROJECT_PLAN.md#data-engineering-roadmap-2026-10-16). Every processed session is written into three analytics tables as partitioned Parquet. The same SQL then runs locally with DuckDB and in AWS with Athena.

## Why this layout

| Choice | Reason |
| --- | --- |
| Parquet, Zstandard-compressed | Columnar: queries read only the columns they use; about 10x smaller than the JSONL it comes from |
| Hive partitions `dt=YYYY-MM-DD/hour=HH` | Engines skip whole folders when a query filters on time (partition pruning) |
| One file per session per partition | A handful of files per hour, not one per message. The earlier M1 archive wrote a file per Kafka message, the classic "small files" problem |
| File named after the session | Rebuilding a session overwrites the same files with identical bytes, so the stage is idempotent and backfills are safe |
| Schema defined once in Python | `src/blockpulse/lake.py` is the source of truth; `blockpulse lake-schema` exports it to `infra/aws/terraform/lake_schema.json` for the Glue tables, and a test fails if they ever differ |
| Raw captures kept as JSONL | The lake is derived and rebuildable; the raw messages remain the source of record |

## Tables

| Table | Grain | Partitioned by | Key columns |
| --- | --- | --- | --- |
| `transactions` | one screened transaction | `dt`, `hour` of `received_at` | `txid`, features, `is_flagged`, `rule_ids` (array) |
| `lifecycle_events` | one lifecycle observation | `dt`, `hour` of `event_time` | `event_id`, `txid`, `event_type`, `replacement_txid` |
| `capture_sessions` | one capture session | `dt` of `started_at` | elapsed time, stop reason, connection errors, recorded gap seconds |

`capture_sessions` makes data coverage queryable: how much time each session lost to disconnects is a column you can aggregate, not a note in a log.

Full column list: `python -m blockpulse lake-schema`.

## The quality gate

Each table has a contract: column types, which columns may be null, and row checks. Examples: `txid` is 64 lowercase hex characters, counts are positive, amounts aren't negative, `round_amount_fraction` is between 0 and 1, and `is_flagged` agrees with the list of rule IDs. Duplicate keys within a session also fail.

A row that breaks the contract is **quarantined, not written and not fixed**. It goes to `lake/_quarantine/<table>/<session>.jsonl` with its reasons. Each session gets `lake/_quality/<session>.json` with rows in, written and quarantined per table, and a count for every failed check. That report is written last, so its presence means the stage finished.

On the five real sessions collected so far, **0 of 57,857 rows were quarantined** (14,038 transactions, 43,814 lifecycle events, 5 sessions). The tests prove that bad rows are caught: a malformed txid, a negative fee, a flag without a signal, a wrong type, a duplicate, and an event without a txid.

## Build it

The lake stage is part of `process-pending`, so the AWS instance, the laptop task and Airflow all build it automatically. To backfill sessions that were processed before the stage existed:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[analytics]"   # pyarrow + duckdb
.\.venv\Scripts\python.exe -m blockpulse process-pending       # builds the lake for every session missing it
```

## Query it locally (DuckDB, free)

```powershell
.\.venv\Scripts\python.exe -m blockpulse lake-query hourly_flag_rate
.\.venv\Scripts\python.exe -m blockpulse lake-query path\to\your.sql
```

DuckDB reads the Parquet files in place. `lake-query` registers one view per table, types the partition keys like the Glue tables (`dt` string, `hour` int) and works in UTC, so a query written here runs unchanged in Athena. The shared queries live in [`analytics/queries/`](../analytics/queries/): `hourly_flag_rate`, `signals_by_rule`, `lifecycle_outcomes`, `capture_coverage`.

Results on the real lake (5 sessions, 2026-10-12 to 2026-10-15):

| Query | Result |
| --- | --- |
| `hourly_flag_rate` | 7 hourly partitions; flag rate between 0.54% and 4.4% per hour |
| `signals_by_rule` | `many_inputs` 164, `many_outputs` 140, `repeated_output_values` 34 |
| `lifecycle_outcomes` | latest state: mined 23,700; still added 10,608; replaced 2,362; removed 319 |
| `capture_coverage` | recorded gaps from 0.39% to 13.3% of capture time; the worst session stopped after 4 disconnects in 7 minutes |

Flags are structural rule matches, not evidence of wrongdoing.

## Query it in AWS (Athena)

`terraform apply` (see [AWS_DEPLOY.md](AWS_DEPLOY.md)) also creates:

- a Glue database (`blockpulse_lake`) with one table per lake table. **Partition projection** derives partition locations from the `dt`/`hour` pattern, so new hours are queryable as soon as the collector uploads them: no crawler, no `MSCK REPAIR TABLE`.
- an Athena workgroup (`blockpulse-lake`) that enforces its settings, **cancels any query scanning more than 1 GiB**, and writes encrypted results to `athena-results/`, which expire after 7 days.
- the four shared queries as saved queries.

Open the Athena console, choose the `blockpulse-lake` workgroup and the `blockpulse_lake` database, and run a saved query. Or from the command line: `terraform output -raw athena_example_command`. Filter on `dt` (and `hour`) to scan less: Athena charges about $5 per TB scanned, so these queries cost fractions of a cent.

## Limits

- The lake is rebuilt per session, not per row; editing one row means rebuilding its session (which is cheap and idempotent).
- Partitions follow UTC receive time, so a session that crosses an hour boundary writes two files.
- The Glue tables, workgroup and saved queries are tested with a mocked provider and validated by Terraform, but have not yet run against real Athena.
- `lifecycle_events` stores no transaction payloads; the raw JSONL keeps them.
