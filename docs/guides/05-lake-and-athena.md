# 05 · Lake and Athena: partitioned Parquet, a quality gate, and SQL

Steps DE1 and DE2 of the [data engineering roadmap](../PROJECT_PLAN.md#3-roadmap). Every processed session is written into three analytics tables as partitioned Parquet. The same SQL then runs locally with DuckDB and in AWS with Athena.

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

On all 94 real sessions (Oct 9 to Oct 19), **0 of 8,499,067 rows were quarantined** (2,648,997 transactions, 5,849,976 lifecycle events, 94 sessions). The tests prove that bad rows are caught: a malformed txid, a negative fee, a flag without a signal, a wrong type, a duplicate, and an event without a txid.

## Build it

The lake stage is part of `process-pending`, so the AWS instance and the laptop task both build it automatically. To backfill sessions that were processed before the stage existed:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[analytics]"   # pyarrow + duckdb
.\.venv\Scripts\python.exe -m blockpulse process-pending       # builds the lake for every session missing it
```

## Query it locally (DuckDB, free)

```powershell
.\.venv\Scripts\python.exe -m blockpulse lake-query hourly_flag_rate
.\.venv\Scripts\python.exe -m blockpulse lake-query path\to\your.sql
```

DuckDB reads the Parquet files in place. `lake-query` registers one view per table, types the partition keys like the Glue tables (`dt` string, `hour` int) and works in UTC, so a query written here runs unchanged in Athena. The shared queries live in [`analytics/queries/`](../../analytics/queries): `hourly_flag_rate`, `signals_by_rule`, `lifecycle_outcomes`, `capture_coverage`.

Results on the real lake (94 sessions, 2026-10-09 to 2026-10-19, of which 89 are hourly AWS sessions):

| Query | Result |
| --- | --- |
| `hourly_flag_rate` | 97 hourly partitions; 96 to 71,099 transactions an hour; flag rate 0.54% to 5.72% per hour |
| `signals_by_rule` | `many_outputs` 21,451; `many_inputs` 17,926; `repeated_output_values` 3,590 |
| `lifecycle_outcomes` | latest state: mined 2,361,132; replaced 284,639; removed 135,869; still unconfirmed 31,470 |
| `capture_coverage` | AWS sessions lost at most 0.08% of capture time to recorded gaps (median 0.01%); laptop sessions lost up to 13.3% |

Flags are structural rule matches, not evidence of wrongdoing.

## Query it in AWS (Athena)

`terraform apply` (see [08 · AWS](08-aws.md)) also creates:

- a Glue database (`blockpulse_lake`) with one table per lake table. **Partition projection** derives partition locations from the `dt`/`hour` pattern, so new hours are queryable as soon as the collector uploads them: no crawler, no `MSCK REPAIR TABLE`.
- an Athena workgroup (`blockpulse-lake`) that enforces its settings, **cancels any query scanning more than 1 GiB**, and writes encrypted results to `athena-results/`, which expire after 7 days.
- the four shared queries as saved queries.

Open the Athena console, choose the `blockpulse-lake` workgroup and the `blockpulse_lake` database, and run a saved query. Or from the command line: `terraform output -raw athena_example_command`. Filter on `dt` (and `hour`) to scan less: Athena charges about $5 per TB scanned, so these queries cost fractions of a cent.

## The lake feeds the models too

`load_split` (training, evaluation, sweeps, packaging) reads each session from its Parquet partitions when the session's quality report matches the manifest, reading only the columns it needs and only `txid` for sessions outside the requested split. On the 527k-row test split that is 31 s against 105 s from JSONL, with identical rows. Pass `source="jsonl"` for a strict checksum-verified read. Details and the row-order rule: [rerun report addendum](../experiments/2026-10-19-m3-rerun-aws-data.md#addendum-reading-splits-from-the-lake-de3-same-day).

## Limits

- The lake is rebuilt per session, not per row; editing one row means rebuilding its session (which is cheap and idempotent).
- Partitions follow UTC receive time, so a session that crosses an hour boundary writes two files.
- The Glue tables, workgroup and saved queries are tested with a mocked provider and validated by Terraform, but have not yet run against real Athena.
- `lifecycle_events` stores no transaction payloads; the raw JSONL keeps them.
