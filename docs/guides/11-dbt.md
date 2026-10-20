# 11 · dbt on the lake

A modelling layer on top of the Parquet lake: dbt with DuckDB turns the three lake tables into tested, documented marts. dbt never copies the raw lake; the staging views read the Parquet files in place.

```powershell
.\.venv\Scripts\python.exe -m blockpulse dbt build                # models and every test (about 1.5–2.5 min on 2.65 M transactions)
.\.venv\Scripts\python.exe -m blockpulse dbt docs generate        # docs site and lineage in data/dbt/target/
.\.venv\Scripts\python.exe -m blockpulse dbt docs serve --port 8081
```

`blockpulse dbt …` passes its arguments to dbt and sets every path from the data root (`--root`, default `data`): the lake, the warehouse file (`data/dbt/blockpulse.duckdb`) and dbt's build output. The paths are absolute on purpose, because DuckDB resolves a view's file path when the view is queried. With relative paths the warehouse only worked from dbt's own folder (found while building this).

## Lineage

```text
lake.transactions ------> stg_transactions -----+--> fct_transactions ---> fct_outcomes_by_flag
                                                |          ^
lake.lifecycle_events --> stg_lifecycle_events -+--> int_transaction_lifecycle
                                    |           |
                                    +-----------+--> fct_hourly_activity
lake.capture_sessions --> stg_capture_sessions ----> dim_capture_sessions (also uses stg_transactions)
```

| Layer | Models | Materialized |
| --- | --- | --- |
| Staging | `stg_transactions`, `stg_lifecycle_events`, `stg_capture_sessions`: typed, renamed, partition `hour` cast from text | Views over the Parquet files |
| Intermediate | `int_transaction_lifecycle`: each transaction's latest status from all its evidence, with the **same rule as Postgres** (latest event; at equal times mined > replaced > removed > added) | Table |
| Marts | `fct_transactions` (one row per txid, first observation, features, flags, lifecycle), `fct_hourly_activity`, `dim_capture_sessions` (coverage per session), `fct_outcomes_by_flag` | Tables |

## Tests

28 tests run with every `dbt build`: unique and not-null keys, accepted values (event types, statuses, feature and rule versions), a relationship from transactions to sessions, a uniqueness test on `(dt, hour)`, and four business rules as SQL tests: flag rates between 0 and 100%, `is_flagged` agrees with the signals (the lake's quality-gate rule, checked again downstream), hourly totals reconcile with staging, and outcome shares add up to 100%.

A pytest (`tests/test_warehouse.py`) builds a fixture lake, runs `dbt build` through the wrapper, and checks the marts against the shared SQL. CI runs it.

## Results on the real lake (2026-10-20)

| Check | Result |
| --- | --- |
| `dbt build` | 8 models and 28 tests, all pass, 1 min 26 s |
| `fct_hourly_activity` against `analytics/queries/hourly_flag_rate.sql` | Identical, all 98 hours |
| `int_transaction_lifecycle` against `analytics/queries/lifecycle_outcomes.sql` | Identical |
| dbt (DuckDB) statuses against Postgres statuses, for the 2.81 M transactions whose evidence is in the lake | Identical in every status |
| `fct_transactions` | 2,649,030 rows, one per txid |

`fct_outcomes_by_flag` shows how transactions ended up:

| | Confirmed | Replaced | Removed | Still unconfirmed |
| --- | ---: | ---: | ---: | ---: |
| Flagged by the rules (42,969) | 96.2% | 2.6% | 0.3% | 0.9% |
| Not flagged (2,606,061) | 82.9% | 10.8% | 5.1% | 1.2% |

Flagged transactions (large fan-ins, fan-outs and equal-output shapes) were confirmed more often and replaced less often. This describes outcomes within the observed window; it says nothing about intent, and "replaced" is mostly fee bumping, which ordinary wallets do.

## Freshness

`dbt source freshness` checks the newest `received_at` and `event_time` in the lake and warns after 2 hours. On recorded data it warns, as it should: the last session ended on 2026-10-19. It becomes meaningful with live collection, and DE5 adds the same signal as a Prometheus metric.

## Limits

- Staging views re-read Parquet on every query; marts are tables, so dashboards and ad hoc queries should use the marts.
- dbt-athena is not set up yet; the models use standard SQL plus DuckDB's `qualify`, `filter` and `median`, which would need small changes for Athena.
- `fct_hourly_activity` counts every screened observation (a transaction seen in two sessions counts twice), to match the shared query; `fct_transactions` counts each transaction once.
