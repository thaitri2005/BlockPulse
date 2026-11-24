# Milestone history

The detailed record of how each milestone and roadmap step was completed, moved out of the [plan](../PROJECT_PLAN.md) on 2026-11-24 so the plan stays short. Nothing here is current guidance; the guides and the architecture document are. Links are relative to this folder.

## Status table as of 2026-11-24

| Area | Status | Evidence |
| --- | --- | --- |
| M0 · Source validation | Done | Byte-identical replay; [connectivity](../experiments/2026-10-08-source-connectivity.md), [validation](../experiments/2026-10-09-phase0-phase1-validation.md) |
| M1 · Kafka ingest, enrich, archive, replay | Done (local) | 402-message replay reproduced 2,732 features exactly ([03](../guides/03-kafka.md)). Open: a live `capture-kafka` smoke test |
| M2 · Rules, Postgres, API, dashboard | Done (local) | Duplicate-safe loads, lifecycle projection, read-only API, Grafana ([02](../guides/02-structural-rules.md), [04](../guides/04-postgres-api-dashboard.md)) |
| M3 · ML evaluation | Done | Rules stay champion; Isolation Forest fails 2 of 4 gate criteria on 2.65 M rows ([06](../guides/06-ml-evaluation.md)) |
| DE1–DE2 · Lake and Athena | Done | Partitioned Parquet, quality gate (0 of 8.5 M rows quarantined across 94 sessions), same SQL in DuckDB and Athena ([05](../guides/05-lake-and-athena.md)) |
| E3a · AWS collector | Done | 89 h, 0 failures, 2.65 M transactions, about $0.80, torn down ([08](../guides/08-aws.md)) |
| DE3 · Proof artifacts | Done | Hashed lock and isolated venv; CI green; splits from the lake 3.4x faster; batch load test 250x real time; streaming 86 to 152 msg/s and p50 latency 72 s to 0.16 s; three failure drills pass with 0 lost ([09](../guides/09-load-test.md), [10](../guides/10-failure-drills.md)) |
| DE3.5 · Serving and storage hardening | Done | Bulk loads 30–52x faster; serving holds all 2.65 M transactions (rebuilt in 10.6 min); raw 10.3 to 2.1 GB; worker survives a broker restart; lint in CI |
| DE4 · dbt on the lake | Done | 8 models, 28 tests, `dbt build` in 1.5 min on 2.65 M transactions; marts reproduce the shared queries exactly and agree with Postgres on every status ([11](../guides/11-dbt.md)) |
| DE5 · Streaming into the lake, with metrics | Done | `kafka-lake` sink: 13,707 messages to 207,913 transactions and 466,920 events, row-for-row identical to the batch lake; Prometheus metrics, 4 alert rules, Grafana pipeline dashboard ([12](../guides/12-streaming-lake-and-metrics.md)) |
| DE6 · Airflow orchestration | Done | DAG per UTC day (screen, lake, load, dbt build) with retries and a one-slot pool; a 3-day backfill equals the direct run row for row in the lake, the marts and Postgres; a Postgres outage recovered by retry; four real failures found and fixed ([13](../guides/13-airflow.md)) |
| DE7 · Kubernetes | Done (cloud session, synthetic data) | A k3d cluster from one script and a Helm chart for Postgres, Kafka, the API, the worker and Grafana: installs from nothing in 19 s, about 1.4 GiB. Three pod-kill drills pass with the invariants of [10](../guides/10-failure-drills.md): 0 of 30,000 messages lost when the worker or the broker pod is killed, 200,000 rows loaded exactly once when the loader or the Postgres pod is killed. Open: a run on the laptop ([14](../guides/14-kubernetes.md), [log](../experiments/2026-11-24-de7-kubernetes.md)) |

Tests (2026-10-20): 154 passed and 2 skips (one Linux-only) with the Postgres test database; Terraform 9 of 9 with a mocked provider; Amazon Linux 2023 container rehearsals all pass; CI green. Rechecked 2026-11-24 on Linux after a five-week pause: 155 passed, 1 skipped, lint clean, lock unchanged; 179 passed, 1 skipped, of which 11 Helm chart tests skip without `helm`; CI green on `main`.


## Roadmap steps as they were completed

### DE1–DE3

| Step | What | Evidence |
| --- | --- | --- |
| DE1 | Partitioned Parquet lake with a contract-based quality gate | 8.5 M rows, 0 quarantined ([05](../guides/05-lake-and-athena.md)) |
| DE2 | Glue + Athena with partition projection and a scan cap; shared SQL with DuckDB | Queries ran in Athena on the AWS lake ([05](../guides/05-lake-and-athena.md)) |
| E3a | Self-expiring AWS collector | 89 h, 2.65 M transactions, about $0.80 ([08](../guides/08-aws.md)) |
| DE3 | Pinned environment, CI, load tests, failure drills | 250x real time batch; 86 to 152 msg/s streaming; 3 drills, 0 lost ([09](../guides/09-load-test.md), [10](../guides/10-failure-drills.md)) |

### DE3.5 · Serving and storage hardening

The audit after DE3 found that the serving layer held 16,770 of 2.65 M rows, loading was row by row, and raw captures were 62% of disk. Fixing these changes how serving and storage work, so it comes before new tooling.

| Item | Change | Acceptance | Status |
| --- | --- | --- | --- |
| a | Bulk Postgres loads: validate in Python, `COPY` into a staging table, merge with set-based SQL in one transaction | Identical table contents to the row-by-row loader (checksums); measured speedup | Done: scores 52x, events 30x faster, contents identical |
| b | Serve all the data: rebuild the serving tables from the lake (`serving-rebuild`: COPY into unindexed tables, build indexes once, atomic swap), because incremental merges slowed to about 60 s a session once the random-keyed index outgrew memory | API and dashboard counts match the files | Done: 2.65 M scores, 5.86 M events, 2.82 M transactions in 10.6 min; all 98 runs reconcile exactly |
| c | Gzip finished raw captures; every reader opens either form | Processing a compressed capture is byte-identical; disk measured before and after | Done: raw 10.3 GB to 2.1 GB (92 sessions); reprocessing is byte-identical |
| d | Enrichment worker rejoins its group after a broker restart instead of stopping | Drill 2 passes with the worker surviving the restart | Done: survived, 0 lost, 120 duplicates absorbed |
| e | Ruff lint in CI; fix the existing findings | CI fails on lint | Done |

### DE4–DE7

| Step | Scope | Acceptance | Runs on | Effort / status |
| --- | --- | --- | --- | --- |
| **DE4 · dbt on the lake** | dbt-duckdb over the lake: staging, intermediate lifecycle, marts, tests, docs and lineage, `dbt build` in CI | `dbt build` green locally and in CI; marts reproduce the shared queries | Laptop | **Done** 2026-10-20 ([11](../guides/11-dbt.md)) |
| **DE5 · One pipeline: streaming into the lake, with metrics** | A Kafka consumer that writes the enriched topic into the same lake tables (hourly files, same contract and quality gate) instead of one Parquet file per message; Prometheus metrics (consumer lag, throughput, error and quarantine rates, data freshness) scraped by Prometheus in the `m2` profile and charted in Grafana; alert rules for lag and freshness | Lake built from the stream equals the lake built from files for the same sessions (row-level check); dashboards show lag and freshness live during a replay | Laptop for build and short replays | **Done** 2026-10-20 ([12](../guides/12-streaming-lake-and-metrics.md)) |
| **DE6 · Airflow orchestration** | A DAG per date partition: process, screen, lake, load, dbt build; retries; a demonstrated backfill of past dates; runs in the existing Compose stack | A backfill of 3 past days produces the same lake and marts as the direct run; a deliberately failed task retries and recovers | Laptop | **Done** 2026-10-20 ([13](../guides/13-airflow.md)) |
| **DE7 · Kubernetes** | k3d cluster; a Helm chart for Kafka (a plain StatefulSet, D29), Postgres (StatefulSet with a volume), API, worker, Grafana; probes, requests and limits; the drills rerun as pod kills | Chart installs from scratch; pod-kill drills pass with the same invariants as [10](../guides/10-failure-drills.md) | Cloud session (D30); not yet run on the laptop | **Done** 2026-11-24 ([14](../guides/14-kubernetes.md)) |

