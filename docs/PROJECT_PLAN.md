# BlockPulse: plan and status

Last updated 2026-10-20. This is the working plan: purpose, what's done, what's next, the decisions behind it, and the known limits. The original long-form design (2026-10-08) is kept in [archive/original-design.md](archive/original-design.md); where it disagrees with what was built, [ARCHITECTURE.md](ARCHITECTURE.md) and the guides are current.

## 1. Purpose

BlockPulse is a Bitcoin mempool data pipeline built to develop **data engineering** skill on a real live source (decision D18): streaming capture, a partitioned Parquet lake with data-quality checks, SQL analytics locally and in AWS, reproducible evaluation, infrastructure as code, and measured operational behaviour. Anomaly screening is the use case that gives the data a reason to flow.

What the project will not claim:

- A flag means unusual structure, not fraud, illegality or intent.
- A model score is not a probability. There are no independent labels, so no accuracy, precision or recall is claimed.
- One public provider's mempool view is not the whole network, and reconnect gaps are recorded as a lower bound.
- A single-broker, single-node setup is not highly available.
- Public blockchain data can be linked to people: results describe patterns, never address owners.

## 2. Status

| Area | Status | Evidence |
| --- | --- | --- |
| M0 · Source validation | Done | Byte-identical replay; [connectivity](experiments/2026-10-08-source-connectivity.md), [validation](experiments/2026-10-09-phase0-phase1-validation.md) |
| M1 · Kafka ingest, enrich, archive, replay | Done (local) | 402-message replay reproduced 2,732 features exactly ([03](guides/03-kafka.md)). Open: a live `capture-kafka` smoke test |
| M2 · Rules, Postgres, API, dashboard | Done (local) | Duplicate-safe loads, lifecycle projection, read-only API, Grafana ([02](guides/02-structural-rules.md), [04](guides/04-postgres-api-dashboard.md)) |
| M3 · ML evaluation | Done | Rules stay champion; Isolation Forest fails 2 of 4 gate criteria on 2.65 M rows ([06](guides/06-ml-evaluation.md)) |
| DE1–DE2 · Lake and Athena | Done | Partitioned Parquet, quality gate (0 of 8.5 M rows quarantined across 94 sessions), same SQL in DuckDB and Athena ([05](guides/05-lake-and-athena.md)) |
| E3a · AWS collector | Done | 89 h, 0 failures, 2.65 M transactions, about $0.80, torn down ([08](guides/08-aws.md)) |
| DE3 · Proof artifacts | **In progress** | Done: model splits read from the lake (3.4x faster, identical rows), declared row order, `verify-dataset`, reproduction for growing data. CI green on GitHub (2026-10-20; the first run caught an undeclared `pytz` dependency). Clean pinned environment (2026-10-20): hashed `requirements.lock` for every dependency, isolated venv, CI installs from the lock and fails if it's stale, the AWS bundle ships the same pins; both recorded runs still reproduce 6 of 6. Load test (2026-10-20): a day of traffic processes in 5.8 min (about 250x real time); the streaming test found per-message commits as the bottleneck, and batched checkpoints raised enrichment from 86 to 152 msg/s and cut p50 latency from 72 s to 0.16 s ([09](guides/09-load-test.md)). Open: failure drills |
| DE4–DE6 | Planned | See the roadmap |

Tests (2026-10-20): 127 passed and 1 optional skip with the Postgres test database; Terraform 9 of 9 with a mocked provider; Amazon Linux 2023 container rehearsals all pass.

## 3. Roadmap

Ordered by how much each step builds and demonstrates data engineering skill. Each step ends with tests, docs and a commit.

| Step | What | Skills | Status |
| --- | --- | --- | --- |
| DE1 | Partitioned Parquet lake with a contract-based quality gate | Columnar storage, partitioning, data contracts, idempotent rebuilds | Done |
| DE2 | Glue + Athena with partition projection and a scan cap; shared SQL with DuckDB | Lake querying, catalogs, cost-aware SQL | Done |
| E3a | Self-expiring AWS collector, 4 days | IaC, cloud operations, teardown | Done |
| DE3 | CI green; pinned environment; replay load test with measured throughput and latency; failure drills (kill a consumer, Postgres, the broker) showing no loss and no duplicates | Benchmarking, reliability, at-least-once correctness | In progress |
| DE4 | dbt on the lake (dbt-duckdb, optionally dbt-athena): staging and marts (`fct_transactions`, `fct_hourly_activity`, `dim_capture_sessions`), tests, docs and lineage | SQL modelling, data tests | Next |
| DE5 | Airflow DAG over the batch tier by date partition, with a demonstrated backfill | Orchestration, backfills, retries | Planned |
| DE6 | Kubernetes: k3d and a Helm chart (Kafka, Postgres StatefulSet, API, Grafana) with probes and limits; pod-kill drills | Container orchestration, stateful workloads | Planned, last |

Optional, not scheduled: entity analysis (address grouping over time windows), a fee feature relative to the hour's median, and a small hand-labelled sample.

Deliberately avoided: managed Kafka (MSK), EKS and RDS (each costs more per month than the whole project), and anything that must keep running after a demo. Stopping after DE4 still leaves a complete project.

**Next session:**

1. DE3: failure drills (kill the enrichment worker, Postgres, and the broker mid-run; show no loss and no duplicate rows).
2. DE4: dbt.

## 4. Decisions

| ID | Decision | Why / revisit when |
| --- | --- | --- |
| D01 | Transaction-level screening is the core | Well-defined inputs; entity histories need more evidence and state |
| D02 | Kafka is a learning choice | The volume doesn't need it; keep while its lessons justify the overhead |
| D03 | Public source first, replay always available | Cheap; revisit if coverage becomes unusable |
| D04 | One shared feature implementation | No separate training and serving definitions |
| D05 | At-least-once delivery plus idempotent sinks | Exactly-once across Kafka, Postgres and files isn't promised |
| D06 | Rules before ML, both evaluated | A defensible baseline and comparison |
| D07 | No Redis, Flink or custom UI | No proven need |
| D08 | One enrichment worker | Keeps the upstream request budget enforceable |
| D09 | Grafana and a read-only API | Investigation with little frontend work |
| D12 | Cloud runs are temporary | Learn deployment and cost without recurring hosting |
| D13 | Rules stay champion until a model clears the recorded gate | 2026-10-14; revisit with labels or a better model |
| D14 | Local append-only detector registry, not MLflow | Same operations without another service |
| D15 | One collector implementation for every runner | Runners differ only in scheduling, so behaviour and tests are shared |
| D16 | Run a minimal AWS collector before M4 | Days of continuous data; one instance, no NAT, systemd, self-expiring |
| D17 | Infrastructure is tested offline before any deploy, and only the owner applies or destroys | Mocked-provider Terraform tests and container rehearsals in CI |
| D18 | The purpose is data engineering skill; work follows the roadmap | 2026-10-16 |
| D19 | One schema and one SQL for local and cloud | The Python lake contract generates the Glue columns; queries are tested in DuckDB first |
| D20 | Manifests declare row order; old manifests keep file order | 2026-10-19: reading from the lake must not silently change recorded results |

## 5. Risks and limits

| Risk or limit | Handling |
| --- | --- |
| No labels | Conclusions stay about behaviour, stability and agreement. A small hand-reviewed sample is the only route to accuracy figures |
| Gate decisions near a boundary move with the training sample | Check across seeds before promoting |
| The public feed drops every few minutes | Long captures allow 300 reconnects; gaps are recorded as a lower bound |
| Disk: about 4–5 GB a day of collection | Free-space guard (stops below 15 GiB); compaction; raw stays as source of truth |
| Dependency drift (system numpy changed between two runs before the lock) | Hashed lock, isolated venv, CI lock check; models record library versions |
| A cloud run keeps billing | Deadline poweroff, budget alert, bucket expiry, teardown checklist ([08](guides/08-aws.md)) |
| Too many services slow delivery | Compose profiles; stop what isn't in use |

## 6. Resuming work

1. Read this page and the README.
2. Run the tests: `.\.venv\Scripts\python.exe -m pytest -q` (set `BLOCKPULSE_TEST_DSN` and start Postgres for the database tests; see [04](guides/04-postgres-api-dashboard.md)).
3. Pick the first open item under "Next session".
4. Before stopping: update section 2 and "Next session", and note anything still running (Compose containers).
