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
| DE3 · Proof artifacts | Done | Hashed lock and isolated venv; CI green; splits from the lake 3.4x faster; batch load test 250x real time; streaming 86 to 152 msg/s and p50 latency 72 s to 0.16 s; three failure drills pass with 0 lost ([09](guides/09-load-test.md), [10](guides/10-failure-drills.md)) |
| DE3.5 · Serving and storage hardening | Done | Bulk loads 30–52x faster; serving holds all 2.65 M transactions (rebuilt in 10.6 min); raw 10.3 to 2.1 GB; worker survives a broker restart; lint in CI |
| DE4–DE7 | Staged | See the roadmap |

Tests (2026-10-20): 133 passed and 1 optional skip with the Postgres test database; Terraform 9 of 9 with a mocked provider; Amazon Linux 2023 container rehearsals all pass; CI green.

## 3. Roadmap

Ordered by how much each step builds and demonstrates data engineering skill. Each step ends with tests, a measurement where it applies, docs and a commit.

**Where work runs (decision D21).** The laptop runs only jobs measured in minutes: tests, drills, benchmarks on a day of data or less, loads and rebuilds. Anything that runs for hours (multi-day collection, long soaks, large replays) runs on a short-lived AWS instance with a deadline, as E3a did.

### Done

| Step | What | Evidence |
| --- | --- | --- |
| DE1 | Partitioned Parquet lake with a contract-based quality gate | 8.5 M rows, 0 quarantined ([05](guides/05-lake-and-athena.md)) |
| DE2 | Glue + Athena with partition projection and a scan cap; shared SQL with DuckDB | Queries ran in Athena on the AWS lake ([05](guides/05-lake-and-athena.md)) |
| E3a | Self-expiring AWS collector | 89 h, 2.65 M transactions, about $0.80 ([08](guides/08-aws.md)) |
| DE3 | Pinned environment, CI, load tests, failure drills | 250x real time batch; 86 to 152 msg/s streaming; 3 drills, 0 lost ([09](guides/09-load-test.md), [10](guides/10-failure-drills.md)) |

### DE3.5 · Serving and storage hardening (done)

The audit after DE3 found that the serving layer held 16,770 of 2.65 M rows, loading was row by row, and raw captures were 62% of disk. Fixing these changes how serving and storage work, so it comes before new tooling.

| Item | Change | Acceptance | Status |
| --- | --- | --- | --- |
| a | Bulk Postgres loads: validate in Python, `COPY` into a staging table, merge with set-based SQL in one transaction | Identical table contents to the row-by-row loader (checksums); measured speedup | Done: scores 52x, events 30x faster, contents identical |
| b | Serve all the data: rebuild the serving tables from the lake (`serving-rebuild`: COPY into unindexed tables, build indexes once, atomic swap), because incremental merges slowed to about 60 s a session once the random-keyed index outgrew memory | API and dashboard counts match the files | Done: 2.65 M scores, 5.86 M events, 2.82 M transactions in 10.6 min; all 98 runs reconcile exactly |
| c | Gzip finished raw captures; every reader opens either form | Processing a compressed capture is byte-identical; disk measured before and after | Done: raw 10.3 GB to 2.1 GB (92 sessions); reprocessing is byte-identical |
| d | Enrichment worker rejoins its group after a broker restart instead of stopping | Drill 2 passes with the worker surviving the restart | Done: survived, 0 lost, 120 duplicates absorbed |
| e | Ruff lint in CI; fix the existing findings | CI fails on lint | Done |

### Staged after DE3.5

| Step | Scope | Acceptance | Runs on | Effort |
| --- | --- | --- | --- | --- |
| **DE4 · dbt on the lake** | dbt-duckdb over the lake views: staging models per table; marts `fct_transactions`, `fct_hourly_activity`, `dim_capture_sessions`, `fct_lifecycle_outcomes`; schema and data tests (unique, not null, accepted values, relationships, freshness); generated docs and lineage; `dbt build` in CI on a fixture lake. Optional profile for dbt-athena | `dbt build` green locally and in CI; marts reproduce the four shared queries' results; docs site generated | Laptop | 1–2 sessions |
| **DE5 · One pipeline: streaming into the lake, with metrics** | A Kafka consumer that writes the enriched topic into the same lake tables (hourly files, same contract and quality gate) instead of one Parquet file per message; Prometheus metrics (consumer lag, throughput, error and quarantine rates, data freshness) scraped by Prometheus in the `m2` profile and charted in Grafana; alert rules for lag and freshness | Lake built from the stream equals the lake built from files for the same sessions (row-level check); dashboards show lag and freshness live during a replay | Laptop for build and short replays | 2–3 sessions |
| **DE6 · Airflow orchestration** | A DAG per date partition: process, screen, lake, load, dbt build; retries and SLAs; a demonstrated backfill of past dates; runs in the existing Compose stack | A backfill of 3 past days produces the same lake and marts as the direct run; a deliberately failed task retries and recovers | Laptop | 1–2 sessions |
| **DE7 · Kubernetes** | k3d cluster; a Helm chart for Kafka (Strimzi or Redpanda), Postgres (StatefulSet with a volume), API, Grafana; probes, requests and limits; the drills rerun as pod kills | Chart installs from scratch; pod-kill drills pass with the same invariants as [10](guides/10-failure-drills.md) | Laptop (short); optional k3s-on-EC2 demo on AWS, then destroy | 2–3 sessions |

### Optional, not scheduled

| Item | Why | Runs on |
| --- | --- | --- |
| Hand-labelled sample (about 200 flagged rows, using the review rubric) | The only route to any accuracy figure | Laptop |
| Fee feature relative to the hour's median | The model's measured blind spot (new `FEATURE_SET_VERSION`) | Laptop |
| A second, longer AWS collection (for example a week) to retrain and recheck the gate across seeds | Stability of the model comparison | AWS only |
| Live `capture-kafka` smoke test (60 s) | Last open M1 check | Laptop, owner's shell |
| Entity analysis (address grouping over windows) | Original stretch goal; needs coverage and memory evidence | Laptop for design, AWS for long runs |

Deliberately avoided: managed Kafka (MSK), EKS and RDS (each costs more per month than the whole project), and anything that must keep running after a demo. Stopping after DE4 still leaves a complete project.

**Next session:**

1. DE4: dbt models, tests and docs on the lake.
2. DE5: streaming into the lake, with Prometheus metrics.

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
| D21 | The laptop runs only minute-scale jobs; hour-scale runs go to a short-lived AWS instance | 2026-10-20: the laptop sleeps and has 16 GB; E3a showed AWS runs are cheap and reliable |
| D22 | Postgres is the serving copy and holds all the data. New sessions merge in bulk (`COPY` into staging, set-based merge); backfills rebuild the tables from the lake and swap them in | 2026-10-20: row-by-row loads were 30-52x slower; incremental merges slow down once the random-keyed index outgrows memory; a rebuild loads everything in 10.6 min |
| D23 | Finished raw captures are gzipped in place; all readers accept either form | 2026-10-20: raw was 62% of disk; gzip is 5.4x smaller, stdlib only, byte-identical on read |

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
| Postgres incremental loads slow as indexes outgrow its 512 MB container | Hourly loads stay small; backfills use `serving-rebuild` |

## 6. Resuming work

1. Read this page and the README.
2. Run the tests: `.\.venv\Scripts\python.exe -m pytest -q` (set `BLOCKPULSE_TEST_DSN` and start Postgres for the database tests; see [04](guides/04-postgres-api-dashboard.md)).
3. Pick the first open item under "Next session".
4. Before stopping: update section 2 and "Next session", and note anything still running (Compose containers).
