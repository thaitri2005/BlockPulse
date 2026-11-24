# Architecture

What is built, how the pieces connect, and where the code lives. The [README](../README.md) gives the short version and the results; for why, see the [plan](PROJECT_PLAN.md); for how to run each part, see the [guides](guides/). Last reviewed 2026-11-24.

## Data flow

```text
                 public mempool WebSocket (mempool.space regional node)
                                   |
              capture / collect-loop (gaps recorded, 300 reconnects per session)
                                   |
                   data/raw/<session>/messages.jsonl      <- source of truth, never rewritten
                      |                          \
        process (events, features)          capture-kafka -> Kafka raw topic -> enrich -> enriched topic
                      |                                          -> JSONL archive / Parquet archive
                      |                                          -> kafka-lake: features, rules, quality gate
                      |                                             -> data/lake-stream (same tables)
                      |                                             -> /metrics -> Prometheus -> Grafana
        detect (structural rules)
                      |
     data/processed/<session>/, data/screened/<session>/
           |                 |                         |
   lake stage: quality   load-results / load-events   build-dataset -> train / evaluate / sweep
   gate -> partitioned   -> Postgres -> API + Grafana  -> detector registry (champion: rules)
   Parquet (dt/hour)       ^
           |---------------' serving-rebuild (bulk, from the lake)
           |
   DuckDB (lake-query) locally, Athena over S3 in AWS
           |
   dbt (DuckDB): staging -> int_transaction_lifecycle -> marts (fct_transactions, fct_hourly_activity, ...)
```

Unattended runs wrap the same steps: `collect-loop` captures hourly sessions, `process-pending` brings every finished session up to date (process, screen, lake, compact, log, optional Postgres load), `sync-pending` uploads to S3. On AWS these run as systemd services on one instance ([08](guides/08-aws.md)). Airflow runs the same commands as a DAG, one run per UTC day (`process-pending --date`), with retries and backfills ([13](guides/13-airflow.md)):

```text
screen ---> lake ---+--> load (Postgres, then compact) <--- prepare_database
                    +--> dbt_build
```

## Where it runs

The pipeline logic is the `blockpulse` package and its CLI. Four runtimes wrap it; none of them contains pipeline logic.

| Runtime | What runs there | Defined in |
| --- | --- | --- |
| A shell on the laptop | Any command by hand; the hourly collection task | `scripts/` |
| Docker Compose | Kafka (profile `m1`); Postgres, Grafana, Prometheus (`m2`); Airflow (`airflow`). Workers and the API run on the host and connect to these | `compose.yaml` |
| AWS | One EC2 instance running `collect-loop`, `process-pending` and `sync-pending` under systemd, with S3, Glue and Athena. Temporary: it powers off at a deadline | `infra/aws/` |
| Kubernetes (k3d) | The serving stack as pods in one namespace, from a Helm chart | `infra/k8s/` |

The Kubernetes layout, which mirrors the Compose services:

```text
namespace blockpulse (Helm release "bp")

  StatefulSet bp-postgres --- PVC data-bp-postgres-0     Secret bp-postgres (user, password, database)
  StatefulSet bp-kafka ------ PVC data-bp-kafka-0        one KRaft broker, reached as bp-kafka-0.bp-kafka:9092

  Job bp-db-init       blockpulse db-init      (migrations)   \  Helm hooks: run after every install and
  Job bp-kafka-init    blockpulse kafka-init   (topics)       /  upgrade; both apply only what is missing

  Deployment bp-api (2 pods) --- Service bp-api:8000     blockpulse serve; reads Secret bp-app (BLOCKPULSE_DSN)
  Deployment bp-worker (0 or 1)                          blockpulse kafka-enrich; off by default
  Deployment bp-grafana -------- Service bp-grafana:3000 datasource and dashboard from ConfigMaps
```

One image, `blockpulse:dev`, runs the API, the worker and both Jobs with different arguments. State lives only in the two volumes and in Kafka's committed offsets, so any pod can be deleted and its controller recreates it. Prometheus, Airflow and the stream sink are not in the chart; they run under Compose.

## Design rules

The principles are stated in the [README](../README.md#design-principles). What they mean in the code:

- **Raw is the source of truth.** Finished captures are gzipped (`messages.jsonl.gz`, about 5x smaller); every reader opens either form through `storage.open_capture`.
- **The lake is the analytics store; Postgres is the serving copy.** `serving-rebuild` recreates the Postgres tables from the lake and swaps them in.
- **Every stage is idempotent.** Output folders are never overwritten; `process-pending` skips stages whose summary exists; loads rely on unique keys; consumers commit offsets after writing.
- **One implementation per concern.** `features.py`, `detector.py`, the contracts in `lake.py` (which also generate the Glue schema) and `collector.py` are each the only definition of their thing.
- **Runtimes mirror each other.** The Helm chart's defaults copy `compose.yaml`, so results can be compared.
- **Generated data is labelled.** `synth-capture` output carries `source_mode: synthetic`.

## Commands

`python -m blockpulse --help` lists them all.

| Group | Commands | Guide |
| --- | --- | --- |
| Capture and process | `capture`, `process`, `detect`, `lab`, `trace` | [01](guides/01-capture-and-process.md), [02](guides/02-structural-rules.md) |
| Kafka | `kafka-init`, `capture-kafka`, `kafka-replay`, `kafka-archive`, `kafka-enrich`, `kafka-parquet`, `kafka-lake` | [03](guides/03-kafka.md), [12](guides/12-streaming-lake-and-metrics.md) |
| Postgres and API | `db-init`, `load-results`, `load-events`, `serving-rebuild`, `serve` | [04](guides/04-postgres-api-dashboard.md) |
| Lake and SQL | `lake-query`, `lake-schema`, `lake-compact` (the lake itself is built by `process-pending` or `kafka-lake`) | [05](guides/05-lake-and-athena.md) |
| Modelling | `dbt build`, `dbt docs generate`, any dbt command (`blockpulse dbt ...`) | [11](guides/11-dbt.md) |
| ML evaluation | `build-dataset`, `verify-dataset`, `train-iforest`, `evaluate-iforest`, `sweep`, `detectors ...` | [06](guides/06-ml-evaluation.md) |
| Unattended collection | `collect`, `collect-loop`, `process-pending` (`--date` for one day), `sync-pending`, `compact-raw` | [07](guides/07-collection.md), [08](guides/08-aws.md) |
| Orchestration | Airflow DAG `blockpulse_daily` in the `airflow` Compose profile; `airflow backfill create` | [13](guides/13-airflow.md) |
| Kubernetes | `infra/k8s/cluster-up.sh`, `infra/k8s/image-load.sh`, `helm upgrade --install bp infra/k8s/chart/blockpulse` | [14](guides/14-kubernetes.md) |
| Load test and drills | `benchmark batch`, `benchmark streaming`, `drill enrich-crash`, `drill broker-restart`, `drill postgres-crash`; on Kubernetes `drill k8s-enrich-kill`, `drill k8s-broker-kill`, `drill k8s-postgres-kill`; `synth-capture` for generated traffic | [09](guides/09-load-test.md), [10](guides/10-failure-drills.md) |

## Code

Python package `src/blockpulse/` (about 6,200 lines, flat on purpose):

| Module | Lines | Role |
| --- | ---: | --- |
| `capture.py` | 198 | Bounded WebSocket capture, reconnects, gap records |
| `events.py` | 118 | One saved message to normalized lifecycle events |
| `features.py` | 66 | The shared feature function (`transaction-v2`) |
| `process.py` | 131 | Capture JSONL to events, features and an error report |
| `detector.py`, `detect.py`, `lab.py` | 330 | The three structural rules, offline screening, synthetic rule cases |
| `storage.py` | 56 | Never-overwrite run folders, atomic summaries |
| `kafka_pipeline.py`, `kafka_workers.py` | 571 | Topics, publisher, replay, archive consumer, enrichment consumer (batched checkpoints) |
| `stream_lake.py` | 216 | Enriched topic to lake tables in offset-named micro-batches, through the quality gate (`kafka-lake`) |
| `lake_compact.py` | 80 | Merge the streamed lake's small files, one per closed hour (`lake-compact`) |
| `metrics.py` | 71 | Prometheus metrics for the stream sink: lag, freshness, throughput, quarantines |
| `enrichment.py`, `parquet_archive.py` | 370 | Rate-limited REST enrichment; Kafka-to-Parquet archive |
| `db.py`, `results.py`, `lifecycle.py` + `migrations/` | 303 | Postgres migrations; bulk (`COPY` + set-based merge) duplicate-safe loads; lifecycle status projection |
| `warehouse.py` | 46 | Runs the dbt project with absolute paths (`dbt`) |
| `serving.py` | 196 | Rebuild the serving tables from the batch or the streamed lake and swap them in atomically (`serving-rebuild`) |
| `api.py`, `trace.py` | 238 | Read-only FastAPI; one transaction traced back to its raw message |
| `lake.py`, `analytics.py` | 356 | Lake contracts, quality gate, partitioned writer, Glue schema export; DuckDB views |
| `dataset.py` | 320 | Checksummed chronological manifests, lake- or JSONL-backed split loading |
| `model.py`, `evaluate.py`, `sweep.py`, `registry.py` | 719 | Isolation Forest, threshold calibration, synthetic sweep, detector registry and gate |
| `collector.py` | 359 | Capture loop, idempotent session processing (all pending, or one start date), lock, S3 upload |
| `benchmark.py` | 198 | Batch and streaming load tests on recorded sessions (`benchmark`) |
| `drills.py` | 201 | Failure drills: worker crash, broker restart, Postgres crash (`drill`) |
| `k8s_drills.py` | 316 | The same drills as pod kills in a k3d cluster (`drill k8s-*`) |
| `synth.py` | 66 | Deterministic synthetic capture sessions for drills (`synth-capture`) |
| `cli.py` | 614 | Command-line entry point |

Elsewhere:

| Path | What it is |
| --- | --- |
| `tests/` | pytest (180 tests); Postgres tests opt in with `BLOCKPULSE_TEST_DSN`; Helm chart tests need `helm`; the Kubernetes drill logic is tested with `kubectl` faked |
| `analytics/queries/` | Shared SQL, run by DuckDB locally and saved as Athena named queries |
| `analytics/dbt/` | dbt project: staging, intermediate and mart models with tests, over the lake |
| `orchestration/airflow/` | Airflow image (BlockPulse in its own venv) and the `blockpulse_daily` DAG |
| `infra/aws/terraform/` | The AWS collector stack (VPC, instance, S3, IAM, Glue, Athena, budget) and its mocked-provider tests |
| `infra/aws/instance/` | `install.sh`, `deadline-check.sh` and the hardened systemd units that run on the instance |
| `infra/aws/tests/` | Amazon Linux 2023 container rehearsals: bootstrap smoke test and real systemd end to end |
| `infra/aws/build_bundle.py` | Reproducible app zip for the instance |
| `observability/grafana/` | Provisioned datasources (Postgres, Prometheus) and dashboards (data, pipeline health) |
| `observability/prometheus/` | Scrape config and alert rules |
| `compose.yaml` | Profiles `m1` (Kafka), `m2` (Postgres, Grafana, Prometheus) and `airflow` |
| `infra/k8s/` | k3d cluster definition and creation script; the BlockPulse image (`Dockerfile`, `image-load.sh`); Helm chart `chart/blockpulse/` with Postgres and Kafka StatefulSets, setup Jobs, API, worker and Grafana ([14](guides/14-kubernetes.md)) |
| `scripts/` | Laptop collection task, M2 demo, M3 reproduction |
| `examples/` | Learning exercise 1 and synthetic fixtures |
| `.github/workflows/ci.yml` | CI: lock check, lint, pytest with Postgres, Terraform checks, shellcheck, container rehearsals. It does not create a Kubernetes cluster |

## Technology

| Concern | Choice | Notes |
| --- | --- | --- |
| Language | Python 3.11+ | websockets, confluent-kafka, pyarrow, duckdb, psycopg, FastAPI, scikit-learn |
| Streaming | Apache Kafka 4.3 (KRaft, one broker) | Topics `blockpulse.raw.v1`, `.enriched.v1`, `.errors.v1`; 3 partitions; keyed by capture session |
| Raw and replay | JSONL | Inspectable; byte-identical replay |
| Lake | Parquet + Zstandard, Hive partitions `dt`/`hour` | One file per session per partition |
| SQL | DuckDB locally, Athena in AWS | Same queries; Glue partition projection; 1 GiB scan cap |
| Modelling | dbt-core 1.12 with dbt-duckdb | Marts and data tests over the lake |
| Serving | PostgreSQL 17, FastAPI, Grafana 12 | Read-only API |
| Orchestration | Apache Airflow 3.3 (standalone, LocalExecutor, SQLite metadata) | Tasks call the `blockpulse` CLI from a separate venv; a one-slot pool guards the data root |
| Kubernetes | k3d (k3s 1.31) and Helm 3 | The serving stack as StatefulSets, Deployments and Jobs; defaults mirror `compose.yaml` |
| Metrics | Prometheus 3.5, prometheus-client | Scrapes the stream sink every 5 s; alert rules for lag, freshness, quarantine, sink down |
| ML | scikit-learn Isolation Forest | Local append-only registry instead of MLflow |
| Infrastructure | Terraform (AWS provider 6.x), systemd on Amazon Linux 2023 | No inbound ports, Session Manager, self-expiring |

## Known limits

- One Kafka broker and one instance: nothing is highly available. The Kubernetes cluster has one node, so it shows self-healing of pods, not survival of a node.
- Scoring is offline (files in, files out); no streaming feature or score worker.
- The streamed lake reaches Postgres only through a full `serving-rebuild --lake`, not a continuous load, and it is a separate lake (`data/lake-stream`) from the batch one.
- The stream sink makes small files (one per partition per 5-second batch). `lake-compact` merges closed hours, but nothing schedules it.
- Postgres is a serving copy on one 512 MB container. Incremental loads slow down as the random-keyed `lifecycle_events` index outgrows memory, so large backfills use `serving-rebuild` ([04](guides/04-postgres-api-dashboard.md#two-ways-data-gets-in)).
- The Kafka-to-Parquet archive writes one file per message (built for restart clarity); the lake is the right-sized store.
- The Helm chart covers the serving stack only. Prometheus, the pipeline dashboard, Airflow and the stream sink run under Compose.
- The Kubernetes drills and a real chart install are not part of CI, and have run only on generated traffic in a cloud session.
