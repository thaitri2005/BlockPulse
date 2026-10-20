# Architecture

What is built, how the pieces connect, and where the code lives. For why, see the [plan](PROJECT_PLAN.md); for how to run each part, see the [guides](guides/).

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
```

Unattended runs wrap the same steps: `collect-loop` captures hourly sessions, `process-pending` brings every finished session up to date (process, screen, lake, compact, log, optional Postgres load), `sync-pending` uploads to S3. On AWS these run as systemd services on one instance ([08](guides/08-aws.md)).

Design rules that hold everywhere:

- **Raw is the source of truth.** Everything downstream can be rebuilt from `data/raw`. Finished captures are gzipped (`messages.jsonl.gz`, about 5x smaller); every reader opens either form.
- **The lake is the analytics store; Postgres is the serving copy.** Postgres holds all the data for the API and dashboard and can be rebuilt from the lake in minutes.
- **Every stage is idempotent.** Output folders are never overwritten; finished stages are detected and skipped; Postgres loads and Kafka consumers are safe to repeat (at-least-once delivery plus idempotent sinks).
- **One implementation per concern.** One feature function, one rule set, one lake schema (which also generates the Athena catalog), one collector for every runner.
- **Evidence is recorded.** Summaries, quality reports, manifests and checksums make every claimed number traceable.

## Commands

`python -m blockpulse --help` lists them all.

| Group | Commands | Guide |
| --- | --- | --- |
| Capture and process | `capture`, `process`, `detect`, `lab`, `trace` | [01](guides/01-capture-and-process.md), [02](guides/02-structural-rules.md) |
| Kafka | `kafka-init`, `capture-kafka`, `kafka-replay`, `kafka-archive`, `kafka-enrich`, `kafka-parquet` | [03](guides/03-kafka.md) |
| Postgres and API | `db-init`, `load-results`, `load-events`, `serving-rebuild`, `serve` | [04](guides/04-postgres-api-dashboard.md) |
| Lake and SQL | `lake-query`, `lake-schema` (the lake itself is built by `process-pending`) | [05](guides/05-lake-and-athena.md) |
| ML evaluation | `build-dataset`, `verify-dataset`, `train-iforest`, `evaluate-iforest`, `sweep`, `detectors ...` | [06](guides/06-ml-evaluation.md) |
| Unattended collection | `collect`, `collect-loop`, `process-pending`, `sync-pending`, `compact-raw` | [07](guides/07-collection.md), [08](guides/08-aws.md) |
| Load test and drills | `benchmark batch`, `benchmark streaming`, `drill enrich-crash`, `drill broker-restart`, `drill postgres-crash` | [09](guides/09-load-test.md), [10](guides/10-failure-drills.md) |

## Code

Python package `src/blockpulse/` (about 4,500 lines, flat on purpose):

| Module | Lines | Role |
| --- | ---: | --- |
| `capture.py` | 198 | Bounded WebSocket capture, reconnects, gap records |
| `events.py` | 118 | One saved message to normalized lifecycle events |
| `features.py` | 66 | The shared feature function (`transaction-v2`) |
| `process.py` | 131 | Capture JSONL to events, features and an error report |
| `detector.py`, `detect.py`, `lab.py` | 330 | The three structural rules, offline screening, synthetic rule cases |
| `storage.py` | 56 | Never-overwrite run folders, atomic summaries |
| `kafka_pipeline.py`, `kafka_workers.py` | 571 | Topics, publisher, replay, archive consumer, enrichment consumer (batched checkpoints) |
| `enrichment.py`, `parquet_archive.py` | 370 | Rate-limited REST enrichment; Kafka-to-Parquet archive |
| `db.py`, `results.py`, `lifecycle.py` + `migrations/` | 303 | Postgres migrations; bulk (`COPY` + set-based merge) duplicate-safe loads; lifecycle status projection |
| `serving.py` | 190 | Rebuild the serving tables from the lake and swap them in atomically (`serving-rebuild`) |
| `api.py`, `trace.py` | 238 | Read-only FastAPI; one transaction traced back to its raw message |
| `lake.py`, `analytics.py` | 356 | Lake contracts, quality gate, partitioned writer, Glue schema export; DuckDB views |
| `dataset.py` | 320 | Checksummed chronological manifests, lake- or JSONL-backed split loading |
| `model.py`, `evaluate.py`, `sweep.py`, `registry.py` | 719 | Isolation Forest, threshold calibration, synthetic sweep, detector registry and gate |
| `collector.py` | 314 | Capture loop, idempotent session processing, lock, S3 upload |
| `benchmark.py` | 198 | Batch and streaming load tests on recorded sessions (`benchmark`) |
| `drills.py` | 191 | Failure drills: worker crash, broker restart, Postgres crash (`drill`) |
| `cli.py` | 453 | Command-line entry point |

Elsewhere:

| Path | What it is |
| --- | --- |
| `tests/` | pytest; Postgres tests opt in with `BLOCKPULSE_TEST_DSN` |
| `analytics/queries/` | Shared SQL, run by DuckDB locally and saved as Athena named queries |
| `infra/aws/terraform/` | The AWS collector stack (VPC, instance, S3, IAM, Glue, Athena, budget) and its mocked-provider tests |
| `infra/aws/instance/` | `install.sh`, `deadline-check.sh` and the hardened systemd units that run on the instance |
| `infra/aws/tests/` | Amazon Linux 2023 container rehearsals: bootstrap smoke test and real systemd end to end |
| `infra/aws/build_bundle.py` | Reproducible app zip for the instance |
| `observability/grafana/` | Provisioned datasource and dashboard |
| `compose.yaml` | Profiles `m1` (Kafka) and `m2` (Postgres, Grafana) |
| `scripts/` | Laptop collection task, M2 demo, M3 reproduction |
| `examples/` | Learning exercise 1 and synthetic fixtures |
| `.github/workflows/ci.yml` | CI: pytest with Postgres, Terraform checks, container rehearsals |

## Technology

| Concern | Choice | Notes |
| --- | --- | --- |
| Language | Python 3.11+ | websockets, confluent-kafka, pyarrow, duckdb, psycopg, FastAPI, scikit-learn |
| Streaming | Apache Kafka 4.3 (KRaft, one broker) | Topics `blockpulse.raw.v1`, `.enriched.v1`, `.errors.v1`; 3 partitions; keyed by capture session |
| Raw and replay | JSONL | Inspectable; byte-identical replay |
| Lake | Parquet + Zstandard, Hive partitions `dt`/`hour` | One file per session per partition |
| SQL | DuckDB locally, Athena in AWS | Same queries; Glue partition projection; 1 GiB scan cap |
| Serving | PostgreSQL 17, FastAPI, Grafana 12 | Read-only API |
| ML | scikit-learn Isolation Forest | Local append-only registry instead of MLflow |
| Infrastructure | Terraform (AWS provider 6.x), systemd on Amazon Linux 2023 | No inbound ports, Session Manager, self-expiring |

## Known limits

- One Kafka broker and one instance: nothing is highly available.
- Scoring is offline (files in, files out); no streaming feature or score worker.
- The Kafka path and the Postgres path connect through files, not topics.
- Postgres is a serving copy on one 512 MB container. Incremental loads slow down as the random-keyed `lifecycle_events` index outgrows memory, so large backfills use `serving-rebuild` ([04](guides/04-postgres-api-dashboard.md#two-ways-data-gets-in)).
- The Kafka-to-Parquet archive writes one file per message (built for restart clarity); the lake is the right-sized store.
