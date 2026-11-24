# BlockPulse: plan and status

Last updated 2026-11-24. This page says what the project is for, where it stands, what could come next, and which decisions shaped it. How the system works is in [ARCHITECTURE.md](ARCHITECTURE.md); how to run each part is in the [guides](guides/); the dated evidence is in [experiments](experiments/); the step-by-step history is in [archive/milestone-history.md](archive/milestone-history.md).

## 1. Purpose

BlockPulse is a Bitcoin mempool data pipeline built to develop **data engineering** skill on a real live source (decision D18): streaming capture, a partitioned Parquet lake with data-quality checks, SQL analytics locally and in AWS, reproducible evaluation, infrastructure as code, and measured operational behaviour. Anomaly screening is the use case that gives the data a reason to flow.

What the project will not claim:

- A flag means unusual structure, not fraud, illegality or intent.
- A model score is not a probability. There are no independent labels, so no accuracy, precision or recall is claimed.
- One public provider's mempool view is not the whole network, and reconnect gaps are recorded as a lower bound.
- A single-broker, single-node setup is not highly available.
- Public blockchain data can be linked to people: results describe patterns, never address owners.

## 2. Status

The roadmap (milestones M0–M3 and steps DE1–DE7) is complete.

| Area | State | Guide |
| --- | --- | --- |
| Capture, processing, structural rules | Done | [01](guides/01-capture-and-process.md), [02](guides/02-structural-rules.md) |
| Kafka ingest, enrichment, replay | Done. Open: a 60 s live `capture-kafka` smoke test from the owner's shell | [03](guides/03-kafka.md) |
| Postgres serving, API, Grafana | Done | [04](guides/04-postgres-api-dashboard.md) |
| Lake, quality gate, DuckDB and Athena | Done | [05](guides/05-lake-and-athena.md) |
| ML evaluation and detector registry | Done. Rules remain champion | [06](guides/06-ml-evaluation.md) |
| Unattended collection, AWS collector | Done; the AWS stack is torn down | [07](guides/07-collection.md), [08](guides/08-aws.md) |
| Load tests and failure drills | Done | [09](guides/09-load-test.md), [10](guides/10-failure-drills.md) |
| dbt models and tests; the hourly mart refreshable by day | Done | [11](guides/11-dbt.md) |
| Streaming into the lake, metrics, alerts, compaction, serving from the streamed lake | Done; compaction and stream serving tested on fixtures only | [12](guides/12-streaming-lake-and-metrics.md) |
| Airflow orchestration | Done | [13](guides/13-airflow.md) |
| Kubernetes: cluster, Helm chart for the whole streaming and serving path, pod-kill drills | Done on generated traffic, in a cloud session and in CI. Open: a run on the laptop | [14](guides/14-kubernetes.md) |

Checks on 2026-11-24 (Linux, Python 3.12): 190 tests passed and 1 skipped; ruff, shellcheck and `helm lint --strict` clean. On Windows without `helm` the 14 chart tests skip.

## 3. What could come next

Nothing is scheduled. Candidates, roughly by value for learning:

| Item | Why | Runs on |
| --- | --- | --- |
| Run the Kubernetes quick start and drills on the laptop | The image and scripts have only run behind a cloud session's proxy | Laptop |
| Schedule `lake-compact` and time it, and `serving-rebuild --lake`, on recorded traffic | Both exist and are tested on fixtures only | Laptop |
| A continuous load from the stream into Postgres | Serving from the streamed lake is a full rebuild | Laptop |
| Schedule compaction and the serving rebuild in the cluster (a CronJob) | They are run by hand with `kubectl exec` | Cloud session or laptop |
| Airflow on the AWS instance | The laptop sleeps and misses scheduled runs | AWS, deployed by the owner |
| Hand-labelled sample (about 200 flagged rows, using the review rubric) | The only route to any accuracy figure | Laptop |
| Fee feature relative to the hour's median | The model's measured blind spot (new `FEATURE_SET_VERSION`) | Laptop |
| A second, longer AWS collection (for example a week) to retrain and recheck the gate across seeds | Stability of the model comparison | AWS only |
| Entity analysis (address grouping over windows) | Original stretch goal; needs coverage and memory evidence | Laptop for design, AWS for long runs |

Deliberately avoided: managed Kafka (MSK), EKS and RDS (each costs more per month than the whole project), and anything that must keep running after a demo.

**Where work runs.** Jobs measured in minutes run on the laptop or in a cloud development session; anything that runs for hours goes to a short-lived AWS instance with a deadline (decisions D21, D30).

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
| D24 | dbt models run on DuckDB over the Parquet lake, through a wrapper that sets absolute paths | 2026-10-20: no extra service; views must use absolute paths because DuckDB resolves them when queried |
| D25 | The stream writes the same lake tables as the batch path, with micro-batches named by partition and first offset, deleted and rewritten on replay, and offsets committed after writing | 2026-10-20: exactly-once output on at-least-once delivery without transactions; a separate `data/lake-stream` lets the two paths be compared row for row |
| D26 | Metrics go to Prometheus from a per-process registry; labels are table names only | 2026-10-20: low cardinality; alert rules use `for` windows so a catching-up replay doesn't fire |
| D27 | Airflow orchestrates the CLI; it doesn't contain pipeline logic. BlockPulse runs from its own venv in the Airflow image, metadata is SQLite, the unit of work is one UTC day of capture starts (`process-pending --date`), and a one-slot pool guards the data root | 2026-10-20: one implementation for every runner (D15); Airflow's dependencies never mix with the lock; a pool also covers backfill runs, which `max_active_runs` doesn't |
| D28 | A processing lock is stale when its process is gone (same host) or it is 3 h old, and SIGTERM exits through cleanup | 2026-10-20: a task killed by Airflow after the laptop slept left its lock and blocked every retry |
| D29 | Kafka on Kubernetes is a plain StatefulSet running the same `apache/kafka` image as Compose, not Strimzi or Redpanda | 2026-11-24: the drills must compare like with like (same broker, same version); an operator adds custom resources and about 0.5 GB without changing what the drills prove. Revisit if the goal becomes learning operators |
| D31 | Drills that can't use recorded sessions run on generated captures labelled synthetic (`synth-capture`), and reports say so | 2026-11-24: recorded data is local evidence outside the repository; a deterministic generator also lets drills run anywhere, CI included. Counts from synthetic and recorded runs are never compared |
| D30 | DE7 is developed in an ephemeral cloud session on generated or fixture data; every step is pushed | 2026-11-24: the session has the same memory as the laptop with nothing competing for it; the recorded dataset stays on the laptop, so DE7 numbers are labelled with their data |
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
| A restricted host breaks nested containers (untrusted proxy CA, `oom_score_adj` writes refused) | `cluster-up.sh` mounts a CA on request and installs a runc shim only when it detects the restriction ([14](guides/14-kubernetes.md)) |
| Too many services slow delivery | Compose profiles; stop what isn't in use |
| The laptop sleeps during long runs (standby froze Docker for 54 min mid-load and Airflow killed the task) | Minute-scale jobs only on the laptop (D21); a killed task no longer blocks its retries (D28) |
| Postgres incremental loads slow as indexes outgrow its 512 MB container | Hourly loads stay small; backfills use `serving-rebuild` |

## 6. Resuming work

1. Read the [README](../README.md) and this page.
2. Run the tests: `.\.venv\Scripts\python.exe -m pytest -q` (set `BLOCKPULSE_TEST_DSN` and start Postgres for the database tests; see [04](guides/04-postgres-api-dashboard.md)).
3. Pick an item from section 3 with the owner. Ask before adding infrastructure.
4. Before stopping: update sections 2 and 3, and say what is still running (containers, a cluster).

In a cloud session, the Kubernetes scripts need `K3D_CA_BUNDLE=/root/.ccr/ca-bundle.crt` ([14](guides/14-kubernetes.md#restricted-hosts)).
