# Learning path

BlockPulse is also a way to learn data engineering. This page lists 28 walkthroughs in the order the pipeline was built, each pointing at the code to read.

Exercises that need no services (1 to 4, 12 to 16, 19 to 21) run on the bundled fixture or on data you generate with `blockpulse synth-capture`, so they work on any machine. The ones that need Kafka, Postgres, Airflow, AWS or a cluster say which service to start first. Every page ends with a note of which commands were run when it was written and which were taken from the guides; where a command was not run, treat its expected output as a prediction to check.

Each exercise follows the same pattern:

1. **Run it.** Use saved data, so no internet is needed.
2. **Open the output.** Look at real records before reading code.
3. **Read one function.** Trace a single field from input to output.
4. **Change one small thing.** Re-run, and check that the tests still pass.
5. **Write down one question** you still have. The next session starts there.

Each exercise is sized for one sitting of about 30-60 minutes. Stop when a concept is new; understanding one record beats skimming ten modules.

| # | Exercise | Concepts | Code to read | Status |
| --- | --- | --- | --- | --- |
| 1 | [Follow one transaction](01_TRANSACTION_PATH.md) | Transaction fields, features, replay | `features.py` (66 lines) | Written |
| 2 | [From a stream to a file](02_STREAM_TO_FILE.md) | WebSocket, envelopes, JSONL, gaps vs reconnects | `capture.py` → `capture()`; `events.py` → `parse_record()` | Written |
| 3 | [From a file to a feature table](03_FILE_TO_FEATURES.md) | Normalization, dedup, error reports, determinism | `process.py` → `process_capture()` | Written |
| 4 | [Rules as a first detector](04_RULES_AS_DETECTOR.md) | Thresholds, explainability, why flags aren't labels | `detector.py` → `screen_transaction()`; `lab.py` | Written |
| 5 | [Kafka basics with our own data](05_KAFKA_BASICS.md) | Topics, partitions, keys, producers, offsets | `kafka_pipeline.py` → `RawKafkaPublisher`, `publish_capture()` | Written |
| 6 | [Consumers, commits, and restarts](06_CONSUMERS_AND_RESTARTS.md) | Consumer groups, at-least-once, idempotent sinks, lag | `kafka_pipeline.py` → `archive_topic()`; `parquet_archive.py` | Written |
| 7 | [Enrichment under a budget](07_ENRICHMENT_BUDGET.md) | Rate limits, retries, error topics, backpressure | `enrichment.py` → `RateLimitedEnricher`; `kafka_workers.py` → `enrich_topic()` | Written |
| 8 | [Why a database, and what a unique key buys us](08_DATABASE_AND_UNIQUE_KEY.md) | Tables, constraints, migrations, idempotent writes | `migrations/001_initial.sql`; `db.py` → `apply_migrations()`; `results.py` | Written |
| 9 | [Evidence versus projection](09_EVIDENCE_VS_PROJECTION.md) | Lifecycle events, order-independent status, why late data can't roll status back | `lifecycle.py` → `PROJECT`; `tests/test_lifecycle.py` | Written |
| 10 | [A read-only API and a dashboard](10_READ_ONLY_API.md) | Keyset pagination, input validation, read-only connections, SQL panels | `api.py` → `list_alerts()`; `observability/grafana/dashboards/` | Written |
| 11 | [Tracing one transaction end to end](11_TRACE_END_TO_END.md) | Provenance, replay check, why raw data is kept | `trace.py`; run `scripts/demo_m2.ps1` first | Written |
| 12 | [Honest evaluation: splits, leakage, manifests](12_SPLITS_AND_MANIFESTS.md) | Chronological splits, why whole sessions, checksums, what a split can and cannot prove | `dataset.py` → `choose_cuts()`, `load_split()`; [ML evaluation §1](../guides/06-ml-evaluation.md#1-dataset-manifest-and-chronological-split) | Written |
| 13 | [What an Isolation Forest can and can't see](13_ISOLATION_FOREST.md) | Unsupervised anomaly scores, log transforms, no extrapolation, in-sample scores | `model.py` → `feature_matrix()`, `train_isolation_forest()`; [ML evaluation §2](../guides/06-ml-evaluation.md#2-the-model-isolation-forest) | Written |
| 14 | [Choosing a threshold without labels](14_THRESHOLD_WITHOUT_LABELS.md) | Alert budgets, calibration vs test, why the flag rate moved, agreement is not accuracy | `evaluate.py` → `choose_threshold()`; [first results](../experiments/2026-10-14-m3-first-evaluation.md) | Written |
| 15 | [Testing a detector on shapes you define](15_SYNTHETIC_SWEEP.md) | Synthetic labeled cases, benign controls, why a sweep isn't accuracy, saturation | `sweep.py` → `cases()`; [sweep results](../experiments/2026-10-14-m3-synthetic-sweep.md) | Written |
| 16 | [Who is in charge? Champions, gates, and rollback](16_CHAMPIONS_AND_GATES.md) | Registries as event logs, promotion criteria, why a rejected model is still a result | `registry.py` → `evaluate_promotion()`, `state()`; [ML evaluation §5](../guides/06-ml-evaluation.md#5-detector-bundles-registry-and-promotion-gate) | Written |
| 17 | [Infrastructure as code you can test](17_INFRASTRUCTURE_AS_CODE.md) | Terraform resources and variables, mocked-provider tests, least-privilege IAM, why no inbound ports | `infra/aws/terraform/` and `tests/collector.tftest.hcl`; [AWS guide](../guides/08-aws.md) | Written |
| 18 | [Running a service properly on Linux](18_SYSTEMD_SERVICES.md) | systemd services and timers, clean shutdown with SIGINT, sandboxing, idempotent jobs | `infra/aws/instance/systemd/`, `collector.py`; `infra/aws/tests/systemd_e2e.sh` | Written |
| 19 | [Lake layout and data contracts](19_LAKE_AND_CONTRACTS.md) | Parquet vs JSONL, Hive partitions and pruning, small files, quarantine vs silent fixes, idempotent rebuilds | `lake.py` → `validate()`, `build_session()`; [lake guide](../guides/05-lake-and-athena.md) | Written |
| 20 | [One SQL, two engines](20_ONE_SQL_TWO_ENGINES.md) | DuckDB over local files, Athena over S3, partition projection, scan cost | `analytics.py`, `analytics/queries/`, `infra/aws/terraform/analytics.tf` | Written |
| 21 | [Modelling with dbt](21_DBT_MODELLING.md) | Sources, staging, intermediate and marts; `ref` and lineage; schema and data tests; why one rule should live in one place | `analytics/dbt/`; [dbt guide](../guides/11-dbt.md) | Written |
| 22 | [Exactly-once output from an at-least-once stream](22_EXACTLY_ONCE_STREAM_SINK.md) | Micro-batches, offset-named files, commit after write, why replay must delete by offset range | `stream_lake.py` → `stream_to_lake()`, `_remove_replayed()`; `tests/test_stream_lake.py` | Written |
| 23 | [Metrics and alerts](23_METRICS_AND_ALERTS.md) | Counters vs gauges vs histograms, label cardinality, `rate()`, pending vs firing | `metrics.py`; `observability/prometheus/alerts.yml`; [streaming guide](../guides/12-streaming-lake-and-metrics.md) | Written |
| 24 | [Orchestration: DAGs, retries, backfills](24_ORCHESTRATION.md) | Data intervals, idempotent tasks, retries with backoff, pools vs `max_active_runs`, why a killed task must release its lock | `orchestration/airflow/dags/blockpulse_daily.py`; `collector.py` → `_Lock`; [Airflow guide](../guides/13-airflow.md) | Written |
| 25 | [A cluster on your own machine](25_LOCAL_CLUSTER.md) | Clusters, nodes, pods, contexts; declared state vs started processes; debugging a pod that won't start from its events | `infra/k8s/k3d.yaml`, `infra/k8s/cluster-up.sh`; [Kubernetes guide](../guides/14-kubernetes.md) | Written |
| 26 | [State that survives its pod](26_STATE_THAT_SURVIVES.md) | Helm charts and values, StatefulSets, volume claims, headless Services, probes, requests and limits | `infra/k8s/chart/blockpulse/templates/postgres.yaml`, `values.yaml`; `tests/test_helm_chart.py` | Written |
| 27 | [Stateless services, jobs and configuration](27_STATELESS_SERVICES.md) | Deployments vs StatefulSets, Services, Jobs and Helm hooks, Secrets and ConfigMaps, readiness vs liveness | `infra/k8s/chart/blockpulse/templates/api.yaml`, `setup-jobs.yaml`; `infra/k8s/Dockerfile` | Written |
| 28 | [Who restarts it? Drills on Kubernetes](28_K8S_DRILLS.md) | Controllers and desired state, graceful vs forced deletion, consumer-group lag as a progress measure, condition-triggered faults | `k8s_drills.py` → `drill_enrich_kill()`; `synth.py`; [Kubernetes guide](../guides/14-kubernetes.md#the-drills-as-pod-kills-srcblockpulsek8s_drillspy) | Written |

## Useful commands while learning

```powershell
.\.venv\Scripts\python.exe -m pytest -q                       # all tests (~10 s)
.\.venv\Scripts\python.exe -m pytest -q tests/test_features.py  # one area
.\.venv\Scripts\python.exe -m blockpulse --help                # every command
.\.venv\Scripts\python.exe -m blockpulse process --help         # one command's options
```
