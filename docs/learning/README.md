# Learning path

BlockPulse is also a way to learn data engineering. The code moved faster than the walkthroughs: Exercise 1 is written, and the rest are outlined. This page lists them in the order the pipeline was built, each pointing at the code and guide to read.

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
| 2 | From a stream to a file | WebSocket, envelopes, JSONL, gaps vs reconnects | `capture.py` → `capture()`; `events.py` → `parse_record()` | To write together |
| 3 | From a file to a feature table | Normalization, dedup, error reports, determinism | `process.py` → `process_capture()` | To write together |
| 4 | Rules as a first detector | Thresholds, explainability, why flags aren't labels | `detector.py` → `screen_transaction()`; `lab.py` | To write together |
| 5 | Kafka basics with our own data | Topics, partitions, keys, producers, offsets | `kafka_pipeline.py` → `RawKafkaPublisher`, `publish_capture()` | To write together |
| 6 | Consumers, commits, and restarts | Consumer groups, at-least-once, idempotent sinks, lag | `kafka_pipeline.py` → `archive_topic()`; `parquet_archive.py` | To write together |
| 7 | Enrichment under a budget | Rate limits, retries, error topics, backpressure | `enrichment.py` → `RateLimitedEnricher`; `kafka_workers.py` → `enrich_topic()` | To write together |
| 8 | Why a database, and what a unique key buys us | Tables, constraints, migrations, idempotent writes | `migrations/001_initial.sql`; `db.py` → `apply_migrations()`; `results.py` | To write together |
| 9 | Evidence versus projection | Lifecycle events, order-independent status, why late data can't roll status back | `lifecycle.py` → `PROJECT`; `tests/test_lifecycle.py` | To write together |
| 10 | A read-only API and a dashboard | Keyset pagination, input validation, read-only connections, SQL panels | `api.py` → `list_alerts()`; `observability/grafana/dashboards/` | To write together |
| 11 | Tracing one transaction end to end | Provenance, replay check, why raw data is kept | `trace.py`; run `scripts/demo_m2.ps1` first | To write together |
| 12 | Honest evaluation: splits, leakage, manifests | Chronological splits, why whole sessions, checksums, what a split can and cannot prove | `dataset.py` → `choose_cuts()`, `load_split()`; [ML evaluation §1](../guides/06-ml-evaluation.md#1-dataset-manifest-and-chronological-split) | To write together |
| 13 | What an Isolation Forest can and can't see | Unsupervised anomaly scores, log transforms, no extrapolation, in-sample scores | `model.py` → `feature_matrix()`, `train_isolation_forest()`; [ML evaluation §2](../guides/06-ml-evaluation.md#2-the-model-isolation-forest) | To write together |
| 14 | Choosing a threshold without labels | Alert budgets, calibration vs test, why the flag rate moved, agreement is not accuracy | `evaluate.py` → `choose_threshold()`; [first results](../experiments/2026-10-14-m3-first-evaluation.md) | To write together |
| 15 | Testing a detector on shapes you define | Synthetic labeled cases, benign controls, why a sweep isn't accuracy, saturation | `sweep.py` → `cases()`; [sweep results](../experiments/2026-10-14-m3-synthetic-sweep.md) | To write together |
| 16 | Who is in charge? Champions, gates, and rollback | Registries as event logs, promotion criteria, why a rejected model is still a result | `registry.py` → `evaluate_promotion()`, `state()`; [ML evaluation §5](../guides/06-ml-evaluation.md#5-detector-bundles-registry-and-promotion-gate) | To write together |
| 17 | Infrastructure as code you can test | Terraform resources and variables, mocked-provider tests, least-privilege IAM, why no inbound ports | `infra/aws/terraform/` and `tests/collector.tftest.hcl`; [AWS guide](../guides/08-aws.md) | To write together |
| 18 | Running a service properly on Linux | systemd services and timers, clean shutdown with SIGINT, sandboxing, idempotent jobs | `infra/aws/instance/systemd/`, `collector.py`; `infra/aws/tests/systemd_e2e.sh` | To write together |
| 19 | Lake layout and data contracts | Parquet vs JSONL, Hive partitions and pruning, small files, quarantine vs silent fixes, idempotent rebuilds | `lake.py` → `validate()`, `build_session()`; [lake guide](../guides/05-lake-and-athena.md) | To write together |
| 20 | One SQL, two engines | DuckDB over local files, Athena over S3, partition projection, scan cost | `analytics.py`, `analytics/queries/`, `infra/aws/terraform/analytics.tf` | To write together |

## Exercise outlines

These are starting points, not finished walkthroughs. We turn one into a full page when we do it.

**2. From a stream to a file.** Open `data/raw/phase0-10min-*/messages.jsonl` and look at one line: which fields did *we* add (the envelope), and which came from mempool.space (`payload`)? Open `connections.jsonl` and find the two reconnects. Question to answer: why can't a reconnect recover the transactions we missed?

**3. From a file to a feature table.** Run `process` twice on the same capture and compare the outputs. Find one `added` event in `events.jsonl` and its row in `features.csv`. Question to answer: why does `process` record an incomplete transaction as an error instead of using zero?

**4. Rules as a first detector.** Run `lab`, then open `cases.csv` and find a benign counterexample that gets the same flag as its target shape. Then run `detect` on the ten-minute features with `--many-outputs-min 20` and compare the alert count with the default run. Question to answer: what information would the detector need to tell those two cases apart?

**5. Kafka basics.** With the broker running, list the topics, describe `blockpulse.raw.v1`, and replay the fixture capture into a scratch topic. Question to answer: what does the record key decide, and why is ours the capture session ID?

**6. Consumers and restarts.** Run `kafka-archive` with a group ID, stop it with Ctrl+C partway through, then run it again with the same group. Then try a *new* group ID. Question to answer: when is an offset committed, and what would go wrong if we committed before writing the file?

**7. Enrichment under a budget.** Read the mocked tests in `tests/test_enrichment.py` before the code. Question to answer: why must adding more enrichment workers *not* increase the request rate to the public API?

## Useful commands while learning

```powershell
.\.venv\Scripts\python.exe -m pytest -q                       # all tests (~10 s)
.\.venv\Scripts\python.exe -m pytest -q tests/test_features.py  # one area
.\.venv\Scripts\python.exe -m blockpulse --help                # every command
.\.venv\Scripts\python.exe -m blockpulse process --help         # one command's options
```
