# Learning path

BlockPulse is also a way to learn streaming data engineering. The code moved faster than the walkthroughs: Exercise 1 is written, and everything after it was built without a guided explanation. This page lists the exercises in order so we can catch up before adding Postgres and the API in M2.

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
