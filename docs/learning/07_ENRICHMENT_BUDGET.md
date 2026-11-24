# Exercise 7: enrichment under a budget

The goal is to understand how the pipeline asks a public API for missing data without abusing it. **Enrichment** means filling in fields a record lacks by asking another source. A **rate limit** is a cap on requests per second. A **budget** is a cap on total requests for one run. A **retry** is trying a failed request again. An **error topic** is a Kafka topic that receives a record describing each failure, so failures are data instead of lost log lines. **Backpressure** is a slow stage making the stages before it wait.

Some feed messages carry only a transaction ID. `kafka-enrich` fetches the missing fields from the mempool.space REST API, at a default of one request per second and 100 requests per run.

## Run it

Read the tests before the code. They replace the HTTP call with a function, so nothing leaves your machine:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_enrichment.py tests/test_kafka_workers.py
```

Then drive the enricher yourself with a fake fetch and a fake clock. Three ID-only transactions, two requests per second, a budget of two:

```powershell
@'
import json
from blockpulse.enrichment import RateLimitedEnricher

now, sleeps, calls = [0.0], [], []
def sleep(seconds):
    sleeps.append(seconds); now[0] += seconds
def fetch(txid):
    calls.append(txid[:4])
    return {"txid": txid, "vin": [{"sequence": 1}], "vout": [{"value": 42}], "weight": 400, "fee": 10}

added = [{"txid": c * 64} for c in "abc"]
envelope = {"schema_version": 1, "run_id": "run", "session_id": "session", "message_id": "run:1",
            "received_at": "2026-01-01T00:00:00+00:00", "source_mode": "synthetic", "message_type": "text",
            "raw_text": json.dumps({"mempool-transactions": {"sequence": 1, "added": added}})}
enricher = RateLimitedEnricher(requests_per_second=2, max_requests=2, fetch=fetch, clock=lambda: now[0], sleep=sleep)
row, errors = enricher.process(envelope)
print([event["enrichment_status"] for event in row["events"]])
print(row["enrichment_summary"])
print("calls", calls, "sleeps", sleeps)
print(errors)
'@ | .\.venv\Scripts\python.exe -
```

**With the broker (optional).** After `docker compose --profile m1 up -d --wait`, `kafka-init`, and the `kafka-replay` of the generated capture from [Exercise 6](06_CONSUMERS_AND_RESTARTS.md):

```powershell
.\.venv\Scripts\python.exe -m blockpulse kafka-enrich --group-id learn-enrich --max-messages 200
```

Every transaction in the generated capture is complete, so by the code this run makes no HTTP request: confirm it from `enrichment_requests` in the summary. Do not replay `examples/fixtures/mempool_messages.jsonl` for this step: it contains an ID-only transaction, so the worker would call the public API.

## Open the output

The offline script printed, on my run:

```text
['fetched', 'fetched', 'budget_exhausted']
{'requests': 2, 'enriched': 2, 'not_needed': 0, 'failed': 0, 'budget_exhausted': 1}
calls ['aaaa', 'bbbb'] sleeps [0.5]
[{'kind': 'budget_exhausted', 'event_id': '7c8cc9a4...', 'txid': 'cccc...cccc', 'detail': 'Per-run enrichment request budget exhausted'}]
```

The first two events were fetched and the third was refused. There was one sleep of 0.5 seconds: at two requests per second the second request had to wait half a second after the first. The third never reached `fetch`, but it was not dropped. The event is still in `row["events"]`, marked `budget_exhausted`, and `errors` holds a record for it. The Kafka worker publishes that record to `blockpulse.errors.v1`.

## Read one function

Open `src/blockpulse/enrichment.py` and read `RateLimitedEnricher._fetch()`. Trace the counter `self.requests`:

```python
if txid in self.cache:
    return self.cache[txid], "cache"
if self.requests >= self.max_requests:
    raise EnrichmentRequestError("budget_exhausted", "Per-run enrichment request budget exhausted")
now = self.clock()
if self.last_request_at is not None:
    delay = self.interval - (now - self.last_request_at)
    if delay > 0:
        self.sleep(delay)
self.last_request_at = self.clock()
self.requests += 1
```

Three checks happen in order. A transaction already fetched in this run is answered from `self.cache` and costs nothing. Then the budget: once `self.requests` reaches `max_requests`, the function raises before any network call. Then the rate: `self.interval` is `1.0 / requests_per_second`, and the function sleeps for the part of that interval that has not yet passed. The counter goes up before `self.fetch(txid)` is called, so a failed request still uses budget.

There is no retry here. A failed fetch raises `EnrichmentRequestError` with a `kind` such as `rate_limited` (HTTP 429) or `request_failed`, and `process()` turns it into an error record.

In `process()`, each `added` event first goes through `extract_features(payload)`. If that succeeds the event is marked `not_needed` and `_fetch` is never called.

Finally, `enrich_topic()` in `src/blockpulse/kafka_workers.py` calls `enricher.process(row)` once per message, in a single loop. While `_fetch` sleeps, the loop does not poll Kafka. Unread messages wait in the topic and lag grows. That is the backpressure: the topic absorbs it, and nothing is discarded.

## Change one small thing

In the offline script, change `max_requests=2` to `max_requests=3` and run it again. My run printed:

```text
['fetched', 'fetched', 'fetched']
{'requests': 3, 'enriched': 3, 'not_needed': 0, 'failed': 0, 'budget_exhausted': 0}
calls ['aaaa', 'bbbb', 'cccc'] sleeps [0.5, 0.5]
[]
```

No error records, and one more half-second wait. Next, make the third ID equal to the first (`for c in "aba"`) with the budget back at 2, and predict the statuses before you run it.

Then rerun the two test files from "Run it".

## Question to carry forward

Why must adding more enrichment workers not increase the request rate to the public API? Then look at where the limiter's state lives: `self.requests` and `self.last_request_at` belong to one `RateLimitedEnricher` object in one process. What would two workers started side by side actually do, and where would a shared limit have to live?

## What was run

Verified on Linux on 2026-11-24: `pytest tests/test_enrichment.py` (5 passed) and `tests/test_kafka_workers.py` (6 passed), and the offline script with `max_requests` 2 and 3 and with `"aba"` (the Python was run from a shell here-document, not from PowerShell; the `"aba"` output is left out so you can predict it). Not run here, because no Docker or Kafka was available to this session: `docker compose --profile m1 up`, `kafka-init`, `kafka-replay` and `kafka-enrich`. The statement that the generated capture needs no HTTP request comes from reading `process()`, not from a broker run. No request was made to the public API.
