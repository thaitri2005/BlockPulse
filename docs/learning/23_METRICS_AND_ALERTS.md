# Exercise 23: metrics and alerts

The goal is to see how a running process reports on itself, and how a rule turns those numbers into an alert. You will read the metrics the streaming lake sink (`kafka-lake`, Exercise 22) exposes, find the one line of code that feeds each alert, and learn why an alert can be true without firing. A **metric** is a named number a process publishes; **Prometheus** is the service that collects those numbers on a timer (a *scrape*) and keeps their history.

## Run it

Start with the part that needs no infrastructure. This feeds the metrics object two messages and two batch reports by hand, the same inputs as `tests/test_metrics.py`, and prints what Prometheus would scrape:

```powershell
@'
from prometheus_client import generate_latest
from blockpulse.metrics import StreamLakeMetrics
m = StreamLakeMetrics()
m.message_consumed({"received_at": "2026-10-17T00:00:00+00:00"})
m.message_consumed({"received_at": "2026-10-16T00:00:00+00:00"})
r = {"tables": {"transactions": {"rows_written": 5, "rows_quarantined": 1},
                "lifecycle_events": {"rows_written": 9, "rows_quarantined": 0}}}
m.batch_written(r); m.batch_written(r)
print(generate_latest(m.registry).decode())
'@ | .\.venv\Scripts\python.exe -
```

Then the real thing, from the [streaming guide](../guides/12-streaming-lake-and-metrics.md). It needs Kafka with messages in the enriched topic; the [Kafka guide](../guides/03-kafka.md) shows how to replay a saved capture and run `kafka-enrich`.

```powershell
docker compose --profile m1 up -d --wait          # Kafka
docker compose --profile m2 up -d --wait          # Postgres, Grafana, Prometheus (127.0.0.1:9090)
.\.venv\Scripts\python.exe -m blockpulse kafka-lake --group-id lake-1 --metrics-port 9108
```

While it runs, open `http://127.0.0.1:9108/metrics` (the raw text), `http://127.0.0.1:9090/alerts` (Prometheus) and the Grafana dashboard **BlockPulse pipeline health**. Stop both profiles with `docker compose --profile m1 stop` and `--profile m2 stop` when done.

## Open the output

The offline snippet printed this here (comment and `_created` lines removed, histogram buckets shortened):

```text
# TYPE blockpulse_messages_consumed_total counter
blockpulse_messages_consumed_total 2.0
# TYPE blockpulse_rows_written_total counter
blockpulse_rows_written_total{table="transactions"} 10.0
blockpulse_rows_written_total{table="lifecycle_events"} 18.0
# TYPE blockpulse_rows_quarantined_total counter
blockpulse_rows_quarantined_total{table="transactions"} 2.0
blockpulse_rows_quarantined_total{table="lifecycle_events"} 0.0
# TYPE blockpulse_batch_seconds histogram
blockpulse_batch_seconds_bucket{le="0.5"} 2.0
blockpulse_batch_seconds_bucket{le="+Inf"} 2.0
blockpulse_batch_seconds_count 2.0
# TYPE blockpulse_consumer_lag_messages gauge
blockpulse_consumer_lag_messages 0.0
# TYPE blockpulse_newest_event_age_seconds gauge
blockpulse_newest_event_age_seconds 3.32597373059988e+06
```

Three types appear:

- A **counter** only goes up (rows written since the process started). Its value means little by itself; you ask how fast it grows. `rate(x[1m])` is the PromQL function for that: the per-second increase over the last minute. The dashboard's throughput panel is `rate(blockpulse_messages_consumed_total[1m])`.
- A **gauge** is a current value that can go up or down (lag, age). The age above is "now minus 17 October", so yours will be larger.
- A **histogram** counts observations into buckets (`le="0.5"` means "took at most 0.5 s"), so percentiles can be estimated later. Both hand-made batches landed in the smallest bucket.

`{table="transactions"}` is a **label**: a key and value that splits one metric into several time series. Every distinct label value is a new series that Prometheus stores forever after, which is the **cardinality** problem. That is why the table name is the only label and a transaction id never is.

Notice that lag reads `0.0` although nothing ever measured it. Keep that in mind for the last section.

## Read one function

Open `src/blockpulse/metrics.py` and read `batch_written()`. Trace one number, the quarantined rows:

```python
def batch_written(self, report: dict, consumer=None) -> None:
    for table, stats in report["tables"].items():
        self.rows_written.labels(table=table).inc(stats["rows_written"])
        self.rows_quarantined.labels(table=table).inc(stats["rows_quarantined"])
    self.batches.inc()
```

The sink calls this after each micro-batch with the quality report it just wrote. The count goes into the counter, Prometheus scrapes it every 5 s (`observability/prometheus/prometheus.yml`), and `observability/prometheus/alerts.yml` evaluates:

```yaml
- alert: RowsQuarantined
  expr: increase(blockpulse_rows_quarantined_total[10m]) > 0
```

Compare it with the rule above it in the file, `ConsumerFallingBehind`, which has `for: 2m`. An alert whose expression is true is **pending**; it becomes **firing** only when the expression has stayed true for the whole `for` window. `RowsQuarantined` has no `for`, so it fires at once. The recorded replay on 2026-10-20 (guide 12, six recorded sessions) shows the difference: lag was above 1,000 for about 115 s, under the two-minute window, so `ConsumerFallingBehind` went pending and never fired.

Now read `_age()` and the `set_function` line above it: the age gauge is computed at scrape time, so it keeps growing even when no message arrives. A gauge set only inside `message_consumed()` would freeze at its last value exactly when the feed stops.

## Change one small thing

In `metrics.py`, add a bucket to the histogram, for example `0.1` at the front of `buckets=(0.5, 1, 2, ...)`. Rerun the snippet and find the new `le="0.1"` line. Then run:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_metrics.py
```

Restore the file afterwards (`git checkout src/blockpulse/metrics.py`); a new bucket changes the series a dashboard reads.

If the stack is running, a second experiment: change `for: 2m` to `for: 30s` in `alerts.yml`, restart Prometheus (`docker compose --profile m2 restart prometheus`), replay again and watch whether the alert now reaches firing. Restore that file too.

## Question to carry forward

`blockpulse_consumer_lag_messages` reads 0 before the first batch, while the age gauge reads `NaN` until there is data. On a dashboard, how would you tell "caught up" from "never measured", and which of the four alerts would notice a sink that started but never received a message?

## Verified

Run here on 2026-11-24 (Linux, `.venv/bin/python`): the snippet above, through a bash here-document, producing the output shown; `python -m pytest -q tests/test_metrics.py` (2 passed); `python -m blockpulse kafka-lake --help`. `alerts.yml`, `prometheus.yml` and the dashboard JSON were read, not loaded into Prometheus or Grafana. Not run here: the PowerShell form of the snippet, the Compose commands, `kafka-lake` against a broker, and both experiments. The 115 s figure is quoted from guide 12 (2026-10-20).
