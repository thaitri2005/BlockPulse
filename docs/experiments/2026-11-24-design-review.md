# Design review: what to change without rebuilding

Date: 2026-11-24. The project is complete, so the question was narrow: which design weaknesses can be fixed in place, and which would need a rebuild that the gain does not justify. Two were fixed; the rest are recorded with the reason they were left.

## Fixed

| Weakness | Why it mattered | Change |
| --- | --- | --- |
| The stream sink remembered every message id and transaction id it had ever seen | A sink is meant to run for days, so its memory grew without limit (369 Mi after 20,000 messages in the cluster) until it would be killed at its 512 Mi limit | It now remembers the 200,000 most recent message ids and the transaction ids of the 8 most recently active capture runs. Redelivery after a crash only replays recent offsets, so duplicates are still caught; an older one would be written again and removed by `fct_transactions`, as after any restart |
| The enrichment worker was invisible to monitoring | It exposed no metrics and had no health check, and the only lag alert watched the sink. A stuck worker would have shown as nothing at all, because the sink downstream would simply be idle | `kafka-enrich --metrics-port` exposes messages, REST requests, error records and lag. It uses the same lag metric name as the sink, so the existing `ConsumerFallingBehind` rule covers both. In the Helm chart the worker now has a liveness probe on that endpoint and Prometheus scrapes it |

Checked in the cluster: with the worker running, Prometheus reported `up = 1` for both jobs and a lag series for each. Tests: 195 passed, 1 skipped.

## Left as they are

| Weakness | Why it stays |
| --- | --- |
| Two lakes: the batch path names a capture by its folder, the stream by its run id | Unifying them means changing the lake contract and rewriting every existing file, for a system whose two paths are already shown to agree row for row |
| Serving from the stream is a full rebuild, not a continuous load | A rebuild of 2.65 million transactions takes 10.6 minutes and is atomic. An incremental stream-to-Postgres load would be a second loader to keep consistent with the first |
| One broker, one Postgres, one node | High availability needs three brokers and a replicated database, which is a different project and several times the memory |
| The enrichment rate limit lives inside one process | The chart refuses more than one worker, which enforces the budget. A shared limiter would need a new service (decision D07 rules out Redis) |
| The maintenance CronJob mounts the sink's single-writer volume | It works because both run on one node. The clean fix is object storage for the lake, which the AWS path already uses |
| Airflow keeps its metadata in SQLite | Fine for one machine and one scheduler; Postgres metadata only matters with several schedulers |
| `cli.py` dispatches every command from one function | Restructuring is a large diff with no behaviour change, in a finished project |
