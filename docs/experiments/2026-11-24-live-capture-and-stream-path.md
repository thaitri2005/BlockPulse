# Live capture into Kafka, and the streamed path on recorded traffic

Date: 2026-11-24. Run in a cloud development session, which turned out to reach the public mempool WebSocket (earlier agent sessions could not). This closes the two checks that were still open: the live `capture-kafka` smoke test from milestone M1, and compaction and stream serving on recorded rather than generated traffic.

Everything here is real mempool data from `wss://node203.sv1.mempool.space/api/v1/ws`. Kafka was the `m1` Compose service; Postgres was a throwaway container with a separate `blockpulse_live` database. The enrichment worker's REST fallback pointed at an unreachable address, so the public REST API was not called. Raw output is in [`reports/live-2026-11-24/`](reports/live-2026-11-24/).

## 1. Live capture straight into Kafka (60 s)

`blockpulse capture-kafka --duration 60 --url wss://node203.sv1.mempool.space/api/v1/ws`

| Check | Result |
| --- | --- |
| Capture | 12:59:29 to 13:00:29 UTC; stopped on `duration`; 1 connection, 0 connection errors |
| Messages | 57 saved to the capture file |
| Kafka | 57 queued, 57 acknowledged; end offset of `blockpulse.raw.v1` is 57 (all in one partition, as one session has one key) |
| Content | 247 distinct transactions: 243 added, 4 removed, 4 replaced |

Passed: every message written to the file was acknowledged by the broker.

## 2. The streamed path on a recorded capture

A second capture ran for 8 minutes across the 13:00 UTC hour boundary, so the lake has a closed hour and an open one: 446 messages, 8,076 distinct transactions, 3,101 added, 6,950 mined (a block arrived), 46 removed, 44 replaced; 1 connection, 0 errors. With the 60-second session that makes two recorded sessions and 503 messages.

Both sessions went through the batch path (`process-pending`) and through the streaming path (`kafka-replay`, `kafka-enrich`, `kafka-lake` with 25-message batches to produce small files as live traffic does).

| Check | Result |
| --- | --- |
| Batch lake | 3,344 transactions and 10,392 lifecycle events in 4 files per table; 0 quarantined |
| Streamed lake | The same 3,344 and 10,392 rows in 24 files per table (22 micro-batches); 0 quarantined |
| Streamed vs batch, row for row (every column except `session`) | 0 rows only in one, for both tables |
| `lake-compact` | The closed hour (12:00) went from 5 files to 1 per table; the open hour (13:00) was skipped; 24 files became 20 |
| Streamed lake before vs after compaction | 0 differing rows in both tables |
| `lake-compact` a second time | Nothing to do |
| `serving-rebuild` from the batch lake | 3,344 scores (106 flagged), 10,392 events, 8,076 transactions |
| `serving-rebuild --lake` from the compacted streamed lake | The same counts in 0.7 s; row checksums of all three tables identical to the batch rebuild |
| Lifecycle status in Postgres | 6,950 confirmed, 1,083 observed unconfirmed, 38 superseded, 5 removed for an unknown reason |

The comparison script is [`reports/live-2026-11-24/compare.py`](reports/live-2026-11-24/compare.py): for each table it counts the rows of each lake that are not in the other (`EXCEPT ALL` in both directions).

## 3. The same captures in the Kubernetes deployment, for the screenshots

The chart was installed from nothing (29 s), the two capture folders were copied into the sink's volume, and both were replayed from inside the cluster: 57 and 446 messages, both consumer groups at lag 0, then `serving-rebuild --lake` in the sink's pod (3,344 scores, 10,392 events, 8,076 transactions, 106 flagged, 3.3 s). The API's `/pipeline/status` reported the same counts as section 2: 6,950 confirmed, 1,083 observed unconfirmed, 38 superseded, 5 removed.

`docs/images/grafana-overview.png`, `grafana-pipeline.png` and `prometheus-alerts.png` were taken from that deployment at about 13:41 UTC. The capture ended at 13:07, so "newest event age" reads about 35 minutes and `DataStale` is pending. `docs/images/model-vs-gate.png` is drawn from the figures in guide 06; its source is `docs/images/model-vs-gate.html`.

## Limits

- Ten minutes of traffic, not a day. The closed hour held about 300 transactions; the 30-files-to-1 compaction measured on generated traffic in the cluster ([DE7 report](2026-11-24-de7-kubernetes.md)) is the larger case.
- No timing claims: the runs are too small to say anything about speed.
- The enrichment worker made no REST calls, so rate-limited enrichment was not exercised here; it is covered by its tests and by [guide 03](../guides/03-kafka.md).
