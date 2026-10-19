# 09 · Load test

How fast is each part of the pipeline on real recorded traffic, and where is the bottleneck? Both tests replay data collected by the [AWS run](08-aws.md), so they never touch the public source. Reports are written to `data/benchmarks/` with the host and library versions.

```powershell
.\.venv\Scripts\python.exe -m blockpulse benchmark batch --sessions session-aws-20261017            # one full UTC day
docker compose -p blockpulse-bench --profile m1 up -d --wait kafka                                  # throwaway broker, own volume
.\.venv\Scripts\python.exe -m blockpulse benchmark streaming --sessions session-aws-20261017 --limit 6
docker compose -p blockpulse-bench --profile m1 down -v                                             # remove it again
```

The streaming test must run against an empty broker: it reads both topics back to compute latency. The `-p blockpulse-bench` project gets its own volume, so your normal Kafka data is untouched. Stop the normal broker first, because both use port 9092.

Host for every number below: Intel i5-1340P (12 cores), 16 GB RAM, Windows 11, Docker Desktop, one Kafka broker in a container, Python 3.12 from the locked environment.

## Batch path: process, screen, lake

What `process-pending` does for each finished session. Input: all 24 hourly sessions of Saturday 2026-10-17 (3.45 GB raw, 54,938 messages, 1.83 M lifecycle events, 811,672 transactions, 2.64 M lake rows).

| Stage | Seconds | Rate |
| --- | ---: | ---: |
| Process (parse JSON, normalize events, features) | 208 | 8,777 events/s |
| Screen (rules) | 37 | 21,873 transactions/s |
| Lake (validate contracts, write Parquet) | 101 | 26,029 rows/s |
| **Total** | **347 (5.8 min)** | 9.5 MB/s of raw input |

- **A full day of mainnet traffic processes in 5.8 minutes, about 250 times faster than real time** (4.15 capture-hours per wall-clock minute). The AWS instance processes every 10 minutes and never falls behind.
- Per hourly session: p50 15 s, p99 23 s. Peak process memory 352 MB.
- Processing is 60% of the time, almost all of it JSON parsing of the raw messages. That's the place to optimize if it ever matters (for example a faster JSON parser); it doesn't at current volume.

## Streaming path: Kafka replay and enrichment

Six hours of the same Saturday (13,707 messages, 860 MB, 466,920 events) are published to `blockpulse.raw.v1` as fast as possible while the enrichment worker consumes them and writes `blockpulse.enriched.v1`. Latency is the Kafka append time of each enriched record minus that of its raw record, joined by `message_id`.

| | Before | After batching commits |
| --- | ---: | ---: |
| Enrichment throughput | 86 messages/s | **152 messages/s** |
| Time until the backlog was gone | 159 s | **90 s** |
| End-to-end latency p50 | 72 s | **0.16 s** |
| End-to-end latency p90 / p99 | 101 s / 106 s | **5.1 s / 9.4 s** |
| Messages matched, raw to enriched | 13,707 / 13,707 | 13,707 / 13,707 |
| Publish rate (producer, same machine) | 386 messages/s | 186 messages/s |

**What the first run showed.** The producer published 4.5 times faster than the consumer could enrich, so a backlog grew and latency was really just queueing time: a minute or more per message. The worker waited for every output record to be acknowledged and committed its input offset after every single message, a synchronous round trip to the broker each time.

**The fix.** Produce without waiting, and checkpoint every 500 messages or every second: flush the producer, check every delivery succeeded, then commit the last processed offset of each partition. The guarantee is unchanged. A crash before a checkpoint means those messages are processed again (at-least-once); a failed delivery stops the worker without committing (tested). Downstream sinks already deduplicate by `message_id`, so a replayed batch doesn't create duplicate rows.

**Reading the "after" column honestly.** Producer, consumer and broker share one laptop, so the faster consumer now competes with the producer for CPU, and the publish rate fell. The consumer keeps pace with the producer instead of falling behind, which is why latency collapsed. The p90/p99 tail comes from the moments when the producer briefly outran the consumer. On separate machines both numbers would be higher.

**For scale:** the live feed averaged 0.67 messages a second over the 89-hour AWS run, and 0.83 in its busiest hour (each message carries a batch of mempool events). Even the old worker had about 100 times headroom over live traffic; the bottleneck mattered for replays and backfills, which is exactly what a load test is for.

## Limits

- One machine, one broker, one consumer: these are throughput figures for this setup, not a capacity plan.
- The enricher made no REST calls (the AWS data is complete), so the test measures Kafka and processing, not the rate-limited public API, which is capped at 1 request per second by design.
- Latency under a replay measures queueing behind a backlog; steady-state latency at live rates would be lower.
