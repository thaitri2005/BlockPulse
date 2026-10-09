# Phase 0 and Phase 1 validation

Date: 2026-10-09. Capture timestamps below are UTC. This report combines the ten-minute source experiment with an offline replay and a bounded review of the current rule baseline.

Local artifacts: raw capture `data/raw/phase0-10min-c71a01e1cdd340ea9d300a3506c06ee1/`; processed replay `data/processed/phase0-10min-20261009-fixed/`; screened results `data/screened/phase0-10min-20261009/`. The `data/` directory is ignored by Git.

## Host and operating choice

| Measurement | Result |
| --- | --- |
| CPU | 13th Gen Intel Core i5-1340P; 12 cores / 16 logical processors |
| Installed memory visible to Windows | 15.63 GiB |
| Available memory at measurement time | 2.00 GiB (a momentary snapshot, not the machine's idle baseline) |
| Free space on workspace drive D: | 65.93 GiB |

The capture used the full-detail `track-mempool` subscription with finite per-run limits; no app-level sampling or REST enrichment was used. That is appropriate for bounded local experiments: this ten-minute run wrote 12.2 MiB of raw JSONL. The observed burst rate would use about 1.72 GiB/day if sustained continuously, so this does not justify indefinite retention. Keep captures bounded and measure retention before any always-on run. The 50 MiB per-run byte limit remains enabled.

## Capture result

Command configuration: `wss://node203.sv1.mempool.space/api/v1/ws`, `track-mempool`, 600-second duration, 10,000-message limit, 50 MiB raw-file limit, 4 MiB maximum message, 3 reconnects, 15-second opening timeout.

| Measurement | Result |
| --- | ---: |
| Elapsed time / stop reason | 601.0 seconds / duration |
| Connection attempts / successful connections / connection errors | 3 / 3 / 2 |
| Saved source messages | 402 |
| Observed events | 6,983 |
| Added / removed / mined / replaced | 2,732 / 584 / 3,468 / 199 |
| Unique transaction IDs across events | 6,259 |
| Sequence discontinuities | 0 |
| Recorded gaps | 3, total about 7.5 seconds; history was not recovered |

The two connection errors were keepalive ping timeouts. Capture reconnected successfully each time. The first gap covers initial connection setup; the other two are reconnect gaps. All four lifecycle event types were observed, but these counts describe only this provider's messages during this run. They do not establish complete mempool coverage.

## Message size and storage estimate

Measured from the 402 preserved text payloads and the raw JSONL file:

| Measurement | Result |
| --- | ---: |
| Payload bytes | 11.42 MiB |
| Raw JSONL including capture envelopes and escaping | 12.22 MiB |
| Mean / median payload size | 29.8 KB / 9.0 KiB |
| 95th percentile payload size | 102.5 KiB |
| Largest payload | 1.53 MiB |
| Raw-file rate over this run | 1.22 MiB/minute |
| Continuous raw-file rate if this burst persisted | 1.72 GiB/day |

That daily figure is a simple extrapolation from one short, active, interrupted sample. It is a capacity warning, not a forecast. A planned ten-minute capture each day at this run's rate would store about 12.2 MiB/day before processed outputs.

## Replay and provider-shape finding

Offline processing of the saved capture produced 2,732 `transaction-v2` feature rows from 2,732 added transaction events. All rows had `vin`, `vout`, `weight`, and `fee`; no added transaction was incomplete. There were no duplicate feature rows, conflicting features, or processing issues. The two unrecognized messages were `conversions` market-rate updates, not transaction lifecycle messages.

The run exposed the provider's replacement shape: each entry contains the old transaction ID in `replaced` and the replacement transaction under `by`. The original parser preserved these records but could not identify their IDs, so 78 messages had a capture-time parse diagnostic. The adapter now records the old transaction as the event's `txid`, the new one as `replacement_txid`, and preserves the full payload. Replaying the same raw file with the fixed parser produced all 199 replacement events with zero issues. The capture-time summary retains its original pre-fix diagnostic count; raw data was not altered.

The raw input SHA-256 is `7b3f9b3da6c251b34d97c13e0b9b5a4d7dc887637f016aef6a1a4636072e0124`. A second offline replay with the fixed code produced byte-identical events, features, CSV, errors, and summary files.

## Phase 1 review

The current `structural-rules-v1` screen processed all 2,732 features and raised 73 rows (2.67%):

| Rule | Matching rows |
| --- | ---: |
| `many_inputs` | 43 |
| `many_outputs` | 26 |
| `repeated_output_values` | 4 |
| Total flagged rows | 73 |

The counts sum to 73, so these signals did not overlap in this sample. All 2,732 feature rows were independently recomputed from their archived added-transaction payloads and matched exactly. All 73 alert rows matched the documented rule predicates when checked directly. Among unflagged rows, 8 were just below the fan-in input threshold, 6 just below the fan-out output threshold, and 15 had two repeated positive outputs where the rule requires three.

These checks show that the parser, feature function, and rules agree with the stored data. They do not show that the flags are useful or harmful: the real observations have no independent benign/illicit labels, and reconnect gaps limit temporal coverage. For real rows, the review outcome remains `context_unknown` unless separate evidence exists. The synthetic lab's benign counterexamples confirm that the current features cannot distinguish benign batches or consolidation from other transactions with the same shape. See the [review rubric](../PHASE1_REVIEW_RUBRIC.md).

## Result and remaining scope

Phase 0's local capture, preservation, normalization, feature extraction, replay, lifecycle observation, and bounded-storage measurement criteria are complete for this prototype. The selected operating mode is full-detail capture for bounded experiments, with explicit gaps, caps, and no claim of full history.

Phase 1's explainable structural baseline, versioned outputs, controlled cases, threshold sweep, and observational run-through are complete. Calibration or real-world detection quality is not claimed; that needs an independent labeling process and held-out data, which belongs to later evaluation work. Kafka, databases, APIs, dashboards, and ML models are also later milestones, not unfinished requirements of these two local stages.
