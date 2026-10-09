# Phase 0: capture and replay the data path

Implemented path:

```text
WebSocket -> messages.jsonl -> normalized events -> shared features -> JSONL + CSV
```

The capture and processing steps are separate commands. Processing works offline and never fetches transaction details. An ID-only or incomplete transaction is retained and reported as needing more data.

## Setup and commands

Requires Python 3.11+ and the pinned `websockets` dependency. From the repository root on Windows:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

The workspace's `.venv` is already prepared. It reuses the available system packages for this development session; a fresh checkout can use the clean setup above.

Start with the included synthetic recording:

```powershell
.\.venv\Scripts\python.exe -m blockpulse process examples/fixtures/mempool_messages.jsonl
```

Capture for up to one minute:

```powershell
.\.venv\Scripts\python.exe -m blockpulse capture --duration 60 --url wss://node203.sv1.mempool.space/api/v1/ws --output data/raw/first-capture
```

Process that capture:

```powershell
.\.venv\Scripts\python.exe -m blockpulse process data/raw/first-capture/messages.jsonl
```

The explicit regional URL is the endpoint successfully tested from this machine. Omitting it uses `mempool.space`, whose resolved servers timed out on TCP connection during our checks. This is an endpoint override for this run, not a change to system DNS or TLS verification. See the [connectivity report](experiments/2026-10-08-source-connectivity.md).

For a ten-minute experiment, increase `--duration` to `600`. The byte, message, and retry limits still apply. Omit `--output` to create a unique timestamped run directory, or supply a new directory that does not already exist. No command overwrites a previous run.

To compare the IDs-only subscription:

```powershell
.\.venv\Scripts\python.exe -m blockpulse capture --duration 60 --url wss://node203.sv1.mempool.space/api/v1/ws --subscription track-mempool-txids
```

Use `python -m blockpulse capture --help` or `process --help` through the project interpreter for all options. After installation, the equivalent `blockpulse` entry point is available in `.venv/Scripts`.

## Capture behavior

| Setting | Default |
| --- | --- |
| Endpoint | `wss://mempool.space/api/v1/ws` |
| Subscription | `track-mempool` |
| Duration | 60 seconds |
| Saved-message limit | 10,000 |
| `messages.jsonl` byte limit | 50 MiB |
| Maximum received message | 4 MiB |
| Reconnect attempts | 3 after the first attempt |
| Opening handshake timeout | 15 seconds |
| Transport ping interval / timeout | 20 / 20 seconds |

The first reached limit stops capture. Connection opening, receiving, and reconnect waits share the duration budget; closing the connection and interpreter/network cleanup may add time. Ctrl+C requests an orderly stop with a final summary. Abrupt process termination or power loss cannot guarantee a final summary or flushed disk contents.

The pinned WebSocket client supplies TLS verification, protocol pings, bounded incoming-message buffering, and support for configured system proxies. Capture applies its own finite retry count and backoff. [Client reference](https://websockets.readthedocs.io/en/15.0.1/reference/asyncio/client.html).

Each captured message stores its original text, receipt time in UTC, run/session/message identity, and schema version. Unexpected binary data is preserved as base64. The source payload is not rewritten. This records complete WebSocket messages, not individual TCP packets or fragmented WebSocket frames.

The capture directory contains:

| File | Contents |
| --- | --- |
| `messages.jsonl` | Original received messages inside the capture envelope |
| `connections.jsonl` | Connection attempts, subscriptions, errors, sequence discontinuities, and stop reason |
| `summary.json` | Configuration, duration, message/event counts, unique IDs, sizes, rates, and uncovered intervals |

Time spent disconnected is recorded as a gap. Reconnecting does not backfill history. Sequence changes are recorded as possible gaps, repeats, or resets; the script does not infer a precise number of lost transactions from them. Unique IDs and event counts describe this provider's observed data only.

Raw message persistence happens before parsing. Invalid JSON and unexpected payload shapes therefore remain available for inspection. The summary distinguishes saved messages from lifecycle event entries: one message can contain many events.

## Offline processing behavior

The parser recognizes the `mempool-transactions` and `mempool-txids` envelopes and their `added`, `removed`, `mined`, and `replaced` arrays. It accepts IDs or objects containing a transaction ID and retains unsupported entry shapes with a diagnostic. The published provider examples are the initial contract reference, not a guarantee about any future deployment. [Mempool API examples](https://github.com/mempool/mempool/blob/master/frontend/src/app/docs/api-docs/api-docs-data.ts).

For the observed replacement object shape, the normalized `replaced` event carries the old transaction in `txid` and the new transaction in `replacement_txid`, while preserving the original replacement payload.

Normalization uses the recorded receipt time as event time. Raw source fields such as `firstSeen` remain in the payload and do not silently change that definition.

The processed directory contains:

| File | Contents |
| --- | --- |
| `events.jsonl` | One normalized record per supported lifecycle entry, including repeated observations |
| `features.jsonl` | One valid feature record per unique added transaction ID |
| `features.csv` | The same feature records in a table suitable for inspection |
| `errors.jsonl` | Malformed lines, missing fields, unsupported shapes, and identity conflicts |
| `summary.json` | Input checksum, coverage of required fields, event counts, skipped/duplicate records, and output count |

Duplicate capture records with the same message identity are ignored after their first occurrence. Different content under the same message identity produces an explicit conflict. Repeated transactions with equivalent features produce one feature row; conflicting features are reported and the first valid row is retained. An incomplete observation does not prevent a later complete observation from producing features.

All four lifecycle event types are retained, but only `added` transactions feed feature extraction. Confirmations or removals do not erase or recompute previously observed structural features. There is no inferred replacement link when the source does not provide a recognized one.

The original `transaction-v1` feature set contains input count, output count, summed output value, fee, virtual size, and fee rate. Current processing emits `transaction-v2`, which adds equal-positive-output count, round-amount fraction, and an RBF-sequence observation (which can be unknown when data is missing). Amounts are integer satoshis. Missing or invalid required values are errors rather than zero-valued features. Coinbase records are rejected. The package and original one-transaction exercise call the same implementation. See [Phase 1](PHASE1.md) for the added fields and screening behavior.

Processing a fixed input with unchanged code produces identical event, feature, error, and summary files when the input path is unchanged. The summary includes the source's absolute path, so copying the input elsewhere changes that provenance field. Processing targets short local captures; its deduplication sets grow with unique message/transaction counts. Larger archives need a separate storage design.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Capture observed lifecycle entries, or processing wrote at least one feature row; inspect the summary for errors and partial coverage |
| 1 | File/configuration/runtime error |
| 2 | Invalid command-line arguments |
| 3 | Capture produced no lifecycle entries, or processing produced no usable features |
| 130 | Interrupted by the operator |

An empty capture is not replaced by synthetic events. A successful command with some data does not mean the dataset is complete or error-free.

## Tests and verification

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

To include the local WebSocket server/client integration test in an environment that permits loopback sockets:

```powershell
$env:BLOCKPULSE_SOCKET_TESTS = '1'
.\.venv\Scripts\python.exe -m pytest -q
Remove-Item Env:BLOCKPULSE_SOCKET_TESTS
```

The synthetic demo intentionally includes duplicate additions, all four lifecycle kinds, an ID-only addition, a control message, and malformed JSON. Expected result: six messages, seven lifecycle entries, two feature rows, one duplicate added transaction, one incomplete added transaction, and two diagnostics. These are test fixtures, not actual Bitcoin network observations.

Verification on 2026-10-08:

- All 30 tests passed with the socket test enabled at the Phase 0 boundary, including a real loopback WebSocket round trip, interrupted-capture finalization, command-line exit behavior, and protection against overwriting prior outputs.
- Processing the synthetic capture twice produced identical output files.
- Python syntax, documentation links, and Git whitespace checks passed.
- Initial live runs against the default hostname timed out and saved zero messages with explicit failure summaries. Follow-up diagnostics found TCP timeouts on three DNS-returned servers, before TLS or WebSocket negotiation.
- A regional endpoint, `node203.sv1.mempool.space`, connected with normal certificate verification and delivered five messages containing 22 added transactions in a 6.968-second run. All 22 produced features with no missing inputs or parse issues. Processing the real capture twice produced byte-identical output files.
- The follow-up ten-minute capture and corrected replacement schema are detailed in the [validation report](experiments/2026-10-09-phase0-phase1-validation.md). After the parser fix, two offline replays were byte-identical and all 44 tests passed with the loopback socket test enabled.

The current Phase 0 validation, including a ten-minute bounded capture, lifecycle observations, host snapshot, size distribution, and deterministic replay, is recorded in the [2026-10-09 validation report](experiments/2026-10-09-phase0-phase1-validation.md). The source still has reconnect gaps, and the measured active-burst storage rate is not a long-run forecast. REST enrichment and Parquet storage remain later additions if evidence justifies them.
