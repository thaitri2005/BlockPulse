# Source connectivity and first live-data verification

Date: 2026-10-08. All capture timestamps are UTC.

## Symptom

The default capture command repeatedly reported an opening-handshake timeout. The supplied 60-second run made four attempts, established zero connections, and saved zero messages. Increasing the opening timeout to 30 seconds in an earlier run did not resolve it.

## Layer-by-layer diagnosis

- No system HTTP/WebSocket proxy was returned by Python's proxy discovery.
- `mempool.space` resolved in approximately 0.031 seconds to seven IPv4 addresses in `103.165.192.202` through `103.165.192.208`.
- Direct TCP connection attempts to three returned addresses (`.203`, `.206`, `.202`) each timed out after five seconds. TLS and HTTP were never reached in those probes.
- A GitHub TCP connection succeeded in approximately 0.172 seconds, so general outbound TCP connectivity was available.
- `node201.sg1.mempool.space` resolved, but its TCP connection also timed out.
- `node203.sv1.mempool.space` resolved to `103.99.170.203` and connected over TCP in approximately 0.422 seconds.

The regional node names were discovered through the project's [public service monitoring page](https://monitoring.mempool.space/). We did not change system DNS, edit a hosts file, disable certificate verification, or change the subscription to obtain a successful connection.

The evidence isolates the failure to reaching the default route's servers from this machine. It does not establish whether the underlying cause is an ISP route, firewall, remote filtering, or a server problem.

## Working capture

```powershell
.\.venv\Scripts\python.exe -m blockpulse capture --url wss://node203.sv1.mempool.space/api/v1/ws --duration 30 --open-timeout 10 --max-reconnects 0 --max-messages 5
```

Measured result:

| Measurement | Result |
| --- | --- |
| Run ID | `d3ce7834bd1f4059b586a92e089efb76` |
| Started | 2026-10-08 06:23:32.085 UTC |
| Subscribed | 2026-10-08 06:23:33.837 UTC |
| Total duration | 6.968 seconds |
| Connection attempts / successful connections | 1 / 1 |
| Messages saved | 5 |
| Added transactions / unique IDs | 22 / 22 |
| Parse issues / sequence discontinuities | 0 / 0 |
| Payload bytes | 160,902 |
| JSONL file bytes including envelope/escaping | 172,904 |
| Stop reason | Message limit |

The successful run used the existing capture implementation and standard TLS hostname/certificate validation. No application-code change was needed for the connection; the existing `--url` option selected the reachable endpoint.

## Offline processing and replay

Local capture: `data/raw/regional-check-01/messages.jsonl`.

Processed output: `data/processed/regional-check-01/`.

Replay output: `data/processed/regional-replay-01/`.

- All 22 transactions contained the required `vin`, `vout`, `weight`, and `fee` inputs.
- All 22 produced feature rows; there were zero incomplete inputs, conflicts, or diagnostics.
- Events, JSONL features, CSV features, errors, and summary files were byte-identical across both processing runs.
- Capture SHA-256: `26ca25d1dcb95f0dbfd8514d18cc97f0a6afdc61898ad7f6930bfb390e0ffa54`.

These local data files remain ignored by Git. This document retains the experiment's measurements and provenance.

## Limits and next step

This short sample verifies the real source-to-file-to-feature path for added transactions. It does not establish complete coverage, sustainable provider limits, long-run throughput, or the shape of removal, mining, and replacement events. Regional host availability can change; keep the selected URL visible in each capture's configuration.

Next: use the reachable endpoint for a longer bounded capture and inspect the additional lifecycle events and payload-size distribution.
