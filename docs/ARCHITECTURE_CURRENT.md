# How BlockPulse works right now

BlockPulse watches mempool messages, saves them, turns transaction data into a few numbers, then checks those numbers with simple rules. It runs as three data commands, plus a local learning lab:

```text
capture -> process -> detect
```

`blockpulse lab` runs controlled synthetic cases through the same rules and sweeps a few thresholds; it makes no network calls.

## 1. `capture`: collect messages

`blockpulse capture` connects to the mempool.space WebSocket and subscribes to transaction updates. Python's `asyncio` and the `websockets` package handle the connection.

Every message is saved as received in `messages.jsonl`. The capture also writes `connections.jsonl` and `summary.json`, which record connection attempts, gaps, limits, and counts. Capture is the only step that needs the internet. If it disconnects, it records the gap; it cannot recover missed updates.

## 2. `process`: make events and features

`blockpulse process <messages.jsonl>` reads the saved messages offline. It recognizes transaction updates and writes them as normalized rows in `events.jsonl`.

For complete added transactions, it calculates features such as input count, output count, fee, transaction size, repeated output amounts, and whether the inputs signal replaceability. These go into `features.jsonl` and `features.csv`. Incomplete or invalid data is reported in `errors.jsonl`. `summary.json` gives the counts and a checksum of the input.

## 3. `detect`: apply simple rules

`blockpulse detect <features.jsonl>` reads those features offline and checks three patterns: many outputs with few inputs, many inputs with few outputs, or repeated output amounts. Matching rows go to `alerts.jsonl`; all screened rows and explanations go to `screened.jsonl` and `screened.csv`.

A match only means the transaction has that shape. It does not mean wrongdoing. The current rules are a basic starting point, not a trained or tested fraud detector.

## Technologies and what they do

- **Python** runs the commands and processing logic.
- **WebSockets** receive live updates from the mempool source.
- **JSONL** stores one record per line, so the data is easy to inspect and replay.
- **CSV** makes feature and result tables easy to open in a spreadsheet.
- **pytest** checks that parsing, calculations, rules, and replay behave as expected.

Right now, data is stored in local files. Kafka, a database, an API, dashboards, and machine learning are future parts of the project, not things this version uses.

The saved sample had 22 transactions: all 22 were processed and screened, and 2 matched a rule. That confirms the steps connect; it does not tell us whether the rules are good. See [Phase 1](PHASE1.md) for how to run the screen.
