# Exercise 11: tracing one transaction end to end

The goal is to follow one stored result back to the raw line it came from, and to prove the stored features can be rebuilt from that line. **Provenance** is the recorded answer to "where did this value come from?". In BlockPulse it is a chain of IDs: a score row names its event, the event names its source message, and the message is a numbered line in a capture file. A **replay check** recomputes a result from the archived input and compares it with what was stored. This is the reason raw captures are kept after processing.

## Run it

The archive half of the trace needs no database. Process the bundled synthetic fixture, then ask `trace.py` to find a transaction's raw messages and recompute its features:

```powershell
$py = ".\.venv\Scripts\python.exe"
& $py -m blockpulse process examples/fixtures/mempool_messages.jsonl --output data/learning/ex11/processed
& $py -c "import json; from pathlib import Path; from blockpulse.trace import find_archived_features; print(json.dumps(find_archived_features(Path('examples/fixtures/mempool_messages.jsonl'), {'synthetic-demo:2', 'synthetic-demo:3', 'synthetic-demo:4'}, 'a' * 64), indent=2))"
```

The three message IDs are the ones the fixture's events give for transaction `aaaa…` (see the table in [Exercise 9](09_EVIDENCE_VS_PROJECTION.md)). In the full command they come from Postgres instead.

The full trace needs Postgres. Prerequisite, from [guide 04](../guides/04-postgres-api-dashboard.md): `pip install -e ".[m2,dev]"` and `docker compose --profile m2 up -d --wait postgres`. Then:

```powershell
powershell -File scripts/demo_m2.ps1
```

The script creates the database `blockpulse_demo`, runs process, detect, `load-results` and `load-events` on the fixture, and finishes with `blockpulse trace <txid> --capture examples/fixtures/mempool_messages.jsonl --brief`.

## Open the output

The offline command printed this (line breaks condensed; the `capture_file` line, which holds your own path, is left out):

```text
  "messages": {
    "synthetic-demo:2": { "line": 2, "received_at": "2026-10-08T08:00:02.000+00:00" },
    "synthetic-demo:3": { "line": 3, "received_at": "2026-10-08T08:00:03.000+00:00" },
    "synthetic-demo:4": { "line": 4, "received_at": "2026-10-08T08:00:04.000+00:00" }
  },
  "recomputed_features": {
    "n_in": 1, "n_out": 2, "value_out_sat": 100000, "fee_sat": 603, "vsize": 201,
    "fee_rate_sat_vb": 3.0, "max_equal_output_count": 1, "round_amount_fraction": 0.0,
    "rbf_signaled": null
  },
  "problem": null
```

Now open `data/learning/ex11/processed/features.jsonl` and find the row for the same txid (the two IDs are shortened here):

```text
{"txid":"aaaa…","event_id":"ab651b6a…","received_at":"2026-10-08T08:00:02.000+00:00","source_mode":"synthetic","feature_set_version":"transaction-v2","n_in":1,"n_out":2,"value_out_sat":100000,"fee_sat":603,"vsize":201,"fee_rate_sat_vb":3.0,"max_equal_output_count":1,"round_amount_fraction":0.0,"rbf_signaled":null}
```

The nine feature values are the same. One path went capture → `process` → file; the other went capture line 2 → `extract_features()` directly. This is the reproducibility rule from Exercise 1 (same input and same feature code give the same features), now checked across the whole pipeline. `rbf_signaled` is `null` because the fixture's inputs carry no sequence number; the value is unknown, and it is stored as unknown rather than as `false`.

From the demo script, the last lines should be a status line (lifecycle status, number of scores, the event types) followed by `archived features match stored features: True`. I could not run the script in the session that wrote this page, so check that against your own output.

## Read one function

Open [trace.py](../../src/blockpulse/trace.py). It is 64 lines and has two functions.

`trace_transaction()` asks Postgres three questions about a txid: its `transactions` row, its `scores`, and its `lifecycle_events`. Then it hands the events' `source_message_id` values to `find_archived_features()`. Trace `fee_sat` through that second function:

```python
for event in parse_record(record).events:
    if event["event_type"] == "added" and event["txid"] == txid and found["recomputed_features"] is None:
        try:
            found["recomputed_features"] = extract_features(event["payload"])
        except ValueError as error:
            found["problem"] = f"archived payload cannot produce features: {error}"
```

The capture file is scanned once. Lines whose `message_id` is not in the wanted set are skipped. For a wanted line, `parse_record()` (the same parser `process` uses) splits the message into events, and the first `added` event for this txid supplies the payload. `extract_features()` (the same function as in Exercise 1) reads `fee` from that payload and returns `fee_sat: 603`. Nothing is fetched from the network.

Back in `trace_transaction()`, the comparison is strict: for every stored score, the stored features restricted to `FEATURE_NAMES` must equal the recomputed dictionary. The result is `True`, `False`, or `None` when there is nothing to compare. In `cli.py`, `False` becomes exit code 4.

Notice that the trace reuses the pipeline's own parser and feature function. A separate "checking" implementation could agree with itself and still differ from what `process` does.

## Change one small thing

Simulate an archive that no longer matches. Make a copy of the fixture with one fee changed (never edit the fixture itself):

```powershell
& $py -c "from pathlib import Path; t = Path('examples/fixtures/mempool_messages.jsonl').read_text(encoding='utf-8'); Path('data/learning/ex11/tampered.jsonl').write_text(t.replace('603', '703', 1), encoding='utf-8', newline='\n')"
& $py -c "from pathlib import Path; from blockpulse.trace import find_archived_features; print(find_archived_features(Path('data/learning/ex11/tampered.jsonl'), {'synthetic-demo:2'}, 'a' * 64)['recomputed_features'])"
```

```text
{'n_in': 1, 'n_out': 2, 'value_out_sat': 100000, 'fee_sat': 703, 'vsize': 201, 'fee_rate_sat_vb': 3.497512, 'max_equal_output_count': 1, 'round_amount_fraction': 0.0, 'rbf_signaled': None}
```

One changed number in the raw line moved two features. With the demo database loaded, run `blockpulse trace` with `--capture data/learning/ex11/tampered.jsonl --brief` and then `$LASTEXITCODE`: from the code, it should report `False` and exit with 4.

Second experiment: ask for a message that is not in the file by adding `'synthetic-demo:99'` to the set. The `problem` field becomes `1 source message(s) not found in this capture file`. A trace that cannot find its evidence says so instead of reporting a match.

Then rerun the tests that cover this path: `& $py -m pytest -q tests/test_m2_end_to_end.py` (it needs `BLOCKPULSE_TEST_DSN`, see guide 04, and skips without it).

## Question to carry forward

The replay check proves the stored features match the archived line. It does not prove the archived line is what the network sent. Which recorded value would let you detect that a capture file was altered after it was written, and where in the pipeline is that value already computed? (Look at `input_sha256` in a `process` summary, and at Exercise 12.)

## What was run for this page

- Ran (Linux, 2026-11-24): `blockpulse process` on the fixture, the `find_archived_features` one-liner, the tampered-copy experiment and the missing-message experiment (written under `/tmp`). The outputs above are copied from those runs.
- Not run (no Postgres in that session): `scripts/demo_m2.ps1`, `blockpulse trace`, and `tests/test_m2_end_to_end.py` (it skips without a database). The expected `trace` output and exit code are read from `cli.py` and `trace.py`.
