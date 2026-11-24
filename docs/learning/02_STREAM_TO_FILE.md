# Exercise 2: from a stream to a file

The goal is to understand what `capture` writes to disk and why. A **WebSocket** is a long-lived connection over which the server pushes messages as they happen. Each message is saved inside an **envelope**: a small record of fields we add around the provider's text. The file format is **JSONL**, one JSON object per line. By the end you should be able to say which fields are ours, which are the provider's, and what a reconnect does and does not give back.

The recorded captures under `data/raw/` exist only on the owner's machine. This exercise uses a generated capture, so it works anywhere. Generated data is labelled `source_mode: synthetic`; nothing in it was broadcast or observed.

## Run it

From PowerShell in the project folder:

```powershell
.\.venv\Scripts\python.exe -m blockpulse synth-capture --output data/learning/synth-200 --messages 200
```

The command prints its summary:

```text
  "run_id": "synthetic-7-200",
  "source_mode": "synthetic",
  "seed": 7,
  "messages_saved": 200,
  "transactions": 800,
```

The same arguments always produce a byte-identical file. Exercises 3 to 8 reuse this folder; the command refuses to overwrite it, so run it once.

A live capture uses the same envelope. It needs the regional endpoint from this network (see the [capture guide](../guides/01-capture-and-process.md)), and it is optional here:

```powershell
.\.venv\Scripts\python.exe -m blockpulse capture --duration 60 --url wss://node203.sv1.mempool.space/api/v1/ws --output data/raw/learning-capture
```

## Open the output

```powershell
$line = Get-Content data/learning/synth-200/messages.jsonl -TotalCount 1 | ConvertFrom-Json
$line | Select-Object schema_version, run_id, session_id, message_id, received_at, source_mode, message_type
$line.raw_text.Substring(0, 160)
```

The first line, with `raw_text` cut short:

```text
schema_version : 1
run_id         : synthetic-7-200
session_id     : synthetic-7-200
message_id     : synthetic-7-200:1
received_at    : 2026-01-01T00:00:00.250+00:00
source_mode    : synthetic
message_type   : text
raw_text       : {"mempool-transactions":{"sequence":1,"added":[{"txid":"9aa3f488b851d5ab...
```

Everything except `raw_text` is the envelope. `raw_text` is the provider's message kept as a string, not as parsed JSON, so the original bytes are never rewritten. Inside it, `sequence` is the provider's counter and `added` is a list of transactions that entered the mempool.

A live capture also writes `connections.jsonl`, a journal of connection attempts. The generated capture has no connections, so it has no journal. To see one offline, I ran `capture()` with the scripted connection from `tests/test_capture.py`, which delivers two messages, drops, and reconnects. The journal it wrote (session IDs shortened):

```text
{"record_type":"connecting","at":"...34.167+00:00","session_id":"69b0..."}
{"record_type":"subscribed","at":"...34.167+00:00","session_id":"69b0...","subscription":"track-mempool"}
{"record_type":"sequence_discontinuity","session_id":"69b0...","channel":"mempool-transactions","previous":10,"observed":12,"interpretation":"Possible gap, repeat, or reset; history not reconstructed"}
{"record_type":"connection_error","at":"...34.168+00:00","session_id":"69b0...","error_type":"OSError","error":"simulated disconnect"}
{"record_type":"connecting","at":"...35.299+00:00","session_id":"bb75..."}
{"record_type":"subscribed","at":"...35.299+00:00","session_id":"bb75...","subscription":"track-mempool"}
{"record_type":"capture_stopped","at":"...35.299+00:00","reason":"message_limit"}
```

A **reconnect** is a new connection with a new `session_id`. A **gap** is the time between losing one connection and subscribing on the next. That run's `summary.json` recorded the gap from `...34.168` to `...35.299` with `"history_recovered": false`.

## Read one function

Open `src/blockpulse/capture.py` and find `capture()`. Trace `received_at`. It is set once, when the message arrives:

```python
record = {
    "schema_version": 1,
    "run_id": run_id,
    "session_id": session_id,
    "message_id": f"{run_id}:{summary['messages_received']}",
    "received_at": utc_now(),
    "source_mode": "ws",
    "message_type": "text" if isinstance(message, str) else "binary",
}
```

The next lines add `raw_text` and call `raw.append(record, ...)`. Only after the line is on disk does `parse_record(record)` run. So a message that fails to parse is still saved.

Now open `src/blockpulse/events.py` and find `parse_record()`. Each transaction in `added` becomes one event, and the envelope's `received_at` is copied into the event's `event_time`, with `"event_time_basis": "received_at"`. The pipeline's clock is the moment we received a message. The provider's own timestamps stay in the payload untouched.

## Change one small thing

Make a one-line capture and break its envelope version by hand:

```powershell
New-Item -ItemType Directory data/learning/ex02-one | Out-Null
Get-Content data/learning/synth-200/messages.jsonl -TotalCount 1 | Set-Content data/learning/ex02-one/messages.jsonl
```

Open `data/learning/ex02-one/messages.jsonl`, change `"schema_version":1` at the start to `"schema_version":2`, save, then:

```powershell
.\.venv\Scripts\python.exe -m blockpulse process data/learning/ex02-one/messages.jsonl --output data/learning/ex02-one-out
$LASTEXITCODE
Get-Content data/learning/ex02-one-out/errors.jsonl
```

When I ran this the summary showed `"features_written": 0`, the exit code was 3, and `errors.jsonl` held:

```text
{"line":1,"kind":"schema","detail":"Unsupported capture envelope/schema_version"}
```

The parser refuses an envelope version it does not know instead of guessing. The raw line is still in the file. Then rerun the tests for this area:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_capture.py tests/test_events.py
```

Read `test_disconnect_reconnect_and_sequence_discontinuity` in `tests/test_capture.py`; it is the scenario shown above.

## Question to carry forward

Why can't a reconnect recover the transactions we missed? Start from what the subscription message asks the server for, and what the server would need to know about us to resend the gap.

## What was run

Verified on Linux on 2026-11-24 with the project's interpreter: `synth-capture`, the one-line `process` experiment (exit code 3), and `pytest tests/test_capture.py tests/test_events.py` (11 and 5 passed). The journal excerpt came from `capture()` driven by the test's scripted connection, not from a network. The live `capture` command was not run here, and the PowerShell file commands were checked through their Linux equivalents only.
