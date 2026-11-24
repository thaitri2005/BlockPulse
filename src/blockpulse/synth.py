"""Generate a synthetic capture session: realistic-shaped mempool messages for drills and load tests.

A capture made here has the same envelope and payload layout as a recorded one, so every stage (replay, enrich,
process, screen, lake, load) accepts it. It exists because drills need thousands of messages and the recorded
sessions are local evidence that isn't in the repository. Everything is labelled synthetic (`source_mode`), the
transactions were never broadcast, and results on them describe pipeline behaviour only, never the network.
"""

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from blockpulse.storage import CAPTURE_NAME, write_summary
from blockpulse.sweep import _build, _payment_values

SYNTH_SCHEMA = 1
START = datetime(2026, 1, 1, tzinfo=timezone.utc)  # fixed, so the same seed gives byte-identical files


def _transaction(rng: random.Random) -> dict:
    """One transaction with a plausible mix of shapes: mostly ordinary payments, some batches and consolidations."""
    shape = rng.random()
    if shape < 0.80:
        n_in, n_out = rng.choice((1, 1, 2)), 2
    elif shape < 0.92:
        n_in, n_out = 1, rng.randint(3, 40)       # a batch payment
    else:
        n_in, n_out = rng.randint(3, 30), 1       # a consolidation
    tx = _build(rng, n_in, _payment_values(rng, n_out), 1.0 + rng.random() * 20)
    return {"txid": f"{rng.getrandbits(256):064x}", "version": 2, "locktime": 0, **tx, "status": {"confirmed": False}}


def generate_capture(output: Path, *, messages: int, seed: int = 7, transactions_per_message: int = 4) -> dict:
    """Write `output/messages.jsonl` and `output/summary.json`. Deterministic for the same arguments."""
    if type(messages) is not int or messages <= 0:
        raise ValueError("messages must be a positive integer")
    if type(transactions_per_message) is not int or transactions_per_message <= 0:
        raise ValueError("transactions_per_message must be a positive integer")
    output = Path(output)
    if output.exists():
        raise ValueError(f"Output already exists: {output}")
    output.mkdir(parents=True)
    rng = random.Random(f"synth-{seed}")
    run_id = f"synthetic-{seed}-{messages}"
    transactions = 0
    with (output / CAPTURE_NAME).open("w", encoding="utf-8", newline="\n") as handle:
        for number in range(1, messages + 1):
            added = [_transaction(rng) for _ in range(transactions_per_message)]
            transactions += len(added)
            payload = {"mempool-transactions": {"sequence": number, "added": added, "removed": [], "mined": [], "replaced": []}}
            envelope = {
                "schema_version": 1, "run_id": run_id, "session_id": run_id, "message_id": f"{run_id}:{number}",
                "received_at": (START + timedelta(milliseconds=250 * number)).isoformat(timespec="milliseconds"),
                "source_mode": "synthetic", "message_type": "text",
                "raw_text": json.dumps(payload, separators=(",", ":")),
            }
            handle.write(json.dumps(envelope, separators=(",", ":")) + "\n")
    # The same fields a recorded capture's summary has, so the lake's capture_sessions contract accepts the session.
    elapsed = 0.25 * messages
    summary = {
        "schema_version": 1, "synth_schema": SYNTH_SCHEMA, "run_id": run_id, "session_id": run_id, "source_mode": "synthetic",
        "seed": seed, "started_at": START.isoformat(timespec="milliseconds"),
        "ended_at": (START + timedelta(seconds=elapsed)).isoformat(timespec="milliseconds"), "elapsed_seconds": elapsed,
        "stop_reason": "generated", "config": {"url": None}, "connections": 0, "connection_errors": 0, "gaps": [],
        "messages_received": messages, "messages_saved": messages, "transactions": transactions,
        "event_counts": {"added": transactions, "removed": 0, "mined": 0, "replaced": 0},
        "note": "Synthetic capture for drills and load tests. No transaction here was broadcast or observed.",
    }
    write_summary(output / "summary.json", summary)
    return summary
