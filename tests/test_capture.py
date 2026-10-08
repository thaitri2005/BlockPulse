import asyncio
import json

import pytest

from blockpulse.capture import CaptureConfig, capture


class ScriptedConnection:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.subscriptions = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def send(self, payload):
        self.subscriptions.append(json.loads(payload))

    async def recv(self):
        item = next(self.messages, None)
        if isinstance(item, Exception):
            raise item
        if item is None:
            await asyncio.sleep(10)
            raise AssertionError("Deadline did not stop idle receive")
        return item


def test_disconnect_reconnect_and_sequence_discontinuity(tmp_path, transaction):
    text = lambda n: json.dumps({"mempool-transactions": {"sequence": n, "added": [transaction]}})
    first = ScriptedConnection([text(10), text(12), OSError("simulated disconnect")])
    second = ScriptedConnection([text(80)])
    connections = iter([first, second])
    summary = asyncio.run(capture(CaptureConfig(duration=5, max_messages=3), tmp_path / "capture", connector=lambda *a, **kw: next(connections)))
    assert summary["connections"] == 2
    assert summary["messages_saved"] == 3
    assert summary["event_counts"]["added"] == 3
    assert summary["unique_transaction_ids"] == 1
    assert summary["sequence_discontinuities"] == 1
    assert summary["connection_errors"] == 1
    assert summary["stop_reason"] == "message_limit"
    assert first.subscriptions == second.subscriptions == [{"track-mempool": True}]
    records = [json.loads(line) for line in (tmp_path / "capture/messages.jsonl").read_text().splitlines()]
    assert records[0]["raw_text"] == text(10)
    assert records[0]["session_id"] != records[2]["session_id"]
    assert all(not gap["history_recovered"] for gap in summary["gaps"])


def test_idle_connection_honors_deadline(tmp_path):
    summary = asyncio.run(capture(CaptureConfig(duration=0.05), tmp_path / "idle", connector=lambda *a, **kw: ScriptedConnection([])))
    assert summary["stop_reason"] == "duration"
    assert summary["messages_saved"] == 0
    assert summary["elapsed_seconds"] < 1


def test_byte_limit_and_malformed_payload_preservation(tmp_path):
    connection = ScriptedConnection(["{invalid", b"binary", "x" * 2000])
    summary = asyncio.run(capture(CaptureConfig(duration=2, max_bytes=1200), tmp_path / "limited", connector=lambda *a, **kw: connection))
    assert summary["stop_reason"] == "byte_limit"
    assert summary["raw_file_bytes"] <= 1200
    assert summary["messages_saved"] == 2
    assert summary["messages_with_parse_issues"] == 2
    assert summary["messages_received"] == 3


def test_failed_connection_has_summary_and_no_fake_events(tmp_path):
    def fail(*args, **kwargs):
        raise OSError("simulated offline")
    summary = asyncio.run(capture(CaptureConfig(duration=1, max_reconnects=0), tmp_path / "failed", connector=fail))
    assert summary["stop_reason"] == "retry_limit"
    assert summary["connections"] == 0
    assert not any(summary["event_counts"].values())
    assert (tmp_path / "failed/summary.json").exists()


def test_cancellation_finalizes_capture_summary(tmp_path):
    async def exercise():
        task = asyncio.create_task(capture(
            CaptureConfig(duration=10), tmp_path / "interrupted",
            connector=lambda *a, **kw: ScriptedConnection([]),
        ))
        await asyncio.sleep(0.02)
        task.cancel()
        summary = await task
        assert summary["stop_reason"] == "interrupted"
        assert summary["messages_saved"] == 0
        saved = json.loads((tmp_path / "interrupted/summary.json").read_text())
        assert saved == summary
    asyncio.run(exercise())


@pytest.mark.parametrize("kwargs", [{"duration": 0}, {"duration": float("nan")}, {"max_bytes": -1}, {"max_reconnects": -1}, {"url": "https://example.com"}])
def test_invalid_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        CaptureConfig(**kwargs)
