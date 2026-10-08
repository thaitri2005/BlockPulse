"""Real loopback WebSocket test. Opt in where the sandbox permits sockets."""

import asyncio
import json
import os

import pytest
from websockets.asyncio.server import serve

from blockpulse.capture import CaptureConfig, capture
from blockpulse.process import process_capture


@pytest.mark.skipif(os.environ.get("BLOCKPULSE_SOCKET_TESTS") != "1", reason="Set BLOCKPULSE_SOCKET_TESTS=1 to enable loopback sockets")
def test_real_websocket_to_csv(tmp_path, transaction):
    async def exercise():
        async def handler(websocket):
            assert json.loads(await websocket.recv()) == {"track-mempool": True}
            await websocket.send(json.dumps({"mempool-transactions": {"sequence": 1, "added": [transaction]}}))
            await websocket.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            summary = await capture(CaptureConfig(url=f"ws://127.0.0.1:{port}", duration=3, max_messages=1), tmp_path / "live")
        assert summary["messages_saved"] == 1
        result = process_capture(tmp_path / "live/messages.jsonl", tmp_path / "processed")
        assert result["features_written"] == 1
        assert "fee_rate_sat_vb" in (tmp_path / "processed/features.csv").read_text()

    asyncio.run(exercise())
