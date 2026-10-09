from copy import deepcopy
import json

import pytest


@pytest.fixture
def transaction():
    return {
        "txid": "a" * 64,
        "vin": [{"txid": "b" * 64, "vout": 0, "prevout": {"value": 100603}, "sequence": 4294967293}],
        "vout": [{"value": 60000}, {"value": 40000}],
        "weight": 801,
        "fee": 603,
        "status": {"confirmed": False},
    }


@pytest.fixture
def record_factory():
    def make(payload, number=1, **overrides):
        result = {
            "schema_version": 1,
            "run_id": "synthetic-test",
            "session_id": "synthetic-session",
            "message_id": f"synthetic-test:{number}",
            "received_at": "2026-10-08T08:00:00.000+00:00",
            "source_mode": "synthetic",
            "message_type": "text",
            "raw_text": json.dumps(deepcopy(payload)),
        }
        result.update(overrides)
        return result
    return make
