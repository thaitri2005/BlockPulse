import json
from urllib.error import HTTPError
from unittest.mock import patch

import pytest

from blockpulse.enrichment import EnrichmentRequestError, RateLimitedEnricher, fetch_transaction


def envelope(*entries):
    return {
        "schema_version": 1, "run_id": "run", "session_id": "session",
        "message_id": "run:1", "received_at": "2026-10-09T00:00:00+00:00",
        "source_mode": "synthetic", "message_type": "text",
        "raw_text": json.dumps({"mempool-transactions": {"sequence": 1, "added": list(entries)}}),
    }


def detail(txid):
    return {"txid": txid, "vin": [{"sequence": 1}], "vout": [{"value": 42}], "weight": 400, "fee": 10}

import pytest


def test_complete_transaction_does_not_call_api():
    tx = detail("a" * 64)
    row, errors = RateLimitedEnricher(fetch=lambda txid: pytest.fail("unexpected fetch")).process(envelope(tx))
    assert not errors
    assert row["events"][0]["enrichment_status"] == "not_needed"
    assert row["enrichment_summary"]["requests"] == 0


def test_incomplete_transaction_is_fetched_and_missing_fields_are_filled():
    txid = "b" * 64
    calls = []
    row, errors = RateLimitedEnricher(fetch=lambda txid: calls.append(txid) or detail(txid)).process(envelope({"txid": txid}))
    assert calls == [txid]
    assert not errors
    assert row["events"][0]["payload"]["fee"] == 10
    assert row["events"][0]["enrichment_status"] == "fetched"
    assert row["enrichment_summary"]["enriched"] == 1


def test_enrichment_rate_limit_and_budget_are_explicit():
    first, second = "c" * 64, "d" * 64
    now = [0.0]
    sleeps = []
    calls = []
    def sleep(duration):
        sleeps.append(duration)
        now[0] += duration
    enricher = RateLimitedEnricher(
        requests_per_second=2, max_requests=1,
        fetch=lambda txid: calls.append(txid) or detail(txid),
        clock=lambda: now[0], sleep=sleep,
    )
    row, errors = enricher.process(envelope({"txid": first}, {"txid": second}))
    assert calls == [first]
    assert len(errors) == 1 and errors[0]["kind"] == "budget_exhausted"
    assert [event["enrichment_status"] for event in row["events"]] == ["fetched", "budget_exhausted"]


def test_enrichment_rejects_mismatched_transaction_response():
    txid = "e" * 64
    row, errors = RateLimitedEnricher(fetch=lambda _txid: detail("f" * 64)).process(envelope({"txid": txid}))
    assert errors[0]["kind"] == "txid_mismatch"
    assert row["events"][0]["enrichment_status"] == "txid_mismatch"


def test_http_429_is_reported_as_rate_limited():
    error = HTTPError("https://mempool.space/api/tx", 429, "Too Many Requests", {}, None)
    with patch("blockpulse.enrichment.urlopen", side_effect=error):
        with pytest.raises(EnrichmentRequestError) as raised:
            fetch_transaction("a" * 64)
    assert raised.value.kind == "rate_limited"
