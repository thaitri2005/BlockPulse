"""Bounded fallback enrichment for incomplete mempool transaction observations."""

import hashlib
import json
import math
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from blockpulse.events import parse_record
from blockpulse.features import extract_features

DEFAULT_API_BASE = "https://mempool.space/api"
REQUIRED_TRANSACTION_FIELDS = ("vin", "vout", "weight", "fee")


class EnrichmentRequestError(RuntimeError):
    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind


def fetch_transaction(txid: str, *, api_base=DEFAULT_API_BASE, timeout=10):
    """Fetch one public transaction record from the mempool.space REST API."""
    request = Request(
        f"{api_base.rstrip('/')}/tx/{quote(txid, safe='')}",
        headers={"User-Agent": "BlockPulse/0.1 bounded-enrichment"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except HTTPError as error:
        kind = "rate_limited" if error.code == 429 else "http_error"
        raise EnrichmentRequestError(kind, f"Transaction API returned HTTP {error.code}") from error
    except (URLError, TimeoutError, OSError, ValueError) as error:
        raise EnrichmentRequestError("request_failed", f"Transaction API request failed: {error}") from error


class RateLimitedEnricher:
    """Enrich only incomplete added transactions under a per-run request budget."""

    def __init__(
        self,
        *,
        api_base=DEFAULT_API_BASE,
        requests_per_second=1.0,
        max_requests=100,
        fetch=None,
        clock=time.monotonic,
        sleep=time.sleep,
    ):
        if type(requests_per_second) not in (int, float) or not math.isfinite(requests_per_second) or requests_per_second <= 0:
            raise ValueError("requests_per_second must be finite and greater than zero")
        if type(max_requests) is not int or max_requests <= 0:
            raise ValueError("max_requests must be a positive integer")
        self.api_base = api_base
        self.interval = 1.0 / requests_per_second
        self.max_requests = max_requests
        self.fetch = fetch or (lambda txid: fetch_transaction(txid, api_base=api_base))
        self.clock = clock
        self.sleep = sleep
        self.requests = 0
        self.last_request_at = None
        self.cache = {}

    def _fetch(self, txid):
        if txid in self.cache:
            return self.cache[txid], "cache"
        if self.requests >= self.max_requests:
            raise EnrichmentRequestError("budget_exhausted", "Per-run enrichment request budget exhausted")
        now = self.clock()
        if self.last_request_at is not None:
            delay = self.interval - (now - self.last_request_at)
            if delay > 0:
                self.sleep(delay)
        self.last_request_at = self.clock()
        self.requests += 1
        try:
            result = self.fetch(txid)
        except EnrichmentRequestError:
            raise
        except Exception as error:
            raise EnrichmentRequestError("request_failed", f"Transaction API request failed: {error}") from error
        if not isinstance(result, dict):
            raise EnrichmentRequestError("invalid_response", "Transaction API response was not an object")
        if result.get("txid") != txid:
            raise EnrichmentRequestError("txid_mismatch", "Transaction API response did not match requested txid")
        self.cache[txid] = result
        return result, "fetched"

    def process(self, envelope: dict):
        parsed = parse_record(envelope)
        output_events = []
        errors = [{"kind": "parse_issue", "detail": detail} for detail in parsed.issues]
        counts = {"requests": 0, "enriched": 0, "not_needed": 0, "failed": 0, "budget_exhausted": 0}
        for original in parsed.events:
            event = dict(original)
            if event["event_type"] != "added" or not isinstance(event.get("payload"), dict):
                event["enrichment_status"] = "not_needed"
                counts["not_needed"] += 1
                output_events.append(event)
                continue
            payload = dict(event["payload"])
            event["payload"] = payload
            try:
                extract_features(payload)
                event["enrichment_status"] = "not_needed"
                counts["not_needed"] += 1
                output_events.append(event)
                continue
            except (TypeError, ValueError):
                pass
            txid = event.get("txid")
            if not txid:
                event["enrichment_status"] = "unavailable"
                errors.append({"kind": "missing_txid", "event_id": event["event_id"], "detail": "Cannot enrich an added event without a transaction ID"})
                counts["failed"] += 1
                output_events.append(event)
                continue
            request_count_before = self.requests
            try:
                transaction, source = self._fetch(txid)
                counts["requests"] += self.requests - request_count_before
                for name in REQUIRED_TRANSACTION_FIELDS:
                    if name not in payload and name in transaction:
                        payload[name] = transaction[name]
                extract_features(payload)
                event["enrichment_status"] = source
                counts["enriched"] += 1
            except EnrichmentRequestError as error:
                counts["requests"] += self.requests - request_count_before
                event["enrichment_status"] = error.kind
                errors.append({"kind": error.kind, "event_id": event["event_id"], "txid": txid, "detail": str(error)})
                counts["budget_exhausted" if error.kind == "budget_exhausted" else "failed"] += 1
            except (TypeError, ValueError) as error:
                event["enrichment_status"] = "incomplete"
                errors.append({"kind": "incomplete_transaction", "event_id": event["event_id"], "txid": txid, "detail": str(error)})
                counts["failed"] += 1
            output_events.append(event)
        raw_text = envelope.get("raw_text")
        output = {
            "schema_version": 1,
            "message_id": envelope.get("message_id"),
            "run_id": envelope.get("run_id"),
            "session_id": envelope.get("session_id"),
            "received_at": envelope.get("received_at"),
            "source_mode": envelope.get("source_mode"),
            "message_type": envelope.get("message_type", "text"),
            "raw_payload_sha256": hashlib.sha256(
                (raw_text.encode("utf-8") if isinstance(raw_text, str) else envelope.get("raw_base64", "").encode("ascii"))
            ).hexdigest(),
            "events": output_events,
            "parse_issues": parsed.issues,
            "enrichment_summary": counts,
        }
        return output, errors
