import json

from blockpulse.enrichment import RateLimitedEnricher
from blockpulse.kafka_pipeline import ENRICHED_TOPIC, ERROR_TOPIC, RAW_TOPIC
from blockpulse.kafka_workers import enrich_topic


class Message:
    def __init__(self, value, offset=0):
        self._value, self._offset = value, offset
    def value(self): return self._value
    def partition(self): return 0
    def offset(self): return self._offset
    def error(self): return None


class Consumer:
    instances = []
    def __init__(self, config, records):
        self.config, self.records, self.commits = config, list(records), []
        self.subscriptions = []
        Consumer.instances.append(self)
    def subscribe(self, topics): self.subscriptions = topics
    def poll(self, timeout): return self.records.pop(0) if self.records else None
    def assignment(self): return [object()]
    def commit(self, *, message, asynchronous): self.commits.append(message.offset())
    def close(self): pass


class Producer:
    instances = []
    def __init__(self, config): self.config, self.records, self.sent, self.flushes = config, [], [], 0; Producer.instances.append(self)
    def produce(self, topic, *, key, value, on_delivery): self.records.append((topic, key, value, on_delivery))
    def flush(self, timeout):
        self.flushes += 1
        pending, self.records = self.records, []
        self.sent.extend(pending)
        for _, _, _, callback in pending: callback(None, None)
        return 0
    def poll(self, timeout): pass


def source(txid):
    return {
        "schema_version": 1, "run_id": "r", "session_id": "s", "message_id": "r:1",
        "received_at": "2026-10-09T00:00:00+00:00", "source_mode": "synthetic", "message_type": "text",
        "raw_text": json.dumps({"mempool-transactions": {"sequence": 1, "added": [{"txid": txid}]}}),
    }


def test_worker_publishes_enriched_then_error_before_committing():
    txid = "a" * 64
    raw = source(txid)
    record = Message(json.dumps(raw).encode())
    instances = []
    def consumer_factory(config):
        value = Consumer(config, [record]); instances.append(value); return value
    producer = []
    def producer_factory(config):
        value = Producer(config); producer.append(value); return value
    result = enrich_topic(
        "test-enricher", consumer_factory=consumer_factory, producer_factory=producer_factory,
        enricher=RateLimitedEnricher(max_requests=1, fetch=lambda _txid: (_ for _ in ()).throw(RuntimeError("offline"))),
        max_messages=1,
    )
    assert result["messages_consumed"] == 1
    assert result["error_records"] == 1
    assert instances[0].subscriptions == [RAW_TOPIC]
    assert instances[0].commits == [0]
    assert producer[0].flushes == 3
    assert [record[0] for record in producer[0].sent] == [ENRICHED_TOPIC, ERROR_TOPIC]


def test_worker_routes_enriched_payload_to_expected_topic():
    txid = "b" * 64
    payload = {"txid": txid, "vin": [{"sequence": 1}], "vout": [{"value": 10}], "weight": 400, "fee": 5}
    raw = {**source(txid), "raw_text": json.dumps({"mempool-transactions": {"sequence": 1, "added": [payload]}})}
    instance = Consumer({}, [Message(json.dumps(raw).encode())])
    producer = Producer({})
    result = enrich_topic(
        "test-complete", consumer_factory=lambda _config: instance,
        producer_factory=lambda _config: producer,
        enricher=RateLimitedEnricher(fetch=lambda _txid: AssertionError("complete record should not fetch")),
        max_messages=1,
    )
    assert result["enrichment_requests"] == 0
    assert instance.commits == [0]
    assert producer.flushes == 2
    assert [record[0] for record in producer.sent] == [ENRICHED_TOPIC]
