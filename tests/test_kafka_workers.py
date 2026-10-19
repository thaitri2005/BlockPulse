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
    assert producer[0].flushes == 2  # one checkpoint flush (before the commit) and one on close
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


def complete_message(n):
    txid = f"{n:064x}"
    payload = {"txid": txid, "vin": [{"sequence": 1}], "vout": [{"value": 10}], "weight": 400, "fee": 5}
    raw = {**source(txid), "message_id": f"r:{n}",
           "raw_text": json.dumps({"mempool-transactions": {"sequence": n, "added": [payload]}})}
    return Message(json.dumps(raw).encode(), offset=n)


class OrderedProducer(Producer):
    """Records the order of flushes and commits so the test can check output is acknowledged first."""
    def __init__(self, config, log): super().__init__(config); self.log = log
    def flush(self, timeout):
        self.log.append(("flush", len(self.records))); return super().flush(timeout)


def test_worker_commits_in_batches_only_after_a_flush():
    log = []
    consumer = Consumer({}, [complete_message(n) for n in range(5)])
    consumer.commit = lambda *, message, asynchronous: log.append(("commit", message.offset()))
    producer = OrderedProducer({}, log)
    result = enrich_topic("batch", consumer_factory=lambda _c: consumer, producer_factory=lambda _c: producer,
                          enricher=RateLimitedEnricher(fetch=lambda _t: None), max_messages=5, commit_every=2)
    assert result["messages_consumed"] == 5
    commits = [entry for entry in log if entry[0] == "commit"]
    assert [offset for _, offset in commits] == [1, 3, 4]  # every 2 messages, then the remainder
    for index, entry in enumerate(log):  # each commit follows a flush with nothing left unsent
        if entry[0] == "commit":
            assert any(prior[0] == "flush" for prior in log[:index])
    assert len(producer.sent) == 5


def test_worker_does_not_commit_when_a_delivery_fails():
    class FailingProducer(Producer):
        def flush(self, timeout):
            pending, self.records = self.records, []
            for _, _, _, callback in pending: callback("broker unavailable", None)
            return 0
    consumer = Consumer({}, [complete_message(n) for n in range(3)])
    try:
        enrich_topic("fail", consumer_factory=lambda _c: consumer, producer_factory=lambda _c: FailingProducer({}),
                     enricher=RateLimitedEnricher(fetch=lambda _t: None), max_messages=3)
    except RuntimeError as error:
        assert "publish failed before commit" in str(error)
    else:
        raise AssertionError("a failed delivery must stop the worker")
    assert consumer.commits == []  # nothing committed: the messages will be processed again (at-least-once)
