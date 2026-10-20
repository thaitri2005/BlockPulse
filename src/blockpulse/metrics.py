"""Prometheus metrics for the streaming lake sink.

Four questions an operator asks, each answered by a metric Prometheus scrapes from `/metrics`:

* Is it keeping up?        blockpulse_consumer_lag_messages
* Is the data fresh?       blockpulse_newest_event_age_seconds (now minus the newest event's receive time)
* Is bad data arriving?    blockpulse_rows_quarantined_total
* How fast is it going?    blockpulse_messages_consumed_total, blockpulse_rows_written_total, blockpulse_batch_seconds

Labels are low-cardinality on purpose (table names only). Transaction ids and error strings never become labels.
Each process uses its own registry, so tests can create several instances.
"""

import time
from datetime import datetime


class StreamLakeMetrics:
    def __init__(self, port: int | None = None, *, registry=None):
        from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server
        self.registry = registry or CollectorRegistry()
        self.messages = Counter("blockpulse_messages_consumed_total", "Enriched messages read from Kafka",
                                registry=self.registry)
        self.rows_written = Counter("blockpulse_rows_written_total", "Rows written to the lake", ["table"],
                                    registry=self.registry)
        self.rows_quarantined = Counter("blockpulse_rows_quarantined_total", "Rows that failed the lake contract",
                                        ["table"], registry=self.registry)
        self.batches = Counter("blockpulse_batches_total", "Micro-batches written and committed", registry=self.registry)
        self.batch_seconds = Histogram("blockpulse_batch_seconds", "Time between micro-batch flushes",
                                       buckets=(0.5, 1, 2, 5, 10, 30, 60), registry=self.registry)
        self.lag = Gauge("blockpulse_consumer_lag_messages", "Messages behind the end of the enriched topic",
                         registry=self.registry)
        self.newest_event = Gauge("blockpulse_newest_event_timestamp_seconds",
                                  "Receive time of the newest event seen (Unix seconds)", registry=self.registry)
        self.newest_age = Gauge("blockpulse_newest_event_age_seconds",
                                "Seconds since the newest event seen was received; grows when data stops arriving",
                                registry=self.registry)
        self.newest_age.set_function(self._age)
        self._newest = None
        self._last_batch = time.monotonic()
        if port:
            start_http_server(port, registry=self.registry)

    def _age(self) -> float:
        return float("nan") if self._newest is None else max(0.0, time.time() - self._newest)

    def message_consumed(self, record: dict) -> None:
        self.messages.inc()
        received = record.get("received_at")
        if isinstance(received, str):
            try:
                moment = datetime.fromisoformat(received.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return
            if self._newest is None or moment > self._newest:
                self._newest = moment
                self.newest_event.set(moment)

    def batch_written(self, report: dict, consumer=None) -> None:
        for table, stats in report["tables"].items():
            self.rows_written.labels(table=table).inc(stats["rows_written"])
            self.rows_quarantined.labels(table=table).inc(stats["rows_quarantined"])
        self.batches.inc()
        now = time.monotonic()
        self.batch_seconds.observe(now - self._last_batch)
        self._last_batch = now
        if consumer is not None:
            from blockpulse.kafka_workers import _consumer_lag
            lag = _consumer_lag(consumer)
            if lag is not None:
                self.lag.set(lag)
