import pytest

pytest.importorskip("prometheus_client")

from blockpulse.metrics import StreamLakeMetrics  # noqa: E402


def sample(metrics, name, labels=None):
    return metrics.registry.get_sample_value(name, labels or {})


def test_counters_and_freshness_follow_the_stream():
    metrics = StreamLakeMetrics()
    assert sample(metrics, "blockpulse_newest_event_age_seconds") != sample(metrics, "blockpulse_newest_event_age_seconds")  # NaN until data
    metrics.message_consumed({"received_at": "2026-10-17T00:00:00+00:00"})
    metrics.message_consumed({"received_at": "2026-10-16T00:00:00+00:00"})  # older: newest must not move back
    assert sample(metrics, "blockpulse_messages_consumed_total") == 2
    assert sample(metrics, "blockpulse_newest_event_timestamp_seconds") == 1792195200.0
    assert sample(metrics, "blockpulse_newest_event_age_seconds") > 0
    report = {"tables": {"transactions": {"rows_written": 5, "rows_quarantined": 1},
                         "lifecycle_events": {"rows_written": 9, "rows_quarantined": 0}}}
    metrics.batch_written(report)
    metrics.batch_written(report)
    assert sample(metrics, "blockpulse_rows_written_total", {"table": "transactions"}) == 10
    assert sample(metrics, "blockpulse_rows_quarantined_total", {"table": "transactions"}) == 2
    assert sample(metrics, "blockpulse_batches_total") == 2


def test_stream_sink_reports_to_metrics(tmp_path, transaction):
    from blockpulse import collector
    from blockpulse.stream_lake import stream_to_lake
    from tests.test_collector import captured_layout
    from tests.test_stream_lake import FakeConsumer, enriched_messages
    FakeConsumer.committed = {}
    layout = captured_layout(tmp_path / "src", transaction, sessions=2)
    messages = enriched_messages(layout)
    metrics = StreamLakeMetrics()
    summary = stream_to_lake(tmp_path / "lake", "g", "unused", consumer_factory=lambda _c: FakeConsumer(messages),
                             idle_timeout=0.1, metrics=metrics)
    assert sample(metrics, "blockpulse_messages_consumed_total") == summary["messages"]
    assert sample(metrics, "blockpulse_rows_written_total", {"table": "lifecycle_events"}) == summary["lifecycle_events_written"]
    assert collector  # imported for the layout helper
