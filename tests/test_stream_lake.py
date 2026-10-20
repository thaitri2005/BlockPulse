import json

import pytest

pytest.importorskip("pyarrow")

from blockpulse import collector  # noqa: E402
from blockpulse.enrichment import RateLimitedEnricher  # noqa: E402
from blockpulse.storage import open_capture  # noqa: E402
from blockpulse.stream_lake import stream_to_lake  # noqa: E402
from tests.test_collector import captured_layout  # noqa: E402


class Message:
    def __init__(self, value, partition, offset):
        self._value, self._partition, self._offset = value, partition, offset

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None


class FakeConsumer:
    """Serves messages in order; remembers committed offsets per partition like a consumer group would."""

    committed = {}

    def __init__(self, messages, *, commit=True):
        start = {p: o for p, o in FakeConsumer.committed.items()}
        self.queue = [m for m in messages if m.offset() >= start.get(m.partition(), 0)]
        self.allow_commit = commit

    def subscribe(self, topics):
        pass

    def poll(self, timeout):
        return self.queue.pop(0) if self.queue else None

    def assignment(self):
        return [0]

    def commit(self, *, message, asynchronous):
        if self.allow_commit:
            FakeConsumer.committed[message.partition()] = message.offset() + 1

    def close(self):
        pass


def enriched_messages(layout, partitions=2):
    """Enrich every captured message offline and spread them over partitions, keyed by session like the publisher."""
    enricher = RateLimitedEnricher(fetch=lambda txid: None)
    messages, offsets = [], {}
    for index, name in enumerate(collector.finished_captures(layout)):
        partition = index % partitions
        with open_capture(layout.raw / name) as lines:
            for raw in lines:
                enriched, _ = enricher.process(json.loads(raw))
                offset = offsets.get(partition, 0)
                offsets[partition] = offset + 1
                messages.append(Message(json.dumps(enriched).encode(), partition, offset))
    return messages


def read_table(lake, table, key):
    import pyarrow.parquet as pq
    rows = []
    for path in sorted((lake / table).rglob("*.parquet")):
        rows.extend(pq.read_table(path).to_pylist())
    return sorted(rows, key=lambda r: r[key])


@pytest.fixture
def layout(tmp_path, transaction):
    FakeConsumer.committed = {}
    layout = captured_layout(tmp_path, transaction, sessions=3)
    assert collector.process_pending(layout)["failed"] == []  # batch lake, for comparison
    return layout


def test_streamed_lake_equals_the_batch_lake(layout, tmp_path):
    stream_lake = tmp_path / "stream-lake"
    messages = enriched_messages(layout)
    summary = stream_to_lake(stream_lake, "g", "unused", consumer_factory=lambda _c: FakeConsumer(messages),
                             idle_timeout=0.1, batch_messages=2)
    assert summary["messages"] == len(messages) and summary["batches"] >= 2
    run_of = {name: json.loads((layout.raw / name / "summary.json").read_text(encoding="utf-8"))["run_id"]
              for name in collector.finished_captures(layout)}
    for table, key in (("transactions", "txid"), ("lifecycle_events", "event_id")):
        batch = read_table(layout.root / "lake", table, key)
        for row in batch:  # the stream knows captures by run id, the batch path by folder name
            row["session"] = run_of[row["session"]]
        streamed = read_table(stream_lake, table, key)
        drop = ("dt", "hour")  # partition columns aren't stored in the files
        assert [{k: v for k, v in r.items() if k not in drop} for r in streamed] == \
               [{k: v for k, v in r.items() if k not in drop} for r in batch], table
    assert summary["transactions_quarantined"] == 0 and summary["lifecycle_events_quarantined"] == 0


def test_a_crash_before_commit_replays_without_duplicates(layout, tmp_path):
    stream_lake = tmp_path / "stream-lake"
    messages = enriched_messages(layout)
    # First run writes every batch but its commits are lost (crash after write, before commit).
    stream_to_lake(stream_lake, "g", "unused", consumer_factory=lambda _c: FakeConsumer(messages, commit=False),
                   idle_timeout=0.1, batch_messages=2)
    first = read_table(stream_lake, "lifecycle_events", "event_id")
    # The restart reads everything again from the last committed offset (none) and rewrites the same batches.
    stream_to_lake(stream_lake, "g", "unused", consumer_factory=lambda _c: FakeConsumer(messages),
                   idle_timeout=0.1, batch_messages=2)
    second = read_table(stream_lake, "lifecycle_events", "event_id")
    assert second == first and len({r["event_id"] for r in second}) == len(second)
    # A further restart after successful commits has nothing left to read.
    third = stream_to_lake(stream_lake, "g", "unused", consumer_factory=lambda _c: FakeConsumer(messages),
                           idle_timeout=0.1, batch_messages=2)
    assert third.get("messages", 0) == 0


def test_a_replay_with_different_batch_boundaries_leaves_no_stale_rows(layout, tmp_path):
    stream_lake = tmp_path / "stream-lake"
    messages = enriched_messages(layout)
    # The crashed run cut the stream into small batches; the replay uses one big batch per partition.
    stream_to_lake(stream_lake, "g", "unused", consumer_factory=lambda _c: FakeConsumer(messages, commit=False),
                   idle_timeout=0.1, batch_messages=1)
    small = read_table(stream_lake, "lifecycle_events", "event_id")
    summary = stream_to_lake(stream_lake, "g", "unused", consumer_factory=lambda _c: FakeConsumer(messages),
                             idle_timeout=0.1, batch_messages=10_000)
    big = read_table(stream_lake, "lifecycle_events", "event_id")
    assert big == small and len({r["event_id"] for r in big}) == len(big)
    assert summary["stale_files_removed"] > 0
    names = {p.name for p in (stream_lake / "lifecycle_events").rglob("*.parquet")}
    assert all(name.startswith("stream-p") and "-o000000000000." in name for name in names)  # one batch per partition
