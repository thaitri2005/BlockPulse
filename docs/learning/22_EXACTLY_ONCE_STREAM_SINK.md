# Exercise 22: exactly-once output from an at-least-once stream

The goal is to understand how `kafka-lake` can be killed at any moment, restarted, and still leave each row in the lake exactly once, even though Kafka will hand it some messages twice. You will reproduce a crash with a fake consumer (no broker needed), watch the replay clean up after the crashed run, and read the two functions that make that work.

Terms from exercises 5 and 6, in one line each. A Kafka **partition** is an ordered list of messages; a message's **offset** is its position in that list. A consumer **commits** an offset to say "I am done up to here"; after a restart it resumes from the last commit. **At-least-once** delivery means a message may arrive again after a crash but is never skipped. A **sink** is the last step of a pipeline, the one that writes results somewhere. A **micro-batch** is a small group of consecutive messages written together.

## Run it

The real command needs Kafka and is in the [streaming guide](../guides/12-streaming-lake-and-metrics.md): `blockpulse kafka-lake --group-id lake-1`. For this exercise, drive the same function, `stream_to_lake()`, with the fake consumer from the tests. Save this as `$env:TEMP\bp-learn\stream_demo.py`:

```python
import sys
from pathlib import Path

from blockpulse import collector
from blockpulse.stream_lake import stream_to_lake
from tests.test_stream_lake import FakeConsumer, enriched_messages

root = Path(sys.argv[1])
lake = root / "lake-stream"
messages = enriched_messages(collector.Layout(root))  # one partition per session
for label, commit, size in (("crashed run", False, 300), ("replay", True, 500)):
    summary = stream_to_lake(lake, "demo", "unused", idle_timeout=0.1, batch_messages=size,
                             consumer_factory=lambda _c: FakeConsumer(messages, commit=commit))
    files = sorted(p.name for p in (lake / "transactions").rglob("*.parquet"))
    print(label, {k: summary.get(k, 0) for k in ("messages", "batches", "transactions_written", "stale_files_removed")})
    print("  transaction files:", len(files), files[:3])
```

Then, from PowerShell in the project folder, with the scratch root from exercise 19 (two generated sessions, labelled synthetic):

```powershell
$py = ".\.venv\Scripts\python.exe"
$root = "$env:TEMP\bp-learn"
$env:PYTHONPATH = "."
& $py "$root\stream_demo.py" $root
Remove-Item Env:PYTHONPATH
```

The first pass writes every batch but its commits are thrown away (`commit=False`): that is a process that died after writing and before committing. The second pass is the restart. It reads everything again, and it cuts batches at 500 messages where the first used 300.

## Open the output

Between the JSON log lines, the script prints:

```text
crashed run {'messages': 4000, 'batches': 14, 'transactions_written': 16000, 'stale_files_removed': 0}
  transaction files: 14 ['stream-p0-o000000000000.parquet', 'stream-p0-o000000000300.parquet', 'stream-p0-o000000000600.parquet']
replay {'messages': 4000, 'batches': 8, 'transactions_written': 16000, 'stale_files_removed': 42}
  transaction files: 8 ['stream-p0-o000000000000.parquet', 'stream-p0-o000000000500.parquet', 'stream-p0-o000000001000.parquet']
```

Read the file names: `stream-p0-o000000000300` is partition 0, batch starting at offset 300. Each run wrote 16,000 transaction rows, 32,000 in total, yet the lake ends with 8 files holding 16,000 rows and 16,000 distinct transaction ids. The replay removed 42 files first: the crashed run's 14 batches each left a transactions file, a lifecycle-events file and a quality report.

## Read two functions

Open [`src/blockpulse/stream_lake.py`](../../src/blockpulse/stream_lake.py) and find `flush()` inside `stream_to_lake()`. The order of its three steps is the whole design:

```python
batch = batches.pop(partition)
totals["stale_files_removed"] += _remove_replayed(lake, partition, batch["first"], batch["last_message"].offset())
report = write_batch(lake, _batch_name(partition, batch["first"]), batch["rows"])
...
consumer.commit(message=batch["last_message"], asynchronous=False)
```

Delete leftovers, write, then commit. Consider a crash at each point. Before the write: nothing is on disk and nothing is committed, so the batch is read again. After the write but before the commit: the files exist, the offset was not committed, so the batch is read again and written again. After the commit: the batch is done. In no case is data lost. The only risk is the middle one, where the same rows could be written twice, and that is what the first line handles.

If batches always covered the same offsets, a file name based on the first offset would be enough: the rewrite would overwrite the old file. But a batch also closes after 5 seconds, so a replay can cut the same offsets differently, as it did above. Follow offset 300 through `_remove_replayed()`:

```python
for path in folder.glob(pattern.format(p=partition)):
    start = int(path.name.split("-o", 1)[1].split(".", 1)[0])
    if first <= start <= last:
        path.unlink()
        removed += 1
```

The replay's first batch covers offsets 0 to 499. No new file will ever be named `o000000000300`, so nothing would overwrite the crashed run's file with that name. It is deleted because its start, 300, lies inside the range 0 to 499. The delete works on offset *ranges*, not on names.

This is what "exactly-once output" means here. Delivery is still at-least-once: every message arrived twice in the demo. The sink is **idempotent**: processing the same offsets again leaves the same files.

## Change one small thing

In `_remove_replayed()`, change the condition to `if start == first:`, so a replay deletes only the file that shares its exact name. Remove `lake-stream` from the scratch root and run the demo again. The replay now reports 12 stale files removed and leaves 18 transaction files; they hold 27,200 rows for 16,000 distinct transaction ids. Every row from a batch that started at offset 300, 600, 900, 1200 or 1800 is in the lake twice.

Now run the tests and see which one notices:

```powershell
& $py -m pytest -q tests/test_stream_lake.py -v
```

Restore the line and run them again (3 tests should pass).

## Question to carry forward

The guide's limits section says a 5-second batch per partition makes many small Parquet files under live traffic, and suggests compacting each hour after it closes. A compacted file would no longer be named after an offset. What would `_remove_replayed()` need to know to stay correct once compaction exists, and when is an hour safe to compact?

## What was verified

Run on 2026-11-24 on Linux, offline: `pytest tests/test_stream_lake.py` (3 passed) and the demo script above against a scratch root, with the output quoted. The final stream lake was compared with the batch lake from exercise 19 on `txid`, `fee_sat` and `is_flagged` and matched. The `start == first` numbers come from replacing `_remove_replayed` in memory for one run, not from editing the file, so the failing test was not observed. **Not run:** `blockpulse kafka-lake`, Kafka, Prometheus or anything else in Docker. Everything about a real broker (group rebalances, commit failures, lag) is described from the code and the guide's recorded run, not from this session.
