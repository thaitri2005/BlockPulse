# Exercise 6: consumers, commits, and restarts

The goal is to understand how a program that reads from Kafka survives being stopped. A **consumer** reads records from a topic. A **consumer group** is a name under which the broker remembers how far reading has got. To **commit** an offset is to tell the broker "everything up to here is done for this group". **At-least-once** delivery means a record is never skipped but may arrive twice after a restart. An **idempotent sink** is an output that ends up the same whether a record is written once or twice. **Lag** is the number of records in the topic that the group has not yet read.

## Run it

This exercise needs the local Kafka container. Start it and create the topics:

```powershell
docker compose --profile m1 up -d --wait
.\.venv\Scripts\python.exe -m blockpulse kafka-init
```

Publish the generated capture from [Exercise 2](02_STREAM_TO_FILE.md) to the raw topic (create the capture first if it is missing):

```powershell
.\.venv\Scripts\python.exe -m blockpulse synth-capture --output data/learning/synth-200 --messages 200
.\.venv\Scripts\python.exe -m blockpulse kafka-replay data/learning/synth-200/messages.jsonl
```

Archive part of the topic, then stop. `--max-messages 120` is a repeatable stand-in for pressing Ctrl+C partway through:

```powershell
.\.venv\Scripts\python.exe -m blockpulse kafka-archive --output data/learning/ex06-archive --group-id learn-archive --max-messages 120
```

Run it again with the same group and the same folder, this time with no limit. It stops by itself after three idle seconds:

```powershell
.\.venv\Scripts\python.exe -m blockpulse kafka-archive --output data/learning/ex06-archive --group-id learn-archive
```

Then ask the broker what it remembers about the group:

```powershell
docker compose --profile m1 exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group learn-archive
```

Every envelope is labelled `source_mode: synthetic`, and the archive goes under `data/learning/`. If the topic already holds other captures, the group reads those too, and your counts will be larger. Stop the broker afterwards with `docker compose --profile m1 stop`.

## Open the output

The archive folder holds `messages.jsonl`, `manifest.json` and `errors.jsonl`. Each run saves its summary as `manifest.json`.

I could not run a broker in this session. To get real output from the same function, I ran `archive_topic()` with the `FakeConsumer` from `tests/test_kafka_pipeline.py`, feeding it the 200 generated envelopes as partition 0, offsets 0 to 199. Run one stopped at 120 messages. Run two received offsets 110 onward, as if the last ten commits had been lost in a crash:

```text
{'group_id': 'learn-archive', 'messages_consumed': 120, 'messages_archived': 120, 'duplicate_messages_skipped': 0, 'last_offsets_by_partition': {'0': 119}}
{'group_id': 'learn-archive', 'messages_consumed': 90, 'messages_archived': 80, 'duplicate_messages_skipped': 10, 'last_offsets_by_partition': {'0': 199}}
```

Afterwards `messages.jsonl` had exactly 200 lines. Ten envelopes arrived twice and were written once. That is at-least-once delivery absorbed by an idempotent sink.

On your real run, read the same fields. With a clean stop there is no redelivery, so expect the second run's `messages_consumed` to cover only what the first left, and `duplicate_messages_skipped` to be 0. Also read `consumer_lag_messages` in the summary, and the `LAG` column of the `--describe` output.

## Read one function

Open `src/blockpulse/kafka_pipeline.py` and read `archive_topic()`. Trace one record's `offset` through the loop:

```python
prior = seen.get(row["message_id"])
if prior is None:
    _append_jsonl(archive, row)
    seen[row["message_id"]] = digest
    archived += 1
elif prior == digest:
    duplicates += 1
```

and, after the error handling below it:

```python
# Local append/error record is flushed and fsynced before this offset advances.
consumer.commit(message=message, asynchronous=False)
```

The order is: write, then commit. `_append_jsonl()` writes the line, flushes it, and calls `os.fsync`, which asks the operating system to put the bytes on the disk itself. Only then is the offset committed, and `asynchronous=False` waits for the broker's answer.

Two consumer settings make this work. `"enable.auto.commit": False` stops the client library from committing on a timer, which could mark a record done before it is written. `"auto.offset.reset": "earliest"` means a group with no committed offset starts from the oldest retained record. And `seen` is rebuilt from the archive file by `_load_seen()` at startup, so the duplicate check survives a restart.

`parquet_archive.py` follows the same pattern. Find its `os.fsync`, `os.replace` and `consumer.commit` lines and check they come in that order.

## Change one small thing

Read the topic again under a new group. Use a new folder:

```powershell
.\.venv\Scripts\python.exe -m blockpulse kafka-archive --output data/learning/ex06-archive-2 --group-id learn-archive-2
```

A new group has no committed offsets, so it starts from the earliest record and reads everything again. The first group's position is unaffected.

Now try the new group with the old folder:

```powershell
.\.venv\Scripts\python.exe -m blockpulse kafka-archive --output data/learning/ex06-archive --group-id learn-archive-2
```

The function refuses before it connects. I checked the same call against the offline archive and it raised:

```text
ValueError: Existing archive topic/group_id differs; use a new output directory or the original settings
```

The manifest ties a folder to one topic and one group. Then rerun the tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_kafka_pipeline.py tests/test_parquet_archive.py
```

Read `test_archive_is_restart_safe_and_deduplicates_before_committing`. Its `check_durable_before_commit` hook fails the test if a commit ever happens before an `fsync`.

## Question to carry forward

When is an offset committed, and what would go wrong if we committed before writing the file? Walk through a crash between the two steps in each order, and say which order loses a record and which only repeats one.

## What was run

Verified on Linux on 2026-11-24: `synth-capture`; `archive_topic()` driven by the tests' `FakeConsumer` over the generated capture (the two summaries above, and the 200-line archive); the refused call with a different group ID; `pytest tests/test_kafka_pipeline.py` (7 passed) and `tests/test_parquet_archive.py` (2 passed). Not run here, because no Docker or Kafka was available to this session: `docker compose --profile m1 up`, `kafka-init`, `kafka-replay`, every `kafka-archive` command, and `kafka-consumer-groups.sh`. Those steps are written from the code, the tests and the [Kafka guide](../guides/03-kafka.md).
