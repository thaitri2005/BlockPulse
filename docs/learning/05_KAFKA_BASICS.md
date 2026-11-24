# Exercise 5: Kafka basics with our own data

The goal is to see what happens when a saved capture is sent into Kafka. A **topic** is a named, append-only log of records. A topic is split into **partitions**, each an ordered log of its own. A **producer** is a program that writes records to a topic. Each record can carry a **key**, and the producer hashes the key to choose the partition. An **offset** is a record's position number inside its partition. Kafka keeps order within one partition only, so the key decides which records stay in order relative to each other.

The first half of this exercise needs no broker. The second half needs the local Kafka container.

## Run it

Use the generated capture from [Exercise 2](02_STREAM_TO_FILE.md); create it if it is missing. It is labelled synthetic.

```powershell
.\.venv\Scripts\python.exe -m blockpulse synth-capture --output data/learning/synth-200 --messages 200
```

**Without a broker.** `publish_capture()` accepts a stand-in producer, which is how the tests run it. This one records what would have been sent:

```powershell
@'
from collections import Counter
from pathlib import Path
from blockpulse.kafka_pipeline import publish_capture

class RecordingProducer:
    records = []
    def __init__(self, config): self.config = config
    def produce(self, topic, *, key, value, on_delivery): self.records.append((topic, key))
    def poll(self, timeout): pass
    def flush(self, timeout): return 0

summary = publish_capture(Path("data/learning/synth-200/messages.jsonl"), producer_factory=RecordingProducer)
print(summary["messages_published"], summary["partition_key"])
print(Counter(RecordingProducer.records))
'@ | .\.venv\Scripts\python.exe -
```

**With the broker.** Start Kafka (1 GiB memory limit) and create the three topics:

```powershell
docker compose --profile m1 up -d --wait
.\.venv\Scripts\python.exe -m blockpulse kafka-init
docker compose --profile m1 exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose --profile m1 exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic blockpulse.raw.v1
```

The broker does not create topics on first use (`KAFKA_AUTO_CREATE_TOPICS_ENABLE` is `false` in `compose.yaml`), so make a scratch topic before writing to it:

```powershell
docker compose --profile m1 exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic blockpulse.scratch.ex5 --partitions 3 --replication-factor 1
```

`kafka-replay` always writes to `blockpulse.raw.v1`. To use the scratch topic, call the same function with `topic=`:

```powershell
@'
from pathlib import Path
from blockpulse.kafka_pipeline import publish_capture
print(publish_capture(Path("data/learning/synth-200/messages.jsonl"), topic="blockpulse.scratch.ex5"))
'@ | .\.venv\Scripts\python.exe -
```

Do not use `examples/fixtures/mempool_messages.jsonl` here: its last line is deliberately invalid JSON, and `publish_capture()` raises on a line it cannot decode. When you are done: `docker compose --profile m1 stop`.

## Open the output

The offline script printed, on my run:

```text
200 session_id, then run_id, then message_id
Counter({('blockpulse.raw.v1', b'synthetic-7-200'): 200})
```

All 200 records have the same key, `synthetic-7-200`, which is the capture's `session_id`. Equal keys hash to the same partition, so on a real broker these 200 records sit in one of the three partitions, in file order, and the other two receive none of them.

With the broker running, check this by reading the scratch records back with their partition and offset:

```powershell
docker compose --profile m1 exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic blockpulse.scratch.ex5 --from-beginning --max-messages 3 --property print.partition=true --property print.offset=true --property print.key=true --property print.value=false
```

I could not run the broker commands here, so there is no output to show for them. Write down the partition number you see, and whether the offsets count up from zero.

## Read one function

Open `src/blockpulse/kafka_pipeline.py` and read `publish_capture()`. Trace `session_id` from the envelope to the record key:

```python
key = next(
    (envelope[field] for field in ("session_id", "run_id", "message_id")
     if isinstance(envelope.get(field), str) and envelope[field]),
    envelope["message_id"],
)
value = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
```

The key is the first of three fields that is a non-empty string. The value is the whole envelope, unchanged. A few lines later `producer.produce(...)` hands the record to the client library, which sends it in the background.

Two producer settings near the top of the function matter. `"acks": "all"` means the broker must confirm a record before it counts as delivered. `"enable.idempotence": True` lets the broker discard a record the producer sent twice while retrying. At the end, `producer.flush(30)` waits for the confirmations, and any delivery error raises.

`RawKafkaPublisher.__call__` builds the same key for the live path. `capture()` calls it only after the envelope is on disk (Exercise 2), so the local file stays the recovery copy.

## Change one small thing

Give the producer a second session. Generate another capture with a different seed:

```powershell
.\.venv\Scripts\python.exe -m blockpulse synth-capture --output data/learning/synth-8-50 --messages 50 --seed 8
```

In the offline script, replace the `summary = ...` and first `print` lines with a loop that publishes both folders:

```python
for folder in ("synth-200", "synth-8-50"):
    publish_capture(Path("data/learning") / folder / "messages.jsonl", producer_factory=RecordingProducer)
```

My run printed:

```text
Counter({('blockpulse.raw.v1', b'synthetic-7-200'): 200, ('blockpulse.raw.v1', b'synthetic-8-50'): 50})
```

Two keys now. Each session keeps its own order, and the two may or may not share a partition, depending on their hashes. Then rerun the tests for this area:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_kafka_pipeline.py
```

## Question to carry forward

What does the record key decide, and why is ours the capture session ID? Consider what would break downstream if there were no key at all. The [Kafka guide](../guides/03-kafka.md) has the surrounding pipeline.

## What was run

Verified on Linux on 2026-11-24: `synth-capture` with both seeds, the offline recording script in both forms (the Python was run from a shell here-document, not from PowerShell), and `pytest tests/test_kafka_pipeline.py` (7 passed). Not run here, because no Docker or Kafka was available to this session: `docker compose --profile m1 up`, `kafka-init`, every `kafka-topics.sh` and `kafka-console-consumer.sh` command, and the publish to `blockpulse.scratch.ex5`. Those steps are written from `compose.yaml`, the code and the guide.
