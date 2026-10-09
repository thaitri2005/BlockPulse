# How BlockPulse works right now

BlockPulse listens for Bitcoin mempool updates, preserves what it observed, turns transaction updates into features, and screens those features with simple rules. It has a direct offline path and an optional Kafka path:

    Live:       mempool WebSocket -> capture -> messages.jsonl -> process -> features -> detect
    Streaming:  WebSocket -> local messages.jsonl + Kafka raw topic -> enrich -> Kafka enriched topic -> Parquet
    Replay:     saved messages.jsonl -> Kafka raw topic -> JSONL archive -> process

The raw JSONL file is the source record. Kafka moves and buffers messages; it does not replace that file.

## What each command does

- capture connects to the public WebSocket and saves each message to messages.jsonl. It also writes connections.jsonl and summary.json so connection attempts and missed-history gaps are visible.
- capture-kafka does the same capture, writes each envelope to the local file first, then sends it to the Kafka raw topic. If Kafka publishing fails, the local record remains.
- kafka-init creates three Kafka topics: raw observations, enriched messages, and errors.
- kafka-replay reads an existing messages.jsonl file and sends its envelopes to the raw topic. It does not contact the public source.
- kafka-archive reads raw Kafka messages and saves them to another messages.jsonl file. It syncs each new line before committing its Kafka offset; if restarted, it skips duplicate message IDs.
- kafka-enrich reads raw messages and parses lifecycle events. When an added transaction already has the fields needed for features, it makes no web request. If those fields are missing, it fetches transaction details from the mempool.space REST API under a request-per-second limit and a per-run cap. Failures are written to the Kafka error topic.
- kafka-parquet reads enriched messages and stores them as Zstandard-compressed Parquet files. Each message ID has a stable file, so a restart does not add it twice.
- process reads a saved capture JSONL file offline. It writes normalized lifecycle events, transaction features, and an error report.
- detect reads features offline and flags three simple shapes: many inputs, many outputs, or repeated output values. A flag means unusual structure; it does not establish wrongdoing.

Kafka consumers use a group ID to remember their progress. Reuse the same group ID and output directory to resume. A new group starts at the earliest retained message, so it can read old topic data again. Replaying a capture into a topic appends messages; avoid replaying it again unless you intend to test duplicate handling.

## Technologies and how they fit

Python runs the commands and pipeline logic. The websockets library receives live source messages. Apache Kafka runs in Docker Compose as one local KRaft broker; confluent-kafka connects Python producers and consumers to it. PyArrow writes Parquet, a compressed columnar format for later analysis. JSONL keeps the original messages inspectable and makes offline replay easy. CSV makes feature rows easy to open in a spreadsheet. pytest checks the parsing, features, Kafka behavior, and recovery rules.

The broker has one node, so it is a learning setup, not a highly available service. The current Parquet writer makes one small file per message for simple restart behavior. Enrichment only calls the public REST endpoint for incomplete added transactions, and the provider may return rate-limit errors. There is no database, API, dashboard, trained model, or entity analysis yet.

## Check the verified run

The broker and saved outputs from the last end-to-end run are already in the workspace. Check the broker, run the tests, then inspect the records:

    docker compose --profile m1 ps
    ./.venv/Scripts/python.exe -m pytest -q

    (Get-Content data/kafka/archive-854913a015a0453dad3b15c8b8af12cc/messages.jsonl | Measure-Object -Line).Lines
    $processed = Get-Content data/processed/kafka-854913a015a0453dad3b15c8b8af12cc-retry/summary.json | ConvertFrom-Json
    $processed.features_written
    $processed.event_counts
    $processed.issue_count

    $parquet = Get-Content data/archives/m1-049c29f100aa4dd68bbaac637f77527f/manifest.json | ConvertFrom-Json
    $parquet.archive_messages_total
    $parquet.archive_bytes
    $parquet.consumer_lag_messages

Expected results: the JSONL archive has 402 lines; processing reports 2,732 features, event counts of 2,732 added, 584 removed, 3,468 mined, and 199 replaced, with zero issues. The Parquet manifest reports 402 archived messages and zero lag. The last consumer run may show messages_archived as zero because that run was a restart check; archive_messages_total is the accumulated count.

To test a new live capture, run capture-kafka with a short duration and a new output directory. That step needs a working route to the public WebSocket; the saved-data replay and the rest of the pipeline do not.
