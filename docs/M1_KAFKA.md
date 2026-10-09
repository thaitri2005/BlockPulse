# M1: ingest, enrich, archive, and replay

M1 adds a local Kafka path around the capture and processing code already used in Phase 0 and Phase 1:

    live WebSocket -> local JSONL + Kafka raw topic -> bounded enrichment -> enriched topic -> Parquet archive
    saved messages.jsonl -> Kafka raw topic -> JSONL archive -> offline process

The local broker is a learning environment, not a highly available service. The raw JSONL capture remains the recovery copy.

## What each part does

- Docker Compose runs Apache Kafka 4.3.1 as a single KRaft broker, bound to 127.0.0.1. The raw, enriched, and error topics each have three partitions, replication factor one, and seven-day retention. Broker and producer message limits are set above the capture's four MiB per-message limit.
- capture-kafka writes every WebSocket envelope to local messages.jsonl first, then publishes that same envelope to the raw topic. A Kafka failure leaves the local record available and causes the command to report failure.
- kafka-replay publishes a previously saved capture without opening a WebSocket.
- kafka-enrich consumes raw envelopes. Complete added transactions need no HTTP request. If required fields are missing, it calls the mempool.space transaction endpoint at a default maximum of one request per second and 100 requests per run. A failed, rate-limited, mismatched, or over-budget fetch produces an explicit error record; offsets advance only after enriched/error output is acknowledged. See the [REST API reference](https://mempool.space/docs/api/rest).
- kafka-archive writes raw envelopes to JSONL, flushes them to disk before committing offsets, and deduplicates by message_id on restart.
- kafka-parquet writes enriched records as Zstandard-compressed Parquet files. Each message has a stable file path, so a restart can safely encounter it again. It commits the Kafka offset after the Parquet file is closed and synced.
- Command summaries are JSON. Worker logs include stage events, offsets, throughput, and consumer lag. Capture summaries retain reconnect gaps; a reconnect does not recover missing upstream history.

The first Parquet writer intentionally uses one small file per message to make its commit/restart behavior easy to inspect. A later compaction step can combine small files.

## Install and start

    ./.venv/Scripts/python.exe -m pip install -e ".[m1,dev]"
    docker compose --profile m1 up -d --wait
    ./.venv/Scripts/python.exe -m blockpulse kafka-init

The m1 extra pins confluent-kafka and pyarrow. The profile limits the broker to one GiB of memory and stores its local log in a Docker volume.

To collect a bounded live session and send each message to both local JSONL and Kafka:

    ./.venv/Scripts/python.exe -m blockpulse capture-kafka --duration 60 --url wss://node203.sv1.mempool.space/api/v1/ws --output data/raw/m1-live

This regional WebSocket endpoint was verified during the Phase 0 connectivity check; availability can change. See the [connectivity findings](experiments/2026-10-08-source-connectivity.md). The command reports Kafka acknowledgements in the capture summary.

## Run a recorded interval through the pipeline

For a clean demonstration, use a broker topic that does not already contain the same capture. A fresh Kafka data volume is easiest before the first experiment; do not delete a volume if you need its existing Kafka records.

    $run = [guid]::NewGuid().ToString("N")
    $source = "data/raw/phase0-10min-c71a01e1cdd340ea9d300a3506c06ee1/messages.jsonl"
    $rawArchive = "data/kafka/archive-$run"
    $parquetArchive = "data/archives/m1-$run"
    $processed = "data/processed/m1-$run"

    ./.venv/Scripts/python.exe -m blockpulse kafka-replay $source
    ./.venv/Scripts/python.exe -m blockpulse kafka-archive --output $rawArchive --group-id "raw-$run" --max-messages 402
    ./.venv/Scripts/python.exe -m blockpulse kafka-enrich --group-id "enrich-$run" --max-messages 402
    ./.venv/Scripts/python.exe -m blockpulse kafka-parquet --output $parquetArchive --group-id "parquet-$run" --max-messages 402
    ./.venv/Scripts/python.exe -m blockpulse process "$rawArchive\messages.jsonl" --output $processed

Use the same group ID and archive directory when resuming a consumer. Use a stable enrichment group when continuing to process new live messages. A new group starts from the earliest retained record, so it can replay old topic data too. When capturing directly, use capture-kafka with the same URL and capture bounds as capture; then run the downstream consumers against the topic.

## Verified result (2026-10-09)

The saved ten-minute capture contained 402 messages (12,814,888 bytes). All 402 were published to Kafka, archived, and matched the original envelopes by message_id and content. Enrichment emitted 6,983 normalized lifecycle events with zero REST requests because this capture already contained complete added-transaction data. The Parquet run wrote 402 unique message files containing those 6,983 events, compressed to 6,013,901 bytes, with zero invalid records and zero consumer lag. Restarting the same Parquet group consumed zero new records and left the dataset checksum and file count unchanged.

Processing the JSONL archive produced the same 2,732 feature records and 6,983 event records as direct-file processing. The normalized records matched exactly; both runs had zero processing issues. Kafka can interleave records across partitions, so compare message content and derived records rather than expecting a byte-identical file order.

The full automated suite passes: 62 passed, one optional loopback WebSocket test skipped. A synthetic WebSocket source was also captured to a temporary Kafka topic through the real local broker and received one acknowledged message. The execution environment denied public WebSocket access with WinError 5, so a live public-source smoke test remains to run from the user's PowerShell session. The bounded REST fallback is covered with mocked response, rate-limit, request-budget, and failure tests.

M1 is complete for the local replayable pipeline. A public live-source smoke test is an environment check, not a reason to skip the saved-data replay path.
