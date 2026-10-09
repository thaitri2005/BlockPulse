# BlockPulse

BlockPulse is a Bitcoin transaction anomaly pipeline in development. It captures mempool observations, saves original messages, turns recorded transactions into reproducible features, and applies a first explainable structural screen offline. Later stages will add stronger baselines, an API, and Grafana.

The project is a practical way to learn streaming data engineering, MLOps, reliability, networking, and deployment while keeping hardware requirements and cloud spending manageable. An anomaly score describes unusual behavior; it is not a probability of fraud or evidence of wrongdoing.

**Current status:** Local Phase 0, the Phase 1 structural baseline, and M1 ingestion/archive/replay are complete. M1 adds live capture-to-Kafka while preserving JSONL, bounded missing-field enrichment, a raw JSONL archive, and a restart-safe Parquet archive. A 402-message saved run reproduced the same 2,732 feature rows and 6,983 event rows as direct processing. The next milestone is M2 queryable results and an API/dashboard. The rules are not calibrated and make no claim about wrongdoing. See the [validation report](docs/experiments/2026-10-09-phase0-phase1-validation.md) and [M1 Kafka results](docs/M1_KAFKA.md).

## Run Phase 0

The project environment is prepared at `.venv` in this workspace. On a fresh checkout, use Python 3.11+ and install:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

Process the included **synthetic** capture without network access:

```powershell
.\.venv\Scripts\python.exe -m blockpulse process examples/fixtures/mempool_messages.jsonl
```

Capture a real stream for up to 60 seconds:

```powershell
.\.venv\Scripts\python.exe -m blockpulse capture --duration 60 --url wss://node203.sv1.mempool.space/api/v1/ws --output data/raw/first-capture
```

Then process the saved messages offline:

```powershell
.\.venv\Scripts\python.exe -m blockpulse process data/raw/first-capture/messages.jsonl
```

Each command prints its output directory and summary. Processing writes normalized events, a feature table (`features.csv` and `features.jsonl`), and an error report. Existing output directories are never overwritten; omit `--output` for a new automatically named run.

The command above uses the endpoint verified from this machine on 2026-10-08. The application's default remains `wss://mempool.space/api/v1/ws`. Regional-node availability can change; see the [connectivity findings](docs/experiments/2026-10-08-source-connectivity.md) for the diagnosis and test results.

See [Phase 0 commands and behavior](docs/PHASE0.md) for limits, file formats, test commands, and verification results.

See [Phase 1 structural screening](docs/PHASE1.md) for the rules, interpretation, output files, and how to run it on recorded data.

See [Current architecture and data flow](docs/ARCHITECTURE_CURRENT.md) for the detailed explanation of technologies, modules, schemas, artifacts, and what is or is not implemented at this stage.

The [M1 Kafka guide](docs/M1_KAFKA.md) documents the complete local pipeline, its run commands, and measured verification.

## Project reference

The [project plan and system description](docs/PROJECT_PLAN.md) is the canonical reference for the idea, architecture, scope, milestones, cost assumptions, and outstanding decisions. Start there when returning to the project.

The intended first deliverable runs locally: ingestion, Kafka, transaction features, baseline detection, replayable archives, queryable results, and observability. Entity analysis, Kubernetes, and an ephemeral AWS deployment are later extensions.

The smaller [Exercise 1: follow one transaction](docs/learning/01_TRANSACTION_PATH.md) is also available and shares its feature function with Phase 0. It uses only Python's standard library:

```powershell
python examples/01_trace_transaction.py --sample
python examples/01_trace_transaction.py --replay
python examples/01_trace_transaction.py
```

`--sample` uses a labeled synthetic fixture; `--replay` reads the last saved input; running without a flag fetches one live transaction.
