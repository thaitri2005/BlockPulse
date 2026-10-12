# BlockPulse

BlockPulse is a Bitcoin mempool anomaly pipeline, built as a hands-on way to learn streaming data engineering, MLOps, reliability, and deployment on a laptop budget. It records what a public mempool source reports, turns transactions into reproducible features, and flags unusual structures with explainable rules. An ML comparison, an API, and dashboards come later.

A flag means *unusual structure*. It is not a fraud probability or evidence of wrongdoing.

## Where we are

| Milestone | Status |
| --- | --- |
| M0: validate the source | ✅ Done: ten-minute live capture, replay is byte-identical |
| M1: Kafka ingest, archive, replay | ✅ Done locally. Open item: live `capture-kafka` smoke test from your own shell |
| M2: features, baseline, API | 🔄 Rule baseline done; **next: Postgres (M2.1)** |
| M3: ML comparison / M4: reliable release | Not started |

The detailed tracker and the next concrete steps are in [project plan §21](docs/PROJECT_PLAN.md#21-current-status-and-how-to-resume).

## Quick start (offline, about 2 minutes)

Requires Python 3.11+. Docker is only needed for the Kafka path.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[m1,dev]"
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m blockpulse process examples/fixtures/mempool_messages.jsonl
```

The fixture is **synthetic**. Processing it writes events, features, and an error report to a new directory under `data/processed/`. Commands never overwrite an existing output directory.

Capture real data for 60 seconds, then process it offline:

```powershell
.\.venv\Scripts\python.exe -m blockpulse capture --duration 60 --url wss://node203.sv1.mempool.space/api/v1/ws --output data/raw/my-capture
.\.venv\Scripts\python.exe -m blockpulse process data/raw/my-capture/messages.jsonl
```

The default `mempool.space` host times out from this network, so the regional node is passed explicitly ([why](docs/experiments/2026-10-08-source-connectivity.md)). `python -m blockpulse --help` lists every command.

## Documentation map

| Read this | When you want to |
| --- | --- |
| [Project plan](docs/PROJECT_PLAN.md) | Recall the idea, scope, milestones, decisions, and current status. This is the canonical reference. |
| [Current architecture](docs/ARCHITECTURE_CURRENT.md) | Understand what is actually built: commands, modules, topics, limits |
| [Learning path](docs/learning/README.md) | Work through the code exercise by exercise |
| [Phase 0](docs/PHASE0.md) (M0) | Capture/process details, file formats, exit codes |
| [Phase 1](docs/PHASE1.md) (M2.0) | The structural rules, the synthetic lab, and how to read flags |
| [M1 Kafka guide](docs/M1_KAFKA.md) | Run the Kafka pipeline end to end |
| [Review rubric](docs/PHASE1_REVIEW_RUBRIC.md) | Review flagged rows without over-claiming |
| [Experiments](docs/experiments/) | Dated measurements and evidence behind each claim |
| [CLAUDE.md](CLAUDE.md) | Working conventions for AI-assisted sessions |
