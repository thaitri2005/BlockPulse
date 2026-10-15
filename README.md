# BlockPulse

BlockPulse is a **Bitcoin mempool data pipeline**, built to practice data engineering on a real live source. It captures the public mempool feed, preserves every raw message, processes it into a partitioned Parquet lake behind a data-quality gate, and makes it queryable with SQL locally (DuckDB) and in AWS (Athena). Explainable rules and an Isolation Forest screen transactions for unusual structure, which gives the data a reason to flow.

A flag means *unusual structure*. It is not a fraud probability or evidence of wrongdoing.

## What it demonstrates

| Data engineering skill | Where to look |
| --- | --- |
| Streaming ingestion with replay | WebSocket capture with gap accounting; Kafka topics with restart-safe consumers ([M1](docs/M1_KAFKA.md)) |
| Lake storage layout | Hive-partitioned Parquet (`dt`/`hour`), right-sized files, idempotent rebuilds ([lake](docs/LAKE.md)) |
| Data quality and contracts | Typed table contracts, quarantine with reasons, per-session quality reports ([lake](docs/LAKE.md#the-quality-gate)) |
| SQL analytics | The same queries in DuckDB and Athena, with partition projection and a scan cap ([lake](docs/LAKE.md)) |
| Serving and observability | Postgres with duplicate-safe loads, read-only API, Grafana ([M2](docs/M2_POSTGRES.md)) |
| Reproducible ML evaluation | Checksummed dataset manifests, chronological splits, a gated detector registry ([M3](docs/M3_REPORT.md)) |
| Infrastructure as code | Terraform for a locked-down, self-expiring AWS collector, tested with a mocked provider ([AWS](docs/AWS_DEPLOY.md)) |
| Operations | Hardened systemd services rehearsed in Amazon Linux 2023 containers; CI ([AWS](docs/AWS_DEPLOY.md#what-has-and-hasnt-been-verified)) |

Planned next: replay load tests and failure drills, dbt models, an Airflow backfill, Kubernetes ([roadmap](docs/PROJECT_PLAN.md#data-engineering-roadmap-2026-10-16)).

## Where we are

| Milestone | Status |
| --- | --- |
| M0: validate the source | ✅ Done: ten-minute live capture, replay is byte-identical |
| M1: Kafka ingest, archive, replay | ✅ Done locally. Open item: live `capture-kafka` smoke test from your own shell |
| M2: features, baseline, API | ✅ Done locally: Postgres store, read-only API, Grafana dashboard, `trace` ([guide](docs/M2_POSTGRES.md)) |
| M3: ML comparison | ✅ Done locally ([report](docs/M3_REPORT.md)): the rules stay champion; the Isolation Forest is a registered challenger that failed the promotion gate |
| Lake and Athena (DE1-DE2) | ✅ Partitioned Parquet with a quality gate; SQL in DuckDB and Athena ([lake](docs/LAKE.md)) |
| AWS data collector (E3a) | ✅ Ready to deploy, about $3 for 96 hours ([runbook](docs/AWS_DEPLOY.md)); verified offline, not yet run in a real account |
| DE3: load test, failure drills, CI | **Next** |

The detailed tracker and the next concrete steps are in [project plan §21](docs/PROJECT_PLAN.md#21-current-status-and-how-to-resume).

## Quick start (offline, about 2 minutes)

Requires Python 3.11+. Docker is only needed for the Kafka and Postgres paths.

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

## Query it (Postgres, API, dashboard)

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[m2,dev]"
powershell -File scripts/demo_m2.ps1     # offline demo on synthetic data (needs: docker compose --profile m2 up -d --wait postgres)
```

See the [M2 guide](docs/M2_POSTGRES.md) to load the real capture, run the API at `http://127.0.0.1:8000/docs`, and open Grafana at `http://127.0.0.1:3000`.

## Collect data unattended

The same collector runs on the laptop, under Airflow, or on a small self-expiring AWS instance. For a multi-day run, AWS is the practical option:

```powershell
.\.venv\Scripts\python.exe infra\aws\build_bundle.py
cd infra\aws\terraform; Copy-Item terraform.tfvars.example terraform.tfvars   # set alert_email
terraform init; terraform plan -out tfplan; terraform apply tfplan
```

Everything else (checking status, downloading, tearing down) is in the [AWS runbook](docs/AWS_DEPLOY.md). Laptop and Airflow options: [collection](docs/M4_COLLECTION.md).

## Documentation map

| Read this | When you want to |
| --- | --- |
| [Project plan](docs/PROJECT_PLAN.md) | Recall the idea, scope, milestones, decisions, and current status. This is the canonical reference. |
| [Current architecture](docs/ARCHITECTURE_CURRENT.md) | Understand what is actually built: commands, modules, topics, limits |
| [Learning path](docs/learning/README.md) | Work through the code exercise by exercise |
| [Phase 0](docs/PHASE0.md) (M0) | Capture/process details, file formats, exit codes |
| [Phase 1](docs/PHASE1.md) (M2.0) | The structural rules, the synthetic lab, and how to read flags |
| [M1 Kafka guide](docs/M1_KAFKA.md) | Run the Kafka pipeline end to end |
| [M2 guide](docs/M2_POSTGRES.md) | Load results into Postgres; API, dashboard, and trace |
| [M3 dataset](docs/M3_DATASET.md) | How the train/calibration/test split is built and verified |
| [M3 model](docs/M3_MODEL.md) | The Isolation Forest: what it does, how it is trained, and its limits |
| [M3 evaluation](docs/M3_EVALUATION.md) | Alert-budget thresholds and comparing the model with the rules |
| [M3 registry](docs/M3_REGISTRY.md) | Detector bundles, promotion gate, and rollback |
| [M3 report](docs/M3_REPORT.md) | What the evidence supports and the champion decision |
| [Data collection](docs/M4_COLLECTION.md) | The shared collector and its three runners |
| [Lake](docs/LAKE.md) | Parquet layout, quality gate, DuckDB and Athena queries |
| [AWS runbook](docs/AWS_DEPLOY.md) | Deploy, monitor, download, and tear down the AWS collector; costs; what is verified |
| [Review rubric](docs/PHASE1_REVIEW_RUBRIC.md) | Review flagged rows without over-claiming |
| [Experiments](docs/experiments/) | Dated measurements and evidence behind each claim |
| [CLAUDE.md](CLAUDE.md) | Working conventions for AI-assisted sessions |
