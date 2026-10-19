# BlockPulse

A Bitcoin mempool data pipeline, built to practise data engineering on a real live source. It captures the public mempool feed, keeps every raw message, processes it into a partitioned Parquet lake behind a data-quality gate, and makes it queryable with SQL locally (DuckDB) and in AWS (Athena). Explainable rules and an Isolation Forest screen transactions for unusual structure, which gives the data a reason to flow.

A flag means *unusual structure*. It is not a fraud probability or evidence of wrongdoing.

## What's done

| Area | What exists | Proven by |
| --- | --- | --- |
| **Ingestion and replay** | WebSocket capture with gap accounting; a Kafka path with enrichment and restart-safe consumers | Replays are byte-identical; a 402-message Kafka replay reproduced all 2,732 features ([03](docs/guides/03-kafka.md)) |
| **Cloud collection** | Terraform for a locked-down, self-expiring EC2 collector (no inbound ports, IMDSv2, least-privilege IAM, budget alert) | Ran 89 hours unattended, 0 failures, **2.65 M transactions**, torn down, **about $0.80** ([08](docs/guides/08-aws.md)) |
| **Lake and data quality** | Hive-partitioned Parquet (`dt`/`hour`), typed table contracts, quarantine with reasons, per-session quality reports | **8.5 M rows** across 94 sessions, 0 quarantined ([05](docs/guides/05-lake-and-athena.md)) |
| **SQL analytics** | The same queries in DuckDB and Athena; Glue partition projection, 1 GiB scan cap | Shared queries run locally and in Athena ([05](docs/guides/05-lake-and-athena.md)) |
| **Serving** | Postgres with duplicate-safe loads and lifecycle projection, read-only API, Grafana dashboard | Re-loading adds no rows; dashboard queries verified ([04](docs/guides/04-postgres-api-dashboard.md)) |
| **Reproducible evaluation** | Checksummed manifests with chronological splits, Isolation Forest, threshold calibration, synthetic sweep, gated detector registry | Two recorded runs rebuild exactly (6 of 6 checks); rules stay champion on 2.65 M rows ([06](docs/guides/06-ml-evaluation.md)) |
| **Testing and CI** | 127 pytest tests, Terraform tests with a mocked AWS provider, Amazon Linux 2023 + systemd rehearsals, GitHub Actions | All passing locally ([CI workflow](.github/workflows/ci.yml)) |

**Not done yet:** a replay load test and failure drills, a pinned clean environment, dbt models, an Airflow backfill DAG, and Kubernetes. No accuracy figures exist, because there are no labels. See the [plan](docs/PROJECT_PLAN.md) for status and order.

## Quick start (offline, a few minutes)

Python 3.11+. Docker only for the Kafka and Postgres parts.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[all]"
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m blockpulse process examples/fixtures/mempool_messages.jsonl
```

The fixture is synthetic. Commands write to new folders under `data/` and never overwrite earlier runs.

Capture 60 seconds of live data and process it:

```powershell
.\.venv\Scripts\python.exe -m blockpulse capture --duration 60 --url wss://node203.sv1.mempool.space/api/v1/ws --output data/raw/my-capture
.\.venv\Scripts\python.exe -m blockpulse process data/raw/my-capture/messages.jsonl
```

The regional node is passed explicitly because the default host times out from some networks ([why](docs/experiments/2026-10-08-source-connectivity.md)). `python -m blockpulse --help` lists every command.

## Documentation

| Read | For |
| --- | --- |
| [Plan and status](docs/PROJECT_PLAN.md) | Purpose, what's done, roadmap, decisions, risks |
| [Architecture](docs/ARCHITECTURE.md) | Data flow, commands, modules, technology |
| [01 · Capture and process](docs/guides/01-capture-and-process.md) | Recording the feed, file formats, offline processing |
| [02 · Structural rules](docs/guides/02-structural-rules.md) | The rule baseline, the synthetic lab, the review rubric |
| [03 · Kafka](docs/guides/03-kafka.md) | Streaming ingest, enrichment, archives, replay |
| [04 · Postgres, API, dashboard](docs/guides/04-postgres-api-dashboard.md) | Query store, read-only API, Grafana, tracing one transaction |
| [05 · Lake and Athena](docs/guides/05-lake-and-athena.md) | Parquet layout, quality gate, DuckDB and Athena |
| [06 · ML evaluation](docs/guides/06-ml-evaluation.md) | Manifests, model, threshold, sweep, registry, results |
| [07 · Collection](docs/guides/07-collection.md) | Unattended capture on the laptop or AWS |
| [08 · AWS](docs/guides/08-aws.md) | Deploy, monitor, download, tear down; costs; what was verified |
| [Experiments](docs/experiments/) | Dated measurements behind every claim |
| [Learning path](docs/learning/README.md) | Exercises through the code |

`CLAUDE.md` holds working conventions for AI-assisted sessions.
