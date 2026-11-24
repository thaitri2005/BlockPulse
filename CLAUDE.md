# Working on BlockPulse

The project owner is building this to learn data engineering. Optimize for their understanding, not only for working code.

## How we work

- **Small steps.** One roadmap step (for example, DE3) at a time, ending in passing tests, a doc update, and a commit. Don't land 1,000+ line changes in one go.
- **Explain as you build.** Name each new concept briefly when it first appears (consumer group, idempotent sink, migration, and so on). Point to the exercise in `docs/learning/README.md` that covers it, or add one.
- **Ask before adding infrastructure** (new services, dependencies, cloud resources). Decisions and scope are in `docs/PROJECT_PLAN.md` §3 and §4.
- **Always leave a handoff.** Before stopping, update the status table and "Next session" list in `docs/PROJECT_PLAN.md` §2-3, and tell the user what's running, for example the Kafka container.

## Where things are

`docs/README.md` maps the documentation. Keep each kind of content in its place:

- `README.md` explains the project: what it is, the flow (with diagrams), what outputs mean, results, limits. It is not a changelog; cite only proven numbers.
- `docs/ARCHITECTURE.md`: what is built and how it connects. `docs/guides/01-...14-*.md`: how to run each part, with its results. Keep both in sync with the code.
- `docs/PROJECT_PLAN.md`: purpose, status, candidate next work, decisions, risks. Keep it short. The purpose is data engineering skill (D18).
- `docs/experiments/YYYY-MM-DD-*.md`: dated evidence and session logs; `docs/experiments/reports/` for small raw output. Logs go here, never in the three files above.
- `docs/archive/`: the original design and the milestone history; don't update them.

## Commands (Windows, PowerShell)

```powershell
.\.venv\Scripts\python.exe -m pytest -q          # with BLOCKPULSE_TEST_DSN set (see docs/guides/04-postgres-api-dashboard.md): 154 passed, 2 skipped (one is Linux-only); without it: 136 passed, 20 skipped; on Linux with `helm` and the test database: 195 passed, 1 skipped (the 16 Helm chart tests skip without `helm`)
.\.venv\Scripts\python.exe -m blockpulse --help
docker compose --profile m1 up -d --wait          # Kafka (1 GiB limit); `stop` when done
docker compose --profile m2 up -d --wait          # Postgres + Grafana + Prometheus (~1.1 GB); `stop` when done
docker compose --profile airflow up -d --wait --build   # Airflow at 127.0.0.1:8080 (~1.0-1.3 GB); DAG data root is data/orchestrated/
bash infra/k8s/cluster-up.sh                      # k3d cluster "blockpulse" (Git Bash or WSL; ~0.4 GB idle); `k3d cluster stop blockpulse` when done
bash infra/k8s/image-load.sh                      # build blockpulse:dev and load it into the cluster
helm upgrade --install bp infra/k8s/chart/blockpulse -n blockpulse --create-namespace --wait --wait-for-jobs   # Kafka, worker, sink, Postgres, API, Prometheus, Grafana (~1.4-1.8 GB with the cluster)
```

Rebuild the whole M3 chain and compare with the recorded artifacts: `powershell -File scripts/reproduce_m3.ps1 [-Manifest data/datasets/dataset-<id>.json]` (`blockpulse verify-dataset <manifest>` checks a manifest alone). Model splits read from the Parquet lake when it matches the manifest; manifests built before 2026-10-19 keep file order and read JSONL only, so old results reproduce exactly. The champion detector is in `data/detectors/registry.jsonl` (`blockpulse detectors status`); the rules are champion until a model clears the gate.

Unattended collection uses one implementation (`blockpulse collect-loop`, `process-pending`, `sync-pending` in `collector.py`) for the laptop task and AWS. See docs/guides/07-collection.md and docs/guides/08-aws.md.

Infrastructure checks (no AWS account needed):

```powershell
cd infra/aws/terraform; terraform fmt -check -recursive; terraform validate; terraform test   # mocked provider
bash infra/aws/tests/run_container_tests.sh       # from the repo root: Amazon Linux 2023 + systemd rehearsals (Docker)
.\.venv\Scripts\shellcheck.exe infra/aws/instance/*.sh infra/aws/tests/*.sh
```

The live WebSocket needs `--url wss://node203.sv1.mempool.space/api/v1/ws`, because the default host times out from this network. Some agent runtimes are blocked from public WebSockets (a cloud session on 2026-11-24 was not); try a 15-second capture before asking the user to run one from their own shell.

## Dependencies

`pyproject.toml` declares direct dependencies; `requirements.lock` pins every package with hashes for Windows and Linux. The venv is isolated (no system site-packages). After changing `pyproject.toml`:

```powershell
.\.venv\Scripts\python.exe -m uv pip compile pyproject.toml --extra all --universal --python-version 3.11 --generate-hashes --no-header --annotation-style line -o requirements.lock
.\.venv\Scripts\python.exe -m uv pip sync requirements.lock; .\.venv\Scripts\python.exe -m uv pip install --no-deps -e .
```

CI fails if the lock doesn't match `pyproject.toml`. The AWS bundle ships the same pins as `constraints.txt`.

## Conventions

- **The project is complete and its dataset is final (decision D32): no further AWS collection.** Don't propose one; work with the data that exists.
- **Never run `terraform apply` or `destroy`, or create anything in AWS, from an agent session.** Prepare and test; the owner deploys (decision D17).
- Files that run on Linux (`*.sh`, systemd units, `*.tf`, templates) must have LF line endings. Python's `write_text` on Windows writes CRLF unless you pass `newline="\n"`; `.gitattributes` and the bundle builder guard against it.
- `data/` is git-ignored local evidence. Never delete or overwrite run directories; commands create new ones.
- Postgres tests drop the `public` schema: only ever point `BLOCKPULSE_TEST_DSN` at `blockpulse_test`.
- Schema changes are new numbered files in `src/blockpulse/migrations/`; never edit an applied one.
- The lake contract in `src/blockpulse/lake.py` is the single schema source. After changing it, regenerate `infra/aws/terraform/lake_schema.json` with `python -m blockpulse lake-schema --output infra/aws/terraform/lake_schema.json` (a test fails otherwise) and bump `LAKE_SCHEMA_VERSION` if existing files become incompatible.
- Kubernetes objects live in the Helm chart `infra/k8s/chart/blockpulse/`; its defaults mirror `compose.yaml`. After changing a template, run `helm lint --strict` and `pytest tests/test_helm_chart.py`. The chart's `files/dashboards/*.json` and `files/prometheus/alerts.yml` are copies of the files under `observability/`; copy them again after editing (tests fail otherwise). In a cloud session, run `cluster-up.sh` and `image-load.sh` with `K3D_CA_BUNDLE=/root/.ccr/ca-bundle.crt` (see docs/guides/14-kubernetes.md).
- SQL in `analytics/queries/` must run in both DuckDB and Athena (Trino): stick to standard SQL, and test with `blockpulse lake-query`.
- dbt runs through `blockpulse dbt <args>` (absolute paths). New models need tests in their YAML; if a mart restates a shared query, check it matches.
- Generated captures (`blockpulse synth-capture`) are labelled `source_mode: synthetic`; never mix their counts with recorded data in a claim. The Kubernetes drills (`drill k8s-*`) install into the namespace `blockpulse-drill` and delete it afterwards.
- Tests must not hit the public API. Use the fixtures in `examples/fixtures/` or mocks, as the existing tests do.
- Amounts are integer satoshis. Missing values are errors, never zero.
- Feature or rule changes bump `FEATURE_SET_VERSION` / `RULESET_VERSION`.
- Never describe a flag or score as fraud, illicit activity, or accuracy. Unlabeled data supports only structural statements (see `docs/guides/02-structural-rules.md#review-rubric`).
- Commit messages: short conventional prefix (`feat:`, `docs:`, `fix:`, `test:`) plus a one-line summary. Commits are authored as the owner with no AI attribution trailer; push to `origin main` when the owner asks.
