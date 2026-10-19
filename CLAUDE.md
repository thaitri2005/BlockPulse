# Working on BlockPulse

The project owner is building this to learn data engineering. Optimize for their understanding, not only for working code.

## How we work

- **Small steps.** One roadmap step (for example, DE3) at a time, ending in passing tests, a doc update, and a commit. Don't land 1,000+ line changes in one go.
- **Explain as you build.** Name each new concept briefly when it first appears (consumer group, idempotent sink, migration, and so on). Point to the exercise in `docs/learning/README.md` that covers it, or add one.
- **Ask before adding infrastructure** (new services, dependencies, cloud resources). Decisions and scope are in `docs/PROJECT_PLAN.md` §3 and §4.
- **Always leave a handoff.** Before stopping, update the status table and "Next session" list in `docs/PROJECT_PLAN.md` §2-3, and tell the user what's running, for example the Kafka container.

## Where things are

- Plan, status, roadmap (DE1-DE6), decisions: `docs/PROJECT_PLAN.md`. The purpose is data engineering skill (D18).
- What's built: `docs/ARCHITECTURE.md`. How to run each part: `docs/guides/01-...08-*.md`. Keep them in sync with the code.
- Evidence for every measured claim: `docs/experiments/YYYY-MM-DD-*.md`. The README's "What's done" table must only cite proven numbers.
- `docs/archive/original-design.md` is the historical design; don't update it.

## Commands (Windows, PowerShell)

```powershell
.\.venv\Scripts\python.exe -m pytest -q          # with BLOCKPULSE_TEST_DSN set (see docs/guides/04-postgres-api-dashboard.md): 133 passed, 1 skipped
.\.venv\Scripts\python.exe -m blockpulse --help
docker compose --profile m1 up -d --wait          # Kafka (1 GiB limit); `stop` when done
docker compose --profile m2 up -d --wait          # Postgres + Grafana (~900 MB); `stop` when done
```

Rebuild the whole M3 chain and compare with the recorded artifacts: `powershell -File scripts/reproduce_m3.ps1 [-Manifest data/datasets/dataset-<id>.json]` (`blockpulse verify-dataset <manifest>` checks a manifest alone). Model splits read from the Parquet lake when it matches the manifest; manifests built before 2026-10-19 keep file order and read JSONL only, so old results reproduce exactly. The champion detector is in `data/detectors/registry.jsonl` (`blockpulse detectors status`); the rules are champion until a model clears the gate.

Unattended collection uses one implementation (`blockpulse collect-loop`, `process-pending`, `sync-pending` in `collector.py`) for the laptop task and AWS. See docs/guides/07-collection.md and docs/guides/08-aws.md.

Infrastructure checks (no AWS account needed):

```powershell
cd infra/aws/terraform; terraform fmt -check -recursive; terraform validate; terraform test   # mocked provider
bash infra/aws/tests/run_container_tests.sh       # from the repo root: Amazon Linux 2023 + systemd rehearsals (Docker)
.\.venv\Scripts\shellcheck.exe infra/aws/instance/*.sh infra/aws/tests/*.sh
```

The live WebSocket needs `--url wss://node203.sv1.mempool.space/api/v1/ws`, because the default host times out from this network. The agent runtime may be blocked from public WebSockets entirely; ask the user to run live captures from their own shell.

## Dependencies

`pyproject.toml` declares direct dependencies; `requirements.lock` pins every package with hashes for Windows and Linux. The venv is isolated (no system site-packages). After changing `pyproject.toml`:

```powershell
.\.venv\Scripts\python.exe -m uv pip compile pyproject.toml --extra all --universal --python-version 3.11 --generate-hashes --no-header --annotation-style line -o requirements.lock
.\.venv\Scripts\python.exe -m uv pip sync requirements.lock; .\.venv\Scripts\python.exe -m uv pip install --no-deps -e .
```

CI fails if the lock doesn't match `pyproject.toml`. The AWS bundle ships the same pins as `constraints.txt`.

## Conventions

- **Never run `terraform apply` or `destroy`, or create anything in AWS, from an agent session.** Prepare and test; the owner deploys (decision D17).
- Files that run on Linux (`*.sh`, systemd units, `*.tf`, templates) must have LF line endings. Python's `write_text` on Windows writes CRLF unless you pass `newline="\n"`; `.gitattributes` and the bundle builder guard against it.
- `data/` is git-ignored local evidence. Never delete or overwrite run directories; commands create new ones.
- Postgres tests drop the `public` schema: only ever point `BLOCKPULSE_TEST_DSN` at `blockpulse_test`.
- Schema changes are new numbered files in `src/blockpulse/migrations/`; never edit an applied one.
- The lake contract in `src/blockpulse/lake.py` is the single schema source. After changing it, regenerate `infra/aws/terraform/lake_schema.json` with `python -m blockpulse lake-schema --output infra/aws/terraform/lake_schema.json` (a test fails otherwise) and bump `LAKE_SCHEMA_VERSION` if existing files become incompatible.
- SQL in `analytics/queries/` must run in both DuckDB and Athena (Trino): stick to standard SQL, and test with `blockpulse lake-query`.
- Tests must not hit the public API. Use the fixtures in `examples/fixtures/` or mocks, as the existing tests do.
- Amounts are integer satoshis. Missing values are errors, never zero.
- Feature or rule changes bump `FEATURE_SET_VERSION` / `RULESET_VERSION`.
- Never describe a flag or score as fraud, illicit activity, or accuracy. Unlabeled data supports only structural statements (see `docs/guides/02-structural-rules.md#review-rubric`).
- Commit messages: short conventional prefix (`feat:`, `docs:`, `fix:`, `test:`) plus a one-line summary. Commits are authored as the owner with no AI attribution trailer; push to `origin main` when the owner asks.
