# Working on BlockPulse

The project owner is learning streaming data engineering while building this. Optimize for their understanding, not only for working code.

## How we work

- **Small steps.** One milestone step (for example, M2.1) per session, ending in passing tests, a doc update, and a commit. Don't land 1,000+ line changes in one go.
- **Explain as you build.** Name each new concept briefly when it first appears (consumer group, idempotent sink, migration, and so on). Point to the exercise in `docs/learning/README.md` that covers it, or add one.
- **Ask before adding infrastructure** (new services, dependencies, cloud resources). The plan's scope rules are in `docs/PROJECT_PLAN.md` §2 and §19.
- **Always leave a handoff.** Before stopping, update the tracker and "Next session" list in `docs/PROJECT_PLAN.md` §21, and tell the user what's running, for example the Kafka container.

## Where things are

- Canonical plan, status, and next steps: `docs/PROJECT_PLAN.md` (§16 milestones and M2 step table, §21 tracker).
- What's actually built: `docs/ARCHITECTURE_CURRENT.md`. Where the build differs from the plan, the plan has an **As built** note; keep both in sync.
- Evidence for every measured claim: `docs/experiments/YYYY-MM-DD-*.md`.
- Naming: milestones are M0-M4. `PHASE0.md` is M0 and `PHASE1.md` is M2.0; use M-numbers for new work.

## Commands (Windows, PowerShell)

```powershell
.\.venv\Scripts\python.exe -m pytest -q          # 81 passed, 14 skipped; with BLOCKPULSE_TEST_DSN set (see docs/M2_POSTGRES.md): 94 passed, 1 skipped
.\.venv\Scripts\python.exe -m blockpulse --help
docker compose --profile m1 up -d --wait          # Kafka (1 GiB limit); `stop` when done
docker compose --profile m2 up -d --wait          # Postgres + Grafana (~900 MB); `stop` when done
```

Rebuild the whole M3 chain and compare with the recorded artifacts: `powershell -File scripts/reproduce_m3.ps1`. The champion detector is in `data/detectors/registry.jsonl` (`blockpulse detectors status`); the rules are champion until a model clears the gate.

The live WebSocket needs `--url wss://node203.sv1.mempool.space/api/v1/ws`, because the default host times out from this network. The agent runtime may be blocked from public WebSockets entirely; ask the user to run live captures from their own shell.

## Conventions

- `data/` is git-ignored local evidence. Never delete or overwrite run directories; commands create new ones.
- Tests must not hit the public API. Postgres tests drop the `public` schema: only ever point `BLOCKPULSE_TEST_DSN` at `blockpulse_test`.
- Schema changes are new numbered files in `src/blockpulse/migrations/`; never edit an applied one.
- Tests must not hit the public API. Use the fixtures in `examples/fixtures/` or mocks, as the existing tests do.
- Amounts are integer satoshis. Missing values are errors, never zero.
- Feature or rule changes bump `FEATURE_SET_VERSION` / `RULESET_VERSION`.
- Never describe a flag or score as fraud, illicit activity, or accuracy. Unlabeled data supports only structural statements (see `docs/PHASE1_REVIEW_RUBRIC.md`).
- Commit messages: short conventional prefix (`feat:`, `docs:`, `fix:`, `test:`) plus a one-line summary.
