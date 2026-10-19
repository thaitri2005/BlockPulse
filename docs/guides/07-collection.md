# Unattended data collection

Goal: collect enough traffic for the M3 comparison to mean something, without anyone running commands by hand. Measured need: about 480,000 added transactions a day (3,300 per 10 minutes), so one to three days of continuous collection is far more than the 15,515 rows collected by hand. See the feasibility check in [plan section 20.1](PROJECT_PLAN.md#201-feasibility-check-after-m3-2026-10-15).

## One collector, three ways to run it

All runners call the same tested commands in [`src/blockpulse/collector.py`](../src/blockpulse/collector.py):

| Command | What it does |
| --- | --- |
| `blockpulse collect-loop` | Captures sessions back to back into `data/raw/session-*` until a deadline, a session limit, or low disk (default stop below 15 GiB free). Each session survives up to 300 reconnects. Stops cleanly on Ctrl+C or SIGINT. |
| `blockpulse process-pending` | For every finished capture not yet handled: process, rule-screen, write the partitioned Parquet lake tables through the quality gate ([lake](LAKE.md)), gzip the bulky events file, add a row to `data/capture_log.csv`. With `--load-db`, also loads into Postgres once per session. Safe to repeat; an interrupted stage is moved aside (never deleted) and redone; a lock stops two processors colliding. |
| `blockpulse collect` | One `collect-loop` session followed by `process-pending`. |
| `blockpulse sync-pending` | Uploads each processed session to S3 once, plus `status/status.json` and the capture log (AWS only). |

| Runner | Best for | Status |
| --- | --- | --- |
| **AWS instance** ([AWS_DEPLOY.md](AWS_DEPLOY.md)) | Round-the-clock collection for a few days without keeping the laptop awake; about $3 for 96 hours | Ready; verified in Amazon Linux 2023 containers and with mocked Terraform tests; not yet deployed |
| **Windows Task Scheduler** (`scripts/install_collector.ps1`) | Collecting on the laptop with no cloud account | Ready; the collector code is unit-tested; the scheduled task itself has not been installed by me |
| **Airflow** (`orchestration/airflow/`, Compose profile `airflow`) | Learning orchestration | Written; never run, because the Airflow image would not download on this network |

## AWS (recommended for a multi-day run)

See [AWS_DEPLOY.md](AWS_DEPLOY.md). The instance captures hourly sessions named `session-aws-<UTC time>`, processes and uploads them every 10 minutes, and powers off at a deadline you choose (default 96 hours). Download with one `aws s3 sync` command, then `blockpulse process-pending --load-db` loads them into the local Postgres if you want the dashboard.

## Windows Task Scheduler (laptop)

```powershell
cd D:\BlockPulse
docker compose --profile m2 up -d --wait postgres      # the hourly task loads into Postgres
powershell -File scripts/install_collector.ps1          # installs an hourly task (59-minute sessions)
Get-ScheduledTask BlockPulseCollector | Get-ScheduledTaskInfo
powershell -File scripts/install_collector.ps1 -Remove  # uninstall; collected data stays
```

Each run calls `scripts/capture_session.ps1`, which is `blockpulse collect --load-db`. Processing overlaps the next hour's capture, so the task allows parallel instances. **The laptop must stay awake**: set sleep to "Never" while plugged in.

Disk, measured on a 10-minute session and scaled to an hour: raw 126 MB, processed 174 MB (of which `events.jsonl` is 159 MB), screened 30 MB. Gzip shrinks the events file about 6x, leaving about 200 MB an hour, or 4 to 5 GB a day. With 62 GiB free (2026-10-14), plan on three or four days.

## Airflow (optional)

The `blockpulse_collect` DAG runs `collect-loop --max-sessions 1` then `process-pending --load-db` every hour, in a single container that keeps Airflow's metadata in the existing Postgres (2 GB limit, UI on http://127.0.0.1:8080, user `admin`, password `blockpulse_dev`):

```powershell
docker compose --profile airflow up -d --build      # first run downloads about 1.1 GB
```

It has never run: on 2026-10-15 Docker's registry lookups failed with "no such host" and image layers truncated mid-download (six attempts). The DAG is syntax-checked only. If the image downloads on your network, trigger one run with `session_seconds` set to 60 and check that `data/capture_log.csv` gains a row before relying on it.

## After collecting

1. `blockpulse build-dataset`, then `train-iforest`, `evaluate-iforest`, `sweep`, `detectors package-model`, `detectors promote` (see the [M3 report](M3_REPORT.md)). Keep the old manifest and artifacts as a comparison point.
2. Hourly sessions of roughly 20,000 rows give many sessions per split, so the chronological split gets real calibration and test windows.
3. Loading into Postgres is row by row (about 4 ms a row), so a day of data takes tens of minutes. Training and evaluation read the screened files directly and don't need Postgres.
