# Unattended data collection

Goal: collect enough traffic for the M3 comparison to mean something, without anyone running commands by hand. Measured need: about 480,000 added transactions a day (3,300 per 10 minutes), so one to three days of continuous collection is already far more than the 15,515 rows collected so far. See the feasibility check in [plan section 20.1](PROJECT_PLAN.md#201-feasibility-check-after-m3-2026-10-15).

## Option A: Windows Task Scheduler (verified, no downloads)

```powershell
cd D:\BlockPulse
docker compose --profile m2 up -d --wait postgres      # the collector loads into Postgres
powershell -File scripts/install_collector.ps1          # installs an hourly task
Get-ScheduledTask BlockPulseCollector | Get-ScheduledTaskInfo    # check last/next run
powershell -File scripts/install_collector.ps1 -Remove  # uninstall (collected data stays)
```

Each hour the task runs `scripts/capture_session.ps1 -Duration 3540`:

1. **Disk guard.** Refuses to start a capture below 15 GiB free.
2. **Capture** for 59 minutes (up to 300 reconnects, 200,000 messages, 400 MiB; the defaults would stop the capture after four dropped connections).
3. **Process, screen, load** into Postgres (`process`, `detect`, `load-results`, `load-events`).
4. **Compact.** Gzip the processed `events.jsonl` after it is loaded. Raw captures stay as the source of truth.
5. **Log** one line to `data/capture_log.csv` (messages, features, flagged, connection errors, stop reason).

Processing takes a few minutes and overlaps the next hour's capture; the task is installed with parallel instances allowed so that no hour is skipped. Each hourly capture runs 59 minutes, so about a minute per hour is uncovered between captures, on top of the small reconnect gaps each session records.

**The laptop must stay awake.** The task cannot collect while Windows sleeps. Set sleep to "Never" while plugged in (Settings, System, Power) and keep it plugged in. A closed lid usually sleeps the machine unless the lid action is changed.

**Disk.** Measured on a 10-minute session and scaled to an hour: raw 126 MB, processed 174 MB (of which `events.jsonl` is 159 MB), screened 30 MB, about 330 MB in total. Gzip shrinks the events file about 6x (26.5 MB to 4.2 MB per 10 minutes), leaving about 200 MB an hour, or 4 to 5 GB a day. Free space was 62 GiB on 2026-10-14, so plan for three to four days and stop earlier if you want headroom. The guard stops new captures at 15 GiB free but cannot free space for you.

Tested on 2026-10-15 with a saved capture (`-FromCapture`, isolated output folder and throwaway database): all stages ran, events were gzipped and the plain file removed, one log row was written, and 5,021 rows reached Postgres. A live hourly capture and the installer itself have not been run by me: the agent environment can't reach the live feed, and registering a persistent task on your machine is yours to do.

## Option B: Airflow (written, not yet run)

An hourly DAG, `blockpulse_collect`, does the same steps with Airflow's scheduling, retries and UI. Files: `orchestration/airflow/` (DAG, Dockerfile, start script) and the `airflow` service in `compose.yaml` (single container, LocalExecutor, metadata in the existing Postgres, 2 GB limit, UI on http://127.0.0.1:8080, user `admin`, password `blockpulse_dev`).

```powershell
docker compose --profile airflow up -d --build      # first run downloads about 1.1 GB
```

**Status: unverified.** The Airflow image could not be downloaded from this environment: Docker's registry lookups failed with "no such host" and layers truncated mid-download (six attempts, 2026-10-15). The DAG and start script are syntax-checked only. Nothing in the container has been started. When the image downloads on your network, test it before relying on it: trigger a run from the UI with the `replay_from` parameter set to an existing `data/raw/<session>/messages.jsonl` (no live feed needed), and check that `data/capture_log.csv` gains a row.

Why this is optional: for a fixed hourly job the Task Scheduler route collects identical data with no additional memory. Airflow adds a scheduler, web server and metadata database (about 1.5 to 2 GB of RAM) and is worth it mainly as an orchestration learning exercise or if the pipeline grows more steps and dependencies.

## After collecting

1. `build-dataset`, then `train-iforest`, `evaluate-iforest`, `sweep`, `detectors package-model`, `detectors promote` (see [M3 report](M3_REPORT.md)). The old manifest and artifacts stay as a comparison point.
2. Hourly sessions of roughly 20,000 rows give many sessions per split; the chronological split by whole session then has real calibration and test windows.
3. Loading is row by row (about 4 ms a row), so a day of data takes tens of minutes to load. Training and evaluation read the screened files directly and do not need Postgres.
