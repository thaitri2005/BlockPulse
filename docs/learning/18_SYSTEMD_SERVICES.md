# Exercise 18: running a service properly on Linux

The goal is to understand what turned `blockpulse collect-loop` from a command you start by hand into something that ran for 89 hours on AWS with nobody watching. The collector itself is ordinary Python. What keeps it running, stops it cleanly and limits what it can touch is **systemd**, the program on most Linux systems that starts and supervises background services. You cannot run systemd on Windows, so this exercise runs the same commands the units run, on a scratch folder, and then reads the unit files.

Three terms. A **unit** is a text file that tells systemd about one thing to manage. A **service** unit describes a process to run. A **timer** unit starts a service on a schedule, like a scheduled task.

## Run it

The AWS instance runs two jobs: a long-lived capture service, and a timer that every 10 minutes runs `process-pending` and then `sync-pending`. The second job is the one you can run here. From PowerShell in the project folder:

```powershell
$py = ".\.venv\Scripts\python.exe"
$root = "$env:TEMP\bp-learn"
& $py -m blockpulse synth-capture --output "$root\raw\session-synth-20260101T000000Z" --messages 2000 --seed 1
& $py -m blockpulse process-pending --root $root
& $py -m blockpulse process-pending --root $root
```

`synth-capture` writes a generated capture, labelled `source_mode: synthetic`. It stands in for a finished session; nothing in it was observed on the network. The folder name must start with `session-`, because that is how the collector finds captures.

## Open the output

The first `process-pending` reports each stage it ran for the session (`"processed": true`, `"screened": true`, `"lake": true`, `"compacted": true`). The second prints:

```json
{
  "sessions": [],
  "failed": [],
  "backfill_status_changes": null
}
```

Nothing was pending, so nothing was done. A job is **idempotent** when running it again leaves the same result as running it once. That property is what makes a 10-minute timer safe: the timer does not need to know whether a new session has finished, and a run that is interrupted is simply repeated.

Now simulate a second processor already at work:

```powershell
Set-Content -Encoding ascii "$root\.collector.lock" "1 nobody 2026-01-01"
& $py -m blockpulse process-pending --root $root
Remove-Item "$root\.collector.lock"
```

The command refuses with `another processor holds ...\.collector.lock` and exits with code 1. Read `_Lock` in [`collector.py`](../../src/blockpulse/collector.py) to see when a lock counts as stale and is taken over.

## Read one unit file

Open [`infra/aws/instance/systemd/blockpulse-collect.service`](../../infra/aws/instance/systemd/blockpulse-collect.service) and trace one decision: what happens when someone runs `systemctl stop`.

```ini
# Planned stops (deadline, session limit, low disk) exit 0 and are not restarted; crashes are.
Restart=on-failure
RestartSec=60
# SIGINT lets the capture write its summary before exiting (exit code 130 means a clean interrupt).
KillSignal=SIGINT
TimeoutStopSec=60
SuccessExitStatus=130
```

A **signal** is a small message the operating system delivers to a process. By default systemd stops a service with SIGTERM. This unit sends **SIGINT** instead, the same signal Ctrl+C sends, because the capture already knows how to finish on Ctrl+C. Follow it through the code:

1. Python turns SIGINT into a cancellation of the running capture. In [`capture.py`](../../src/blockpulse/capture.py), `except asyncio.CancelledError: reason = "interrupted"` records why, and the `finally` block still closes the open gap and writes `summary.json`.
2. In `collect_loop()` in `collector.py`, `if summary["stop_reason"] == "interrupted"` ends the loop instead of starting the next session.
3. In [`cli.py`](../../src/blockpulse/cli.py), `code = 130 if summary["stop_reason"] == "interrupted" else 0` sets the exit code.
4. Back in the unit, `SuccessExitStatus=130` tells systemd that 130 is a normal ending, so `Restart=on-failure` does not start it again. `TimeoutStopSec=60` is how long systemd waits before it kills the process outright.

The summary matters because a capture folder without `summary.json` is not a finished capture: `finished_captures()` skips it, and an hour of data would never be processed.

The lower half of the file is **sandboxing**: settings that restrict what the process can do even if it misbehaves. `User=blockpulse` runs it without root. `ProtectSystem=strict` makes the whole file system read-only for it, and `ReadWritePaths=/srv/blockpulse` reopens the one folder it needs. `CapabilityBoundingSet=` with nothing after it removes every special privilege. `MemoryMax=1G` caps its memory.

Then read the pair [`blockpulse-process.timer`](../../infra/aws/instance/systemd/blockpulse-process.timer) and [`blockpulse-process.service`](../../infra/aws/instance/systemd/blockpulse-process.service). The service has `Type=oneshot`: it runs to completion and exits, and the timer starts it again 10 minutes later (`OnUnitActiveSec=10min`). Its first `ExecStart=` begins with `-`, so the upload still runs if one session failed to process.

## Change one small thing

In `blockpulse-collect.service`, delete the line `SuccessExitStatus=130`. Before running anything, predict what `systemctl stop` would now report, and what `Restart=on-failure` would do a minute later.

Then find the assertion that catches it. In [`infra/aws/tests/systemd_e2e.sh`](../../infra/aws/tests/systemd_e2e.sh), the stop check requires the unit to end `inactive` with result `success`. If Docker is free, confirm it:

```powershell
bash infra/aws/tests/run_container_tests.sh
```

Put the line back, then rerun the collector tests:

```powershell
& $py -m pytest -q tests/test_collector.py
```

## Question to carry forward

`blockpulse-deadline.timer` checks every 15 minutes whether the deadline has passed, and [`deadline-check.sh`](../../infra/aws/instance/deadline-check.sh) then stops the capture, runs one last process-and-upload, and powers off. Why is the capture stopped *before* the final upload, and what would be lost if the order were reversed?

## What was verified

Run on 2026-11-24 on Linux, in a fresh scratch root (and again after `synth.py` began writing a full capture summary): `synth-capture`, `process-pending` twice (the second returned empty `sessions`), the hand-written lock (refused, exit code 1), and `pytest tests/test_collector.py` (passed, with the one Postgres test skipped). `systemd-analyze security --offline=true` on both service files, with systemd 255, rated each 1.3 "OK"; the AWS guide records 1.4 for the capture service on Amazon Linux 2023, and scores differ between systemd versions. **Not run here:** `systemctl` anything, `run_container_tests.sh` and `systemd_e2e.sh` (they need Docker), and the experiment of deleting `SuccessExitStatus=130`. The stop sequence in "Read one unit file" was traced by reading the code and the end-to-end script, not by stopping a live service.
