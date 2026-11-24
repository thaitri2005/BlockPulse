# Exercise 28: who restarts it? Drills on Kubernetes

The goal is to watch a controller repair a failure nobody told it about, and to read the code that proves nothing was lost. A **drill** is a failure you cause on purpose, in the middle of real work, followed by checks. The drill here kills the enrichment worker's pod while it is consuming from Kafka. It then does nothing to recover; it only waits and measures. Compare that with the Compose drills of [guide 10](../guides/10-failure-drills.md), where the drill restarts what it killed.

The traffic is generated, not recorded: `blockpulse synth-capture` produces capture sessions labelled `source_mode: synthetic`. Counts below describe that generated traffic and must not be mixed with recorded data. The drills have so far been run only in a cloud session (2026-11-24), twice each.

## Run it

You need the cluster and the image from Exercises 25 and 26, with `kubectl` and `helm` on the PATH of the shell that runs Python. The drill installs the chart into its own namespace, `blockpulse-drill`, so your `blockpulse` namespace is untouched.

Terminal 1 (PowerShell). `--keep` leaves the namespace in place afterwards so you can look around:

```powershell
.\.venv\Scripts\python.exe -m blockpulse drill k8s-enrich-kill --keep
```

Terminal 2, as soon as the first has started:

```bash
kubectl -n blockpulse-drill get pods -w
```

Expect about 3.5 minutes. Most of it is generating traffic and verifying, not the failure. Afterwards, clean up:

```bash
kubectl delete namespace blockpulse-drill
```

## Open the output

In terminal 2, find the moment a `bp-worker-...` pod disappears and another with a different suffix appears. The drill prints its checks and saves a JSON report under `data/drills/`. The second recorded run is in the repository at `docs/experiments/reports/de7-run2/k8s-enrich-kill-20261124T112713.json`. Three of its seven checks, shortened:

```json
{"check": "the kill lands mid-stream: some offsets committed, some messages still unprocessed",
 "detail": {"worker_pod": "bp-worker-6479b7f57c-gwf6r",
            "at_kill": {"partitions": 3, "committed": 4501, "end": 30000, "lag": 25499}}}
{"check": "the Deployment replaced the killed pod without anyone restarting it",
 "detail": {"killed": "bp-worker-6479b7f57c-gwf6r", "replacement": "bp-worker-6479b7f57c-rjh2t", "seconds": 1.2}}
{"check": "duplicates are bounded (replayed since the last checkpoint only)",
 "detail": {"enriched_records": 30117, "duplicate_records": 117}}
```

Read them as a story. A **consumer group** is a named reader of a topic whose position, its committed **offsets**, is stored in Kafka. **Lag** is the end of the topic minus that position: how much work is left. At the kill, 4,501 of 30,000 messages were committed. The new pod was running 1.2 s later and lag reached 0 in 12.9 s. The enriched topic ended with 30,117 records for 30,000 inputs: 117 messages had been processed but not yet committed when the pod died, so the new pod did them again. The last check in the file shows why that is acceptable: the Parquet sink read 30,117 records and holds 30,000 files. The first run of the same day had 413 duplicates ([DE7 report](../experiments/2026-11-24-de7-kubernetes.md)); the number varies, and it is bounded, not zero.

## Read one function

Open `src/blockpulse/k8s_drills.py` and read `drill_enrich_kill()`. Trace the kill and what follows:

```python
worker, at_kill = _start_worker_and_wait_for_progress(checks)
killed_at = time.monotonic()
if worker:
    # --grace-period=0 --force: the container gets SIGKILL, so no clean shutdown and no final checkpoint.
    _kubectl("delete", "pod", worker["name"], "--grace-period=0", "--force", check=False)
replacement = _wait(lambda: (pod := _running_worker()) and worker and pod["name"] != worker["name"] and pod, 120)
```

Three things to notice.

- **When the kill happens.** `_start_worker_and_wait_for_progress()` polls the group until some offsets are committed and lag is still above 0. That is a **condition-triggered fault**: the kill is tied to the state of the system, not to a timer, so it lands mid-stream on a fast machine and a slow one alike.
- **How it kills.** A normal pod deletion is **graceful**: the container gets SIGTERM and time to finish (`terminationGracePeriodSeconds: 30` in `worker.yaml`, where the comment says the worker uses it to commit its offsets). `--grace-period=0 --force` skips that, which is the harder case to survive.
- **What it does next.** Nothing. It waits for a running worker pod with a *different name*. There is no line that starts one.

Now open `src/blockpulse/drills.py` and read `drill_enrich_crash()`. After `worker.kill()` comes `resumed = _enricher(group, bootstrap)`: the Compose drill starts the replacement itself. On Kubernetes that job belongs to a **controller**, a loop inside the cluster that compares what exists with what was declared and acts on the difference. Find the declaration in `infra/k8s/chart/blockpulse/templates/worker.yaml`.

## Change one small thing

Make the kill graceful. In `drill_enrich_kill()`, temporarily remove `"--grace-period=0", "--force"` from the delete call, then run the logic tests and the drill:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_k8s_drills.py
.\.venv\Scripts\python.exe -m blockpulse drill k8s-enrich-kill
```

Before it finishes, write down what you expect `duplicate_records` to be, and why. Compare with the report. This has not been run, so there is no recorded answer; whatever you see is new evidence and belongs in `docs/experiments/`, labelled synthetic. Restore the file afterwards (`git checkout src/blockpulse/k8s_drills.py`).

The tests in `tests/test_k8s_drills.py` fake `kubectl` and `helm`, so they pass without a cluster. Read `test_an_empty_group_is_not_mistaken_for_a_drained_one` and work out which false pass it prevents.

## Question to carry forward

The Compose drill restarts the worker itself and the Kubernetes drill doesn't. Which line of the chart is doing that job, and what would happen if the worker were a bare pod instead of a Deployment?

## Verified

Run here on 2026-11-24 (Linux, no cluster): `python -m pytest -q tests/test_k8s_drills.py` (8 passed); `python -m blockpulse drill --help` (confirms the `k8s-enrich-kill` name and the `--keep` and `--messages` options); `helm template bp infra/k8s/chart/blockpulse` (the worker renders as a Deployment with `replicas: 0` and `strategy: Recreate`). `k8s_drills.py`, `drills.py` and `worker.yaml` were read; the quoted lines are from `k8s_drills.py`. Not run here: the drill, every `kubectl` command, and the graceful-kill experiment. The JSON is copied, shortened, from `docs/experiments/reports/de7-run2/k8s-enrich-kill-20261124T112713.json`; the 12.9 s and 413 figures are from the same report file and from `docs/experiments/2026-11-24-de7-kubernetes.md`. All of it is synthetic traffic in a single-node k3d cluster in a cloud session.
