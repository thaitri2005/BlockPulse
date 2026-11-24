"""The failure drills on Kubernetes: kill a pod mid-run and prove the same invariants as the Compose drills.

The difference from `drills.py` is who recovers. There the drill restarts what it killed. Here the drill only
deletes a pod; a controller (Deployment or StatefulSet) notices and replaces it, and the drill waits and checks.

Each drill installs the chart into its own namespace, so topics start empty, and removes it afterwards. Traffic is
generated inside the cluster (`synth-capture`), so no data has to be copied in and nothing calls the public API:
the worker's REST fallback points at an unreachable address.
"""

import json
import subprocess
import time
from pathlib import Path

from blockpulse.drills import UNREACHABLE_API, _check, _report

NAMESPACE = "blockpulse-drill"
RELEASE = "bp"
GROUP = "blockpulse-enrich"
CHART = Path(__file__).resolve().parents[2] / "infra" / "k8s" / "chart" / "blockpulse"
BOOTSTRAP = f"{RELEASE}-kafka-0.{RELEASE}-kafka:9092"
WORKER_SELECTOR = "app.kubernetes.io/component=worker"


def _run(*command: str, timeout: float = 300, check: bool = True, stdin: str | None = None) -> subprocess.CompletedProcess:
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, input=stdin)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(command[:4])} ... failed ({result.returncode}): {result.stderr.strip()[-600:]}")
    return result


def _kubectl(*args: str, **kwargs) -> subprocess.CompletedProcess:
    return _run("kubectl", "-n", NAMESPACE, *args, **kwargs)


def install(chart: Path = CHART, image: str = "blockpulse:dev") -> float:
    """Install the chart into a fresh namespace; returns the seconds until everything is ready."""
    remove()
    started = time.monotonic()
    _run("helm", "upgrade", "--install", RELEASE, str(chart), "-n", NAMESPACE, "--create-namespace", "--wait",
         "--wait-for-jobs", "--timeout", "8m", "--set", f"app.image={image}", "--set", f"worker.apiBase={UNREACHABLE_API}",
         "--set", "worker.maxRequests=1", "--set", "api.replicas=1", timeout=540)
    return round(time.monotonic() - started, 1)


def remove() -> None:
    """Delete the drill namespace and everything in it, volumes included."""
    _run("kubectl", "delete", "namespace", NAMESPACE, "--ignore-not-found", "--wait=true", timeout=300)


def run_pod(name: str, shell: str, *, image: str = "blockpulse:dev", timeout: float = 600) -> str:
    """Run one shell command in a pod with the BlockPulse image, wait for it to finish, return its output."""
    _kubectl("run", name, f"--image={image}", "--image-pull-policy=IfNotPresent", "--restart=Never", "--command", "--", "sh", "-c", shell)
    deadline = time.monotonic() + timeout
    phase = ""
    while time.monotonic() < deadline:
        phase = _kubectl("get", "pod", name, "-o", "jsonpath={.status.phase}", check=False).stdout
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(1)
    logs = _kubectl("logs", name, check=False).stdout
    _kubectl("delete", "pod", name, "--wait=false", check=False)
    if phase != "Succeeded":
        raise RuntimeError(f"pod {name} ended as {phase or 'unfinished'}: {logs[-600:]}")
    return logs


def parse_group_lag(describe_output: str) -> dict:
    """Totals from `kafka-consumer-groups.sh --describe`. A partition with no commit yet shows `-` and counts as 0."""
    committed = end = partitions = 0
    for line in describe_output.splitlines():
        columns = line.split()
        if len(columns) < 6 or columns[0] == "GROUP" or not columns[2].isdigit():
            continue
        partitions += 1
        committed += int(columns[3]) if columns[3].isdigit() else 0
        end += int(columns[4]) if columns[4].isdigit() else 0
    return {"partitions": partitions, "committed": committed, "end": end, "lag": end - committed}


def group_lag(group: str = GROUP) -> dict | None:
    """The consumer group's committed and end offsets, or None while the broker can't answer."""
    result = _kubectl("exec", f"{RELEASE}-kafka-0", "--", "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server",
                      "localhost:9092", "--describe", "--group", group, check=False, timeout=60)
    return parse_group_lag(result.stdout) if result.returncode == 0 else None


def _worker_pods() -> list[dict]:
    items = json.loads(_kubectl("get", "pods", "-l", WORKER_SELECTOR, "-o", "json").stdout)["items"]
    return [{"name": p["metadata"]["name"], "phase": p["status"].get("phase"),
             "deleting": "deletionTimestamp" in p["metadata"],
             "restarts": sum(c.get("restartCount", 0) for c in p["status"].get("containerStatuses", []))} for p in items]


def _wait(condition, timeout: float, interval: float = 1.0):
    """Poll until `condition()` returns something truthy; returns it, or None on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(interval)
    return None


def _running_worker() -> dict | None:
    pods = [p for p in _worker_pods() if p["phase"] == "Running" and not p["deleting"]]
    return pods[0] if len(pods) == 1 else None


def _publish(messages: int, sessions: int) -> int:
    """Generate `sessions` synthetic captures in a pod and replay them into the raw topic. One session maps to
    one partition (the record key is the session), so several sessions spread the load over the partitions."""
    per_session = messages // sessions
    steps = [f"python -m blockpulse synth-capture --output /tmp/s{n} --messages {per_session} --seed {n} >/dev/null && "
             f"python -m blockpulse kafka-replay /tmp/s{n} --bootstrap-servers {BOOTSTRAP} >/dev/null" for n in range(1, sessions + 1)]
    run_pod("drill-publish", " && ".join(steps))
    return per_session * sessions


def _verify(checks: list, published: int) -> dict:
    """Run the Compose drills' Kafka invariants inside the cluster and merge their checks into this report."""
    # The command exits non-zero when a check fails; the report file is what matters, so print it either way.
    logs = run_pod("drill-verify", f"python -m blockpulse drill kafka-verify --published {published} "
                                   f"--bootstrap-servers {BOOTSTRAP} --output /tmp/drill >/dev/null 2>/tmp/err; "
                                   "cat /tmp/drill/*.json || cat /tmp/err")
    report = json.loads(logs[logs.index("{"):logs.rindex("}") + 1])
    checks.extend(report["checks"])
    return {key: report[key] for key in ("raw_records", "enriched_records", "duplicate_records")}


def _start_worker_and_wait_for_progress(checks: list) -> tuple[dict | None, dict | None]:
    """Scale the worker to one pod and wait until it has committed some, but not all, of the topic."""
    _kubectl("scale", "deployment", f"{RELEASE}-worker", "--replicas=1")
    worker = _wait(_running_worker, 120)
    lag = _wait(lambda: (found := group_lag()) and found["committed"] > 0 and found, 180, interval=0.5)
    _check(checks, "the kill lands mid-stream: some offsets committed, some messages still unprocessed",
           bool(worker and lag and lag["lag"] > 0), {"worker_pod": worker and worker["name"], "at_kill": lag})
    return worker, lag


def _drained() -> dict | None:
    lag = group_lag()
    return lag if lag and lag["partitions"] and lag["lag"] == 0 else None


def drill_enrich_kill(*, messages: int = 30000, sessions: int = 3, keep: bool = False) -> dict:
    """Force-delete the enrichment worker's pod mid-stream; the Deployment must replace it and the new pod must
    resume from the group's committed offsets."""
    started, checks = time.monotonic(), []
    install_seconds = install()
    published = _publish(messages, sessions)
    worker, at_kill = _start_worker_and_wait_for_progress(checks)
    killed_at = time.monotonic()
    if worker:
        # --grace-period=0 --force: the container gets SIGKILL, so no clean shutdown and no final checkpoint.
        _kubectl("delete", "pod", worker["name"], "--grace-period=0", "--force", check=False)
    replacement = _wait(lambda: (pod := _running_worker()) and worker and pod["name"] != worker["name"] and pod, 120)
    replaced_after = round(time.monotonic() - killed_at, 1)
    _check(checks, "the Deployment replaced the killed pod without anyone restarting it", bool(replacement),
           {"killed": worker and worker["name"], "replacement": replacement and replacement["name"], "seconds": replaced_after})
    drained = _wait(_drained, 600, interval=2)
    _check(checks, "the replacement resumed from the committed offsets and finished: lag 0", bool(drained),
           {"final": drained or group_lag(), "seconds_from_kill": round(time.monotonic() - killed_at, 1)})
    _kubectl("scale", "deployment", f"{RELEASE}-worker", "--replicas=0")
    _wait(lambda: not _worker_pods(), 90)
    facts = _verify(checks, published)
    if not keep:
        remove()
    return _report("k8s-enrich-kill", started, checks, published=published, sessions=sessions, data="synthetic",
                   install_seconds=install_seconds, committed_at_kill=at_kill and at_kill["committed"],
                   replacement_running_after_seconds=replaced_after, **facts)


def drill_broker_kill(*, messages: int = 30000, sessions: int = 3, keep: bool = False) -> dict:
    """Force-delete the Kafka pod mid-stream; the StatefulSet must bring it back on the same volume and the worker
    must ride it out without being restarted."""
    started, checks = time.monotonic(), []
    install_seconds = install()
    published = _publish(messages, sessions)
    worker, at_kill = _start_worker_and_wait_for_progress(checks)
    killed_at = time.monotonic()
    _kubectl("delete", "pod", f"{RELEASE}-kafka-0", "--grace-period=0", "--force", check=False)
    time.sleep(2)  # let the API server register the deletion before waiting on the replacement
    back = _kubectl("wait", "--for=condition=Ready", f"pod/{RELEASE}-kafka-0", "--timeout=180s", check=False)
    broker_back_after = round(time.monotonic() - killed_at, 1)
    _check(checks, "the StatefulSet brought the broker back on the same volume", back.returncode == 0,
           {"seconds": broker_back_after})
    drained = _wait(_drained, 600, interval=2)
    _check(checks, "consumption completes after the broker is back: lag 0", bool(drained),
           {"final": drained or group_lag(), "seconds_from_kill": round(time.monotonic() - killed_at, 1)})
    after = _running_worker()
    survived = bool(worker and after and after["name"] == worker["name"] and after["restarts"] == 0)
    _check(checks, "the worker survived the broker restart (same pod, no container restart)", survived,
           {"before": worker, "after": after})
    _kubectl("scale", "deployment", f"{RELEASE}-worker", "--replicas=0")
    _wait(lambda: not _worker_pods(), 90)
    facts = _verify(checks, published)
    if not keep:
        remove()
    return _report("k8s-broker-kill", started, checks, published=published, sessions=sessions, data="synthetic",
                   install_seconds=install_seconds, committed_at_kill=at_kill and at_kill["committed"],
                   broker_ready_after_seconds=broker_back_after, worker_survived=survived, **facts)


# ---------------------------------------------------------------- Postgres drill

POSTGRES_POD = f"{RELEASE}-postgres-0"
TRANSACTIONS_PER_MESSAGE = 4


def _psql(sql: str) -> str | None:
    """One value from Postgres, or None while it can't answer."""
    result = _kubectl("exec", POSTGRES_POD, "--", "psql", "-U", "blockpulse", "-d", "blockpulse", "-qAtc", sql, check=False, timeout=30)
    return result.stdout.strip() if result.returncode == 0 else None


def _scores(run_id: str) -> int | None:
    value = _psql(f"SELECT count(*) FROM scores WHERE run_id = '{run_id}'")
    return int(value) if value is not None and value.isdigit() else None


def _load_in_progress() -> bool:
    """True while another session has a transaction open in the database: the loader is inside its load."""
    return _psql("SELECT count(*) FROM pg_stat_activity WHERE datname = 'blockpulse' AND pid <> pg_backend_pid() "
                 "AND xact_start IS NOT NULL AND state <> 'idle'") not in (None, "", "0")


def load_job_manifest(name: str, run_id: str, messages: int, image: str = "blockpulse:dev") -> dict:
    """A Job that builds one screened session from synthetic traffic and loads it. Every attempt is a new pod
    (restartPolicy Never), and the data is regenerated from the same seed, so a retry loads identical rows."""
    shell = (f"python -m blockpulse synth-capture --output /tmp/s --messages {messages} --seed 11 "
             f"--transactions-per-message {TRANSACTIONS_PER_MESSAGE} >/dev/null && "
             "python -m blockpulse process /tmp/s/messages.jsonl --output /tmp/p >/dev/null && "
             "python -m blockpulse detect /tmp/p/features.jsonl --output /tmp/d >/dev/null && "
             f"python -m blockpulse load-results /tmp/d --run-id {run_id}")
    container = {"name": "load", "image": image, "imagePullPolicy": "IfNotPresent", "command": ["sh", "-c", shell],
                 "envFrom": [{"secretRef": {"name": f"{RELEASE}-app"}}],
                 "securityContext": {"runAsNonRoot": True, "allowPrivilegeEscalation": False}}
    return {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name, "namespace": NAMESPACE},
            "spec": {"backoffLimit": 6, "template": {"spec": {"restartPolicy": "Never", "containers": [container]}}}}


def _start_load(name: str, run_id: str, messages: int) -> None:
    _run("kubectl", "apply", "-f", "-", stdin=json.dumps(load_job_manifest(name, run_id, messages)))


def _job_pods(name: str) -> list[dict]:
    items = json.loads(_kubectl("get", "pods", "-l", f"job-name={name}", "-o", "json").stdout)["items"]
    return [{"name": p["metadata"]["name"], "phase": p["status"].get("phase")} for p in items]


def _job_done(name: str, timeout: float = 900) -> bool:
    return _kubectl("wait", "--for=condition=complete", f"job/{name}", f"--timeout={int(timeout)}s", check=False,
                    timeout=timeout + 30).returncode == 0


def _load_summary(name: str) -> dict:
    logs = _kubectl("logs", f"job/{name}", check=False).stdout
    try:
        return json.loads(logs[logs.index("{"):logs.rindex("}") + 1])
    except ValueError:
        return {}


def drill_postgres_kill(*, messages: int = 50000, keep: bool = False) -> dict:
    """Crash a Postgres load two ways: kill the loader's pod inside its transaction, then kill the Postgres pod
    under a second load. Each time the Job controller retries, and the rows must end up present exactly once."""
    started, checks = time.monotonic(), []
    install_seconds = install()
    rows = messages * TRANSACTIONS_PER_MESSAGE
    run_a, run_b = "drill-kill", "drill-restart"

    _start_load("load-a", run_a, messages)
    in_transaction = _wait(_load_in_progress, 600, interval=0.2)
    victims = [p for p in _job_pods("load-a") if p["phase"] == "Running"]
    for pod in victims:
        _kubectl("delete", "pod", pod["name"], "--grace-period=0", "--force", check=False)
    _check(checks, "loader pod was killed inside its load transaction", bool(in_transaction and victims),
           {"killed": [p["name"] for p in victims]})
    visible = _wait(lambda: (count := _scores(run_a)) is not None and {"rows": count}, 60)
    _check(checks, "atomic: nothing from the killed load is visible", bool(visible) and visible["rows"] == 0, visible)
    finished = _job_done("load-a")
    pods = _job_pods("load-a")
    _check(checks, "the Job retried in a new pod and loaded every row exactly once",
           finished and _scores(run_a) == rows and len(pods) >= 1 and not {p["name"] for p in pods} & {p["name"] for p in victims},
           {"rows": rows, "loaded": _scores(run_a), "inserted": _load_summary("load-a").get("inserted"), "pods_now": pods})
    _start_load("load-a-again", run_a, messages)
    again_done = _job_done("load-a-again")
    again = _load_summary("load-a-again")
    _check(checks, "a second load of the same run inserts nothing (idempotent)",
           again_done and again.get("inserted") == 0 and _scores(run_a) == rows,
           {"inserted": again.get("inserted"), "duplicates": again.get("duplicates")})

    _start_load("load-b", run_b, messages)
    in_transaction = _wait(_load_in_progress, 600, interval=0.2)
    killed_at = time.monotonic()
    _kubectl("delete", "pod", POSTGRES_POD, "--grace-period=0", "--force", check=False)
    time.sleep(2)
    back = _kubectl("wait", "--for=condition=Ready", f"pod/{POSTGRES_POD}", "--timeout=180s", check=False).returncode == 0
    postgres_back_after = round(time.monotonic() - killed_at, 1)
    visible = _wait(lambda: (count := _scores(run_b)) is not None and {"rows": count}, 60)
    failed = _wait(lambda: [p for p in _job_pods("load-b") if p["phase"] == "Failed"], 120)
    _check(checks, "Postgres pod was killed mid-load, came back on its volume, and that load attempt failed",
           bool(in_transaction and back and failed), {"postgres_ready_after_seconds": postgres_back_after,
                                                      "failed_attempts": [p["name"] for p in failed or []]})
    _check(checks, "atomic: nothing from the interrupted load is visible", bool(visible) and visible["rows"] == 0, visible)
    finished = _job_done("load-b")
    _check(checks, "the Job retried after Postgres returned and loaded every row exactly once",
           finished and _scores(run_b) == rows, {"rows": rows, "loaded": _scores(run_b), "attempts": len(_job_pods("load-b"))})
    _check(checks, "the earlier run survived the Postgres kill intact", _scores(run_a) == rows, {"rows": _scores(run_a)})
    if not keep:
        remove()
    return _report("k8s-postgres-kill", started, checks, rows=rows, data="synthetic", install_seconds=install_seconds,
                   postgres_ready_after_seconds=postgres_back_after)
