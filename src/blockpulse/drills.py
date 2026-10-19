"""Failure drills: break a component on purpose mid-run, recover, and prove nothing was lost or duplicated.

Each drill uses recorded sessions, isolated infrastructure (a throwaway Kafka broker, a separate Postgres
database), and real processes killed without warning. A drill passes only if every check passes. The point is
the invariant, not uptime: after a crash and a restart, every input is represented exactly once where it matters.

    enrich-crash    hard-kill the enrichment worker mid-stream (no final checkpoint), restart it
    broker-restart  restart the Kafka broker while the worker is consuming
    postgres-crash  hard-kill a large load-results mid-transaction, and restart Postgres mid-load
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from blockpulse.storage import utc_now

UNREACHABLE_API = "http://127.0.0.1:9/api"  # the drills must never call the public REST API
BENCH_KAFKA_CONTAINER = "blockpulse-bench-kafka-1"
POSTGRES_CONTAINER = "blockpulse-postgres-1"


def _cli(*args: str, env: dict | None = None) -> subprocess.Popen:
    return subprocess.Popen([sys.executable, "-m", "blockpulse", *args], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, env={**os.environ, **(env or {})})


def _run_cli(*args: str, env: dict | None = None) -> dict:
    process = _cli(*args, env=env)
    out, err = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"blockpulse {args[0]} failed ({process.returncode}): {err[-800:]}")
    return json.loads(out.split("Saved to:")[0])


def _check(checks: list, name: str, passed: bool, detail) -> None:
    checks.append({"check": name, "passed": bool(passed), "detail": detail})


def _report(drill: str, started: float, checks: list, **facts) -> dict:
    return {"schema_version": 1, "drill": drill, "created_at": utc_now(),
            "passed": all(c["passed"] for c in checks), "seconds": round(time.monotonic() - started, 1),
            "checks": checks, **facts}


# ---------------------------------------------------------------- Kafka drills

def _kafka_invariants(checks: list, bootstrap: str, published: int, archive: Path) -> dict:
    """Shared checks: no message lost in enrichment, duplicates bounded, Parquet sink holds each message once."""
    from blockpulse.benchmark import read_topic
    from blockpulse.kafka_pipeline import ENRICHED_TOPIC, RAW_TOPIC
    raw_ids = [m for m, _ in read_topic(bootstrap, RAW_TOPIC)]
    enriched_ids = [m for m, _ in read_topic(bootstrap, ENRICHED_TOPIC)]
    missing = set(raw_ids) - set(enriched_ids)
    duplicates = len(enriched_ids) - len(set(enriched_ids))
    _check(checks, "every published message is in the raw topic", len(set(raw_ids)) == published,
           {"published": published, "raw_unique": len(set(raw_ids))})
    _check(checks, "no message lost: every raw message has an enriched record", not missing,
           {"missing": len(missing)})
    _check(checks, "duplicates are bounded (replayed since the last checkpoint only)", duplicates <= 1000,
           {"enriched_records": len(enriched_ids), "duplicate_records": duplicates})
    archived = _run_cli("kafka-parquet", "--output", str(archive), "--group-id", f"drill-parquet-{time.time_ns()}",
                        "--bootstrap-servers", bootstrap, "--idle-timeout", "10")
    files = list((archive).rglob("*.parquet"))
    _check(checks, "idempotent sink: the Parquet archive holds each message exactly once",
           len(files) == len(set(raw_ids)), {"parquet_files": len(files), "unique_messages": len(set(raw_ids)),
                                             "records_read": archived.get("messages_consumed")})
    return {"raw_records": len(raw_ids), "enriched_records": len(enriched_ids), "duplicate_records": duplicates}


def _publish(sessions: list[Path], bootstrap: str) -> int:
    from blockpulse.kafka_pipeline import create_m1_topics, publish_capture
    create_m1_topics(bootstrap)
    return sum(publish_capture(s / "messages.jsonl", bootstrap)["messages_published"] for s in sessions)


def _enricher(group: str, bootstrap: str) -> subprocess.Popen:
    return _cli("kafka-enrich", "--group-id", group, "--bootstrap-servers", bootstrap, "--api-base", UNREACHABLE_API,
                "--max-requests", "1", "--idle-timeout", "10")


def drill_enrich_crash(sessions: list[Path], bootstrap: str, work: Path, *, kill_after: float = 6.0) -> dict:
    started, checks = time.monotonic(), []
    published = _publish(sessions, bootstrap)
    group = f"drill-enrich-{time.time_ns()}"
    worker = _enricher(group, bootstrap)
    time.sleep(kill_after)
    still_running = worker.poll() is None
    worker.kill()  # hard kill: no clean shutdown, no final checkpoint
    worker.wait()
    _check(checks, "worker was killed mid-stream", still_running, {"kill_after_seconds": kill_after})
    resumed = _enricher(group, bootstrap)
    out, err = resumed.communicate()
    summary = json.loads(out) if resumed.returncode == 0 else {}
    _check(checks, "restarted worker resumed from its committed offsets and finished", resumed.returncode == 0
           and summary.get("consumer_lag_messages") == 0,
           {"messages_after_restart": summary.get("messages_consumed"), "final_lag": summary.get("consumer_lag_messages"),
            "stderr_tail": err[-300:] if resumed.returncode else ""})
    facts = _kafka_invariants(checks, bootstrap, published, work / "archive")
    return _report("enrich-crash", started, checks, sessions=[s.name for s in sessions], published=published,
                   messages_after_restart=summary.get("messages_consumed"), **facts)


def drill_broker_restart(sessions: list[Path], bootstrap: str, work: Path, *, restart_after: float = 5.0) -> dict:
    started, checks = time.monotonic(), []
    published = _publish(sessions, bootstrap)
    group = f"drill-broker-{time.time_ns()}"
    worker = _enricher(group, bootstrap)
    time.sleep(restart_after)
    restart = subprocess.run(["docker", "restart", BENCH_KAFKA_CONTAINER], capture_output=True, text=True)
    _check(checks, "broker restarted mid-stream", restart.returncode == 0 and worker.poll() is None,
           {"restart_after_seconds": restart_after})
    out, err = worker.communicate()
    first_ok = worker.returncode == 0
    # Whether the worker rode out the restart or stopped with an error, a rerun with the same group must finish.
    resumed = _enricher(group, bootstrap)
    out2, err2 = resumed.communicate()
    summary = json.loads(out2) if resumed.returncode == 0 else {}
    _check(checks, "consumption completes after the broker is back", resumed.returncode == 0
           and summary.get("consumer_lag_messages") == 0,
           {"worker_survived_restart": first_ok, "worker_error_tail": "" if first_ok else err[-300:],
            "final_lag": summary.get("consumer_lag_messages")})
    facts = _kafka_invariants(checks, bootstrap, published, work / "archive")
    return _report("broker-restart", started, checks, sessions=[s.name for s in sessions], published=published,
                   worker_survived_restart=first_ok, **facts)


# ---------------------------------------------------------------- Postgres drill

def _count(dsn: str, run_id: str) -> int:
    from blockpulse import db
    with db.connect(dsn) as connection:
        return connection.execute("SELECT count(*) FROM scores WHERE run_id = %s", (run_id,)).fetchone()[0]


def drill_postgres_crash(screened: Path, admin_dsn: str, *, kill_after: float = 8.0) -> dict:
    """Load one screened session into a separate drill database, crashing it two different ways."""
    from blockpulse import db
    started, checks = time.monotonic(), []
    with db.connect(admin_dsn) as admin:
        admin.autocommit = True
        admin.execute("DROP DATABASE IF EXISTS blockpulse_drill")
        admin.execute("CREATE DATABASE blockpulse_drill")
    dsn = admin_dsn.rsplit("/", 1)[0] + "/blockpulse_drill"
    env = {"BLOCKPULSE_DSN": dsn}
    _run_cli("db-init", env=env)
    rows = sum(1 for _ in (screened / "screened.jsonl").open(encoding="utf-8"))
    run_a, run_b = "drill-kill", "drill-restart"

    loader = _cli("load-results", str(screened), "--run-id", run_a, env=env)
    time.sleep(kill_after)
    killed_mid_load = loader.poll() is None
    loader.kill()  # hard kill inside the load transaction
    loader.wait()
    _check(checks, "loader was killed mid-transaction", killed_mid_load, {"kill_after_seconds": kill_after})
    _check(checks, "atomic: nothing from the killed load is visible", _count(dsn, run_a) == 0,
           {"rows_visible": _count(dsn, run_a)})
    first = _run_cli("load-results", str(screened), "--run-id", run_a, env=env)
    _check(checks, "rerun loads every row exactly once", first["inserted"] == rows and _count(dsn, run_a) == rows,
           {"rows": rows, "inserted": first["inserted"]})
    again = _run_cli("load-results", str(screened), "--run-id", run_a, env=env)
    _check(checks, "a second rerun inserts nothing (idempotent)", again["inserted"] == 0 and _count(dsn, run_a) == rows,
           {"inserted": again["inserted"], "duplicates": again["duplicates"]})

    loader = _cli("load-results", str(screened), "--run-id", run_b, env=env)
    time.sleep(kill_after)
    restart = subprocess.run(["docker", "restart", POSTGRES_CONTAINER], capture_output=True, text=True)
    loader.communicate()
    _check(checks, "Postgres restarted mid-load and the load failed", restart.returncode == 0 and loader.returncode != 0,
           {"loader_exit_code": loader.returncode})
    for _ in range(30):  # wait for Postgres to accept connections again
        try:
            visible = _count(dsn, run_b)
            break
        except Exception:
            time.sleep(1)
    _check(checks, "atomic: nothing from the interrupted load is visible", visible == 0, {"rows_visible": visible})
    second = _run_cli("load-results", str(screened), "--run-id", run_b, env=env)
    _check(checks, "rerun after the restart loads every row exactly once",
           second["inserted"] == rows and _count(dsn, run_b) == rows, {"rows": rows, "inserted": second["inserted"]})
    return _report("postgres-crash", started, checks, session=screened.name, rows=rows)


def write_report(report: dict, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{report['drill']}-{report['created_at'][:19].replace(':', '').replace('-', '')}.json"
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    return path
