import json
import os
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest

from blockpulse import collector
from blockpulse.cli import main
from tests.test_capture import ScriptedConnection

T0 = datetime(2026, 10, 15, 4, 0, tzinfo=timezone.utc)


def ticking_clock(start=T0, step=timedelta(minutes=1)):
    state = {"now": start}

    def clock():
        state["now"] += step
        return state["now"]
    return clock


def feed(transaction, count=2):
    """A connector whose every connection delivers `count` messages with distinct transactions, then goes quiet."""
    def connector(*args, **kwargs):
        messages = []
        for n in range(count):
            tx = {**transaction, "txid": f"{os.urandom(32).hex()}"}
            messages.append(json.dumps({"mempool-transactions": {"sequence": n, "added": [tx]}}))
        return ScriptedConnection(messages)
    return connector


def test_session_names_must_be_findable():
    assert collector.session_name("session-aws", T0) == "session-aws-20261015T040000Z"
    with pytest.raises(ValueError, match="must start with"):
        collector.session_name("capture", T0)


def test_loop_captures_back_to_back_until_the_session_limit(tmp_path, transaction):
    layout = collector.Layout(tmp_path)
    result = collector.collect_loop(layout, session_seconds=0.3, url="wss://example.invalid/ws", max_sessions=2,
                                    min_free_gib=0, connector=feed(transaction), clock=ticking_clock())
    assert result["stop_reason"] == "max_sessions"
    assert [s["messages_saved"] for s in result["sessions"]] == [2, 2]
    assert collector.finished_captures(layout) == [s["session"] for s in result["sessions"]]


def test_loop_honors_deadline_low_disk_and_cools_down_when_empty(tmp_path, transaction, monkeypatch):
    layout = collector.Layout(tmp_path)
    assert collector.collect_loop(layout, session_seconds=10, url="wss://x.invalid/ws", until=T0 - timedelta(seconds=1),
                                  clock=lambda: T0, min_free_gib=0)["stop_reason"] == "deadline"

    nearly = datetime.now(timezone.utc) + timedelta(seconds=5)  # under a minute left: no sliver session
    assert collector.collect_loop(layout, session_seconds=600, url="wss://x.invalid/ws", until=nearly,
                                  min_free_gib=0, connector=feed(transaction)) == {"sessions": [], "stop_reason": "deadline"}

    monkeypatch.setattr(collector, "MIN_SESSION_SECONDS", 0.5)
    soon = datetime.now(timezone.utc) + timedelta(seconds=2)  # a 600 s session is clipped to the deadline
    started = time.monotonic()
    clipped = collector.collect_loop(layout, session_seconds=600, url="wss://x.invalid/ws", until=soon, min_free_gib=0,
                                     connector=feed(transaction), sleep=lambda seconds: None)
    assert clipped["stop_reason"] == "deadline" and len(clipped["sessions"]) == 1
    assert time.monotonic() - started < 30  # far below the 600 s the session would otherwise run

    naps = []
    empty = collector.collect_loop(layout, session_seconds=0.1, url="wss://x.invalid/ws", max_sessions=1, min_free_gib=0,
                                   connector=lambda *a, **k: ScriptedConnection([]), clock=ticking_clock(), sleep=naps.append)
    assert empty["sessions"][0]["messages_saved"] == 0 and naps == [collector.EMPTY_SESSION_COOLDOWN]

    monkeypatch.setattr(collector, "free_gib", lambda path: 1.0)
    assert collector.collect_loop(layout, session_seconds=10, url="wss://x.invalid/ws", min_free_gib=15)["stop_reason"] == "low_disk"


def captured_layout(tmp_path, transaction, sessions=2):
    layout = collector.Layout(tmp_path)
    collector.collect_loop(layout, session_seconds=0.3, url="wss://x.invalid/ws", max_sessions=sessions, min_free_gib=0,
                           connector=feed(transaction), clock=ticking_clock())
    return layout


def test_process_pending_is_idempotent_compacts_logs_and_ignores_other_folders(tmp_path, transaction):
    layout = captured_layout(tmp_path, transaction)
    other = layout.raw / "phase0-old-run"  # earlier hand-made runs must not be reprocessed under a new name
    other.mkdir()
    (other / "summary.json").write_text("{}", encoding="utf-8")

    first = collector.process_pending(layout)
    assert first["failed"] == [] and len(first["sessions"]) == 2
    for name in collector.finished_captures(layout):
        assert (layout.screened / name / "summary.json").is_file()
        assert (layout.processed / name / "events.jsonl.gz").is_file()
        assert not (layout.processed / name / "events.jsonl").exists()
    assert not (layout.processed / "phase0-old-run").exists()
    rows = layout.log.read_text(encoding="utf-8").splitlines()
    assert len(rows) == 3 and rows[0].startswith('"session"')

    assert collector.process_pending(layout) == {"sessions": [], "failed": [], "backfill_status_changes": None}
    assert len(layout.log.read_text(encoding="utf-8").splitlines()) == 3


def test_interrupted_stage_is_set_aside_and_redone(tmp_path, transaction):
    layout = captured_layout(tmp_path, transaction, sessions=1)
    name = collector.finished_captures(layout)[0]
    half_done = layout.processed / name
    half_done.mkdir(parents=True)
    (half_done / "events.jsonl").write_text("partial", encoding="utf-8")
    done = collector.process_session(layout, name)
    assert done["processed"] and done["screened"]
    kept = [p for p in layout.processed.iterdir() if p.name.startswith(f"{name}.partial-")]
    assert len(kept) == 1 and (kept[0] / "events.jsonl").read_text() == "partial"
    assert collector.finished_captures(layout) == [name]


def test_lock_blocks_a_second_processor_but_not_a_stale_one(tmp_path, transaction):
    layout = captured_layout(tmp_path, transaction, sessions=1)
    lock = tmp_path / collector.LOCK_NAME
    lock.write_text("12345 earlier\n", encoding="utf-8")
    with pytest.raises(collector.CollectorError, match="another processor"):
        collector.process_pending(layout)
    old = time.time() - collector.STALE_LOCK_SECONDS - 60
    os.utime(lock, (old, old))
    assert len(collector.process_pending(layout)["sessions"]) == 1
    assert not lock.exists()


FAKE_AWS = """
import json, os, sys
with open(os.environ["FAKE_AWS_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
fail_on = os.environ.get("FAKE_AWS_FAIL_ON")
sys.exit(1 if fail_on and any(fail_on in arg for arg in sys.argv[1:]) else 0)
"""


def test_sync_uploads_each_session_once_and_reports_failures(tmp_path, transaction, monkeypatch):
    layout = captured_layout(tmp_path / "data", transaction)
    collector.process_pending(layout)
    fake = tmp_path / "fake_aws.py"
    fake.write_text(FAKE_AWS, encoding="utf-8")
    calls = tmp_path / "calls.jsonl"
    monkeypatch.setenv("FAKE_AWS_LOG", str(calls))
    command = [sys.executable, str(fake)]

    monkeypatch.setenv("FAKE_AWS_FAIL_ON", "/raw/")
    failing = collector.sync_pending(layout, bucket="b", aws_command=command)
    assert failing["uploaded_now"] == [] and len(failing["failed"]) == 2

    monkeypatch.delenv("FAKE_AWS_FAIL_ON")
    calls.unlink()
    status = collector.sync_pending(layout, bucket="b", aws_command=command)
    names = collector.finished_captures(layout)
    assert status["uploaded_now"] == names and status["failed"] == []
    logged = [json.loads(line) for line in calls.read_text().splitlines()]
    destinations = [args[-1] for args in logged]
    for name in names:
        for part in ("raw", "processed", "screened"):
            assert f"s3://b/data/{part}/{name}/" in destinations
        assert (layout.screened / name / collector.UPLOADED_MARKER).exists()
    assert "s3://b/status/status.json" in destinations and "s3://b/status/capture_log.csv" in destinations
    assert all(".uploaded" in args for args in logged if "--recursive" in args)  # markers stay local

    calls.unlink()
    again = collector.sync_pending(layout, bucket="b", aws_command=command)
    assert again["uploaded_now"] == []
    assert all("--recursive" not in json.loads(line) for line in calls.read_text().splitlines())


def test_cli_collect_loop_requires_a_time_zone_on_until(tmp_path, capsys):
    assert main(["collect-loop", "--root", str(tmp_path), "--until", "2026-10-19T04:00:00"]) == 1
    assert "time zone" in capsys.readouterr().err


PG_DSN = os.environ.get("BLOCKPULSE_TEST_DSN")


@pytest.mark.skipif(not PG_DSN, reason="Set BLOCKPULSE_TEST_DSN to a throwaway Postgres database")
def test_loading_after_compaction_marks_sessions_loaded_once(tmp_path, transaction):
    from blockpulse import db
    connection = db.connect(PG_DSN)
    connection.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    connection.commit()
    db.apply_migrations(connection)
    connection.commit()

    layout = captured_layout(tmp_path, transaction)
    collector.process_pending(layout)  # processed and compacted, as on the AWS instance
    assert collector.pending_sessions(layout, load_db=True) == collector.finished_captures(layout)
    loaded = collector.process_pending(layout, load_db=True, dsn=PG_DSN)  # e.g. after pulling the data locally
    assert loaded["failed"] == [] and all(s["loaded"] for s in loaded["sessions"])
    assert connection.execute("SELECT count(*) FROM scores").fetchone()[0] == 4
    assert connection.execute("SELECT count(*) FROM lifecycle_events").fetchone()[0] == 4
    assert collector.pending_sessions(layout, load_db=True) == []
    connection.close()


def test_an_interrupted_session_ends_the_loop(tmp_path, monkeypatch):
    """systemd stops the service with SIGINT; the loop must not start another session afterwards."""
    started = []

    async def interrupted_capture(config, output, connector=None):
        started.append(output.name)
        output.mkdir(parents=True)
        (output / "summary.json").write_text("{}", encoding="utf-8")
        return {"messages_saved": 3, "stop_reason": "interrupted"}

    monkeypatch.setattr(collector, "capture", interrupted_capture)
    result = collector.collect_loop(collector.Layout(tmp_path), session_seconds=60, url="wss://x.invalid/ws",
                                    min_free_gib=0, clock=ticking_clock())  # no session limit, no deadline
    assert result["stop_reason"] == "interrupted" and len(started) == 1


def test_process_pending_by_date_in_stages_matches_one_pass(tmp_path, transaction):
    """The Airflow DAG runs one date in stages (screen, then lake, then load); the result must equal one pass."""
    pq = pytest.importorskip("pyarrow.parquet")
    from datetime import date
    staged = captured_layout(tmp_path / "staged", transaction)
    first, second = collector.finished_captures(staged)
    (staged.raw / second).rename(staged.raw / "session-20261016T000000Z")  # a capture from the next day
    shutil.copytree(staged.root, tmp_path / "one-pass")
    one_pass = collector.Layout(tmp_path / "one-pass")
    on = date(2026, 10, 15)

    screened = collector.process_pending(staged, lake=False, on=on, compact=False)
    assert [s["session"] for s in screened["sessions"]] == [first] and screened["failed"] == []
    assert (staged.processed / first / "events.jsonl").is_file()  # left uncompressed for the next stage
    assert not (staged.root / "lake").exists()
    lake = collector.process_pending(staged, on=on, compact=False)
    assert [(s["session"], s["processed"], s["lake"]) for s in lake["sessions"]] == [(first, False, True)]
    assert collector.pending_sessions(staged, on=on) == [] and collector.pending_sessions(staged) == ["session-20261016T000000Z"]

    assert collector.process_pending(one_pass, on=on)["failed"] == []
    for table in ("transactions", "lifecycle_events", "capture_sessions"):
        rows = [sorted((str(r) for p in sorted((layout.root / "lake" / table).rglob("*.parquet"))
                        for r in pq.read_table(p).to_pylist())) for layout in (staged, one_pass)]
        assert rows[0] == rows[1] and rows[0], table


def test_process_pending_refuses_a_date_that_is_still_recording(tmp_path, transaction):
    from datetime import date
    layout = captured_layout(tmp_path, transaction, sessions=1)
    (layout.raw / "session-20261015T230000Z").mkdir()  # started that day, no summary yet
    with pytest.raises(collector.CollectorError, match="still recording"):
        collector.process_pending(layout, on=date(2026, 10, 15))
    assert collector.process_pending(layout, on=date(2026, 10, 14)) == {"sessions": [], "failed": [], "backfill_status_changes": None}
    assert main(["process-pending", "--root", str(layout.root), "--date", "2026-10-15"]) != 0


def test_lock_left_by_a_dead_process_on_this_host_is_taken_over(tmp_path, transaction):
    """Airflow killed a task whose heartbeat stopped while the laptop slept; its lock must not block the retries."""
    import socket
    import subprocess
    layout = captured_layout(tmp_path, transaction, sessions=1)
    lock = tmp_path / collector.LOCK_NAME
    finished = subprocess.run([sys.executable, "-c", "import os; print(os.getpid())"], capture_output=True, text=True)
    dead_pid = int(finished.stdout)
    lock.write_text(f"{dead_pid} other-host 2026-10-20T12:03:19Z\n", encoding="utf-8")
    with pytest.raises(collector.CollectorError, match="another processor"):  # another host: only the age rule
        collector.process_pending(layout)
    lock.write_text(f"{os.getpid()} {socket.gethostname()} 2026-10-20T12:03:19Z\n", encoding="utf-8")
    with pytest.raises(collector.CollectorError, match="another processor"):  # alive: respected
        collector.process_pending(layout)
    lock.write_text(f"{dead_pid} {socket.gethostname()} 2026-10-20T12:03:19Z\n", encoding="utf-8")
    if os.name != "posix":  # no safe liveness probe on Windows: the lock waits for the age rule
        with pytest.raises(collector.CollectorError, match="another processor"):
            collector.process_pending(layout)
        return
    assert len(collector.process_pending(layout)["sessions"]) == 1 and not lock.exists()


@pytest.mark.skipif(os.name != "posix", reason="SIGTERM delivery to a child process is POSIX behaviour")
def test_sigterm_releases_the_lock(tmp_path):
    import signal
    import subprocess
    script = (
        "import sys, time\n"
        "from pathlib import Path\n"
        "from blockpulse import cli, collector\n"
        "def hold(argv):\n"
        "    with collector._Lock(Path(argv[0])):\n"
        "        print('locked', flush=True)\n"
        "        time.sleep(60)\n"
        "cli._main = hold\n"
        "raise SystemExit(cli.main([sys.argv[1]]))\n"
    )
    child = subprocess.Popen([sys.executable, "-c", script, str(tmp_path)], stdout=subprocess.PIPE, text=True)
    assert child.stdout.readline().strip() == "locked" and (tmp_path / collector.LOCK_NAME).exists()
    child.send_signal(signal.SIGTERM)
    assert child.wait(timeout=20) == 128 + signal.SIGTERM
    assert not (tmp_path / collector.LOCK_NAME).exists()
