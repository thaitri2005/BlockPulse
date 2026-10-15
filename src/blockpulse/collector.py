"""Unattended collection, shared by every runner (laptop task, Airflow, the AWS instance).

Two independent jobs, so a slow step never stops the capture:

* ``collect_loop`` records bounded captures back to back into ``raw/<session>``.
* ``process_pending`` turns every *finished* capture (one with a summary.json) into processed and screened
  output, optionally loads it into Postgres, compacts the bulky events file, and logs one line per session.

Both are safe to repeat. Finished stages are detected from their summary files and skipped, interrupted stages
are moved aside (never deleted) and redone, and ``sync_pending`` uploads each session exactly once.
"""

import asyncio
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import time

from websockets.asyncio.client import connect

from blockpulse.capture import CaptureConfig, capture
from blockpulse.detect import detect_features
from blockpulse.process import process_capture

SESSION_PREFIX = "session-"
LOG_FIELDS = ("session", "seconds", "messages", "features", "flagged", "connection_errors", "stop_reason")
LOADED_MARKER = ".loaded"
UPLOADED_MARKER = ".uploaded"
LOCK_NAME = ".collector.lock"
STALE_LOCK_SECONDS = 3 * 3600
EMPTY_SESSION_COOLDOWN = 60.0
MIN_SESSION_SECONDS = 60.0  # near the deadline, stop rather than record a pointless sliver of a session

# Long captures must survive the feed's routine disconnects; the reconnect limit counts for the whole capture.
LONG_CAPTURE_LIMITS = {"max_reconnects": 300, "max_messages": 200_000, "max_bytes": 400 * 2**20}


class CollectorError(RuntimeError):
    pass


@dataclass(frozen=True)
class Layout:
    """Where everything lives under one data root (``data/`` locally, ``/srv/blockpulse/data`` on AWS)."""

    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def processed(self) -> Path:
        return self.root / "processed"

    @property
    def screened(self) -> Path:
        return self.root / "screened"

    @property
    def log(self) -> Path:
        return self.root / "capture_log.csv"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def session_name(prefix: str, moment: datetime) -> str:
    name = f"{prefix}-{moment.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}"
    if not name.startswith(SESSION_PREFIX):
        raise ValueError(f"session names must start with {SESSION_PREFIX!r} so processing can find them; got {name!r}")
    return name


def free_gib(path: Path) -> float:
    return shutil.disk_usage(path).free / 2**30


# ---------------------------------------------------------------- capture

def collect_loop(layout: Layout, *, session_seconds: float, url: str, prefix: str = "session", until: datetime | None = None,
                 max_sessions: int | None = None, min_free_gib: float = 15.0, connector=connect, clock=utc_now,
                 sleep=time.sleep) -> dict:
    """Capture sessions back to back until the deadline, the session limit, or low disk stops it."""
    if session_seconds <= 0:
        raise ValueError("session_seconds must be positive")
    layout.raw.mkdir(parents=True, exist_ok=True)
    sessions, stop_reason = [], "max_sessions"
    while max_sessions is None or len(sessions) < max_sessions:
        now = clock()
        if until is not None and now >= until:
            stop_reason = "deadline"
            break
        if free_gib(layout.root) < min_free_gib:
            stop_reason = "low_disk"
            break
        if until is not None and (until - now).total_seconds() < min(MIN_SESSION_SECONDS, session_seconds):
            stop_reason = "deadline"
            break
        duration = session_seconds if until is None else min(session_seconds, (until - now).total_seconds())
        name = session_name(prefix, now)
        while (layout.raw / name).exists():  # two sessions within one second: wait for a fresh name
            sleep(1)
            name = session_name(prefix, clock())
        config = CaptureConfig(url=url, duration=duration, **LONG_CAPTURE_LIMITS)
        summary = asyncio.run(capture(config, layout.raw / name, connector=connector))
        sessions.append({"session": name, "messages_saved": summary["messages_saved"], "stop_reason": summary["stop_reason"]})
        if summary["stop_reason"] == "interrupted":  # Ctrl+C or systemd stop: the capture saved its summary; end here
            stop_reason = "interrupted"
            break
        if summary["messages_saved"] == 0:  # source unreachable: don't spin
            sleep(EMPTY_SESSION_COOLDOWN)
    return {"sessions": sessions, "stop_reason": stop_reason}


# ---------------------------------------------------------------- processing

def _complete(directory: Path) -> bool:
    return (directory / "summary.json").is_file()


def _set_aside(directory: Path) -> None:
    """An interrupted stage left a folder without a summary. Keep it for inspection and redo the stage."""
    if directory.exists() and not _complete(directory):
        directory.rename(directory.with_name(f"{directory.name}.partial-{utc_now():%Y%m%dT%H%M%SZ}"))


def finished_captures(layout: Layout) -> list[str]:
    if not layout.raw.is_dir():
        return []
    return sorted(p.name for p in layout.raw.iterdir()
                  if p.is_dir() and p.name.startswith(SESSION_PREFIX) and ".partial-" not in p.name and _complete(p))


def pending_sessions(layout: Layout, *, load_db: bool = False) -> list[str]:
    """Finished captures not yet screened, plus (when loading) screened sessions not yet in Postgres."""
    return [name for name in finished_captures(layout)
            if not _complete(layout.screened / name) or (load_db and not (layout.screened / name / LOADED_MARKER).exists())]


def _compact_events(processed: Path) -> bool:
    events = processed / "events.jsonl"
    if not events.is_file():
        return False
    packed = processed / "events.jsonl.gz"
    temporary = processed / "events.jsonl.gz.tmp"
    with events.open("rb") as source, gzip.open(temporary, "wb", compresslevel=6) as target:
        shutil.copyfileobj(source, target)
    with gzip.open(temporary, "rb") as check:  # make sure it reads back before the original goes
        while check.read(1 << 20):
            pass
    temporary.replace(packed)
    events.unlink()
    return True


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def append_log(layout: Layout, name: str) -> dict:
    capture_summary = _read_json(layout.raw / name / "summary.json")
    processed = _read_json(layout.processed / name / "summary.json")
    screened = _read_json(layout.screened / name / "summary.json")
    row = {"session": name, "seconds": capture_summary.get("elapsed_seconds", ""), "messages": processed.get("lines_read", ""),
           "features": processed.get("features_written", ""), "flagged": screened.get("flagged_rows", ""),
           "connection_errors": capture_summary.get("connection_errors", ""), "stop_reason": capture_summary.get("stop_reason", "")}
    new_file = not layout.log.exists()
    with layout.log.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LOG_FIELDS, quoting=csv.QUOTE_ALL)
        if new_file:
            writer.writeheader()
        writer.writerow(row)
    return row


def process_session(layout: Layout, name: str, *, load_db: bool = False, dsn: str | None = None, compact: bool = True) -> dict:
    """Bring one finished capture up to date. Each stage runs only if its output is missing."""
    raw = layout.raw / name
    if not _complete(raw):
        raise CollectorError(f"{raw} is not a finished capture")
    processed, screened = layout.processed / name, layout.screened / name
    done = {"session": name, "processed": False, "screened": False, "loaded": False, "compacted": False}
    if not _complete(processed):
        _set_aside(processed)
        processed.parent.mkdir(parents=True, exist_ok=True)
        process_capture(raw / "messages.jsonl", processed)
        done["processed"] = True
    if not _complete(screened):
        _set_aside(screened)
        screened.parent.mkdir(parents=True, exist_ok=True)
        detect_features(processed / "features.jsonl", screened)
        done["screened"] = True
        append_log(layout, name)
    if load_db and not (screened / LOADED_MARKER).exists():
        from blockpulse import db
        from blockpulse.lifecycle import load_events
        from blockpulse.results import load_screened
        with db.connect(dsn) as connection:
            results = load_screened(connection, screened, name)
            events = load_events(connection, processed)
        if results["conflicts"] or results["invalid"] or events["invalid"]:
            raise CollectorError(f"{name}: load reported conflicts or invalid rows; not marking it loaded")
        (screened / LOADED_MARKER).write_text(utc_now().isoformat() + "\n", encoding="utf-8")
        done["loaded"] = True
    if compact:
        done["compacted"] = _compact_events(processed)
    return done


class _Lock:
    """One processor at a time per data root. A lock older than three hours is treated as left behind by a crash."""

    def __init__(self, root: Path):
        self.path = root / LOCK_NAME

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(2):
            try:
                handle = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(handle, f"{os.getpid()} {utc_now().isoformat()}\n".encode())
                os.close(handle)
                return self
            except FileExistsError:
                if time.time() - self.path.stat().st_mtime < STALE_LOCK_SECONDS:
                    raise CollectorError(f"another processor holds {self.path}")
                self.path.unlink(missing_ok=True)
        raise CollectorError(f"could not take {self.path}")

    def __exit__(self, *args):
        self.path.unlink(missing_ok=True)


def process_pending(layout: Layout, *, load_db: bool = False, dsn: str | None = None) -> dict:
    """Process every pending session. One failure is recorded and does not stop the others."""
    results, failed = [], []
    with _Lock(layout.root):
        for name in pending_sessions(layout, load_db=load_db):
            try:
                results.append(process_session(layout, name, load_db=load_db, dsn=dsn))
            except Exception as error:  # keep going; the failure is reported and retried next time
                failed.append({"session": name, "error": f"{type(error).__name__}: {error}"})
    return {"sessions": results, "failed": failed}


def collect(layout: Layout, *, duration: float, url: str, prefix: str = "session", load_db: bool = False,
            dsn: str | None = None, min_free_gib: float = 15.0, connector=connect) -> dict:
    """One capture followed by processing of everything pending (the laptop's hourly task)."""
    captured = collect_loop(layout, session_seconds=duration, url=url, prefix=prefix, max_sessions=1,
                            min_free_gib=min_free_gib, connector=connector)
    if captured["stop_reason"] == "interrupted":  # the operator asked to stop; leave processing for next time
        return {"capture": captured, "processing": {"sessions": [], "failed": []}}
    return {"capture": captured, "processing": process_pending(layout, load_db=load_db, dsn=dsn)}


# ---------------------------------------------------------------- upload

def sync_pending(layout: Layout, *, bucket: str, prefix: str = "data", aws_command: list[str] | None = None) -> dict:
    """Upload each screened session (raw, processed, screened folders) once, then the log and a status file.

    Uses the AWS CLI, which Amazon Linux ships with and which picks up the instance role's credentials.
    `aws_command` (or BLOCKPULSE_AWS_CLI) replaces the `aws` executable, which tests use to substitute a fake.
    """
    aws = aws_command or shlex.split(os.environ.get("BLOCKPULSE_AWS_CLI", "aws"))
    destination = f"s3://{bucket}/{prefix.strip('/')}"

    def run(*args: str) -> None:
        result = subprocess.run([*aws, *args], capture_output=True, text=True)
        if result.returncode != 0:
            raise CollectorError(f"aws {' '.join(args[:3])} failed: {(result.stderr or result.stdout)[-500:]}")

    uploaded, failed = [], []
    for name in finished_captures(layout):
        screened = layout.screened / name
        if not _complete(screened) or (screened / UPLOADED_MARKER).exists():
            continue
        try:
            for part in ("raw", "processed", "screened"):
                run("s3", "cp", "--recursive", "--only-show-errors", "--exclude", UPLOADED_MARKER, "--exclude", LOADED_MARKER,
                    str(layout.root / part / name), f"{destination}/{part}/{name}/")
            (screened / UPLOADED_MARKER).write_text(utc_now().isoformat() + "\n", encoding="utf-8")
            uploaded.append(name)
        except CollectorError as error:
            failed.append({"session": name, "error": str(error)})
    status = {"updated_at": utc_now().isoformat(), "uploaded_now": uploaded, "failed": failed,
              "finished_captures": len(finished_captures(layout)), "free_gib": round(free_gib(layout.root), 1)}
    status_path = layout.root / "status.json"
    status_path.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
    run("s3", "cp", "--only-show-errors", str(status_path), f"s3://{bucket}/status/status.json")
    if layout.log.exists():
        run("s3", "cp", "--only-show-errors", str(layout.log), f"s3://{bucket}/status/capture_log.csv")
    return status
