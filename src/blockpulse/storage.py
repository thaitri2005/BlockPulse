"""Small local stores: append JSONL; write summaries atomically."""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def new_run_path(parent: Path, prefix: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return parent / f"{prefix}-{timestamp}-{uuid.uuid4().hex[:8]}"


def encode_line(record: dict) -> bytes:
    return (json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


class StorageError(RuntimeError):
    pass


class JsonlWriter:
    def __init__(self, path: Path):
        self.file = path.open("xb")
        self.bytes_written = 0

    def append(self, record: dict, max_bytes: int | None = None) -> bool:
        data = encode_line(record)
        if max_bytes is not None and self.bytes_written + len(data) > max_bytes:
            return False
        try:
            self.file.write(data)
            self.file.flush()
        except OSError as error:
            raise StorageError(f"Cannot persist JSONL: {error}") from error
        self.bytes_written += len(data)
        return True

    def close(self):
        self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def write_summary(path: Path, value: dict):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(path)


CAPTURE_NAME = "messages.jsonl"


def capture_path(path: Path) -> Path:
    """The raw capture file for a session folder or a messages.jsonl path, compressed or not.

    Finished captures are gzipped to messages.jsonl.gz; every reader goes through here, so either form works.
    """
    path = Path(path)
    if path.is_dir():
        path = path / CAPTURE_NAME
    if path.is_file():
        return path
    packed = path.with_name(path.name + ".gz")
    if path.suffix != ".gz" and packed.is_file():
        return packed
    raise FileNotFoundError(f"No capture at {path} (or {packed.name})")


def open_capture(path: Path):
    """Open a raw capture for binary reading. gzip returns the original bytes, so checksums don't change."""
    import gzip
    path = capture_path(path)
    return gzip.open(path, "rb") if path.suffix == ".gz" else path.open("rb")


def compress_capture(folder: Path) -> int:
    """Gzip a finished session's messages.jsonl in place; returns bytes saved (0 if already compressed).

    The compressed file is written next to the original, read back in full and compared by SHA-256 before the
    original is removed, so an interruption at any point leaves a complete copy.
    """
    import gzip
    import hashlib
    import shutil
    plain = Path(folder) / CAPTURE_NAME
    packed = plain.with_name(plain.name + ".gz")
    if not plain.is_file():
        return 0
    temporary = packed.with_name(packed.name + ".tmp")
    with plain.open("rb") as source, gzip.open(temporary, "wb", compresslevel=6) as target:
        shutil.copyfileobj(source, target, 1 << 20)

    def digest(opener, path):
        h = hashlib.sha256()
        with opener(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()

    if digest(gzip.open, temporary) != digest(open, plain):
        temporary.unlink()
        raise StorageError(f"Compressed copy of {plain} did not read back identically; original kept")
    saved = plain.stat().st_size - temporary.stat().st_size
    temporary.replace(packed)
    plain.unlink()
    return saved
