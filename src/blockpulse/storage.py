"""Small local stores: append JSONL; write summaries atomically."""

from datetime import datetime, timezone
import json
from pathlib import Path
import uuid


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
