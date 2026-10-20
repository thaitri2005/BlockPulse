import json

import pytest

from blockpulse.process import process_capture
from blockpulse.storage import StorageError, capture_path, compress_capture, open_capture


@pytest.fixture
def session(tmp_path, transaction, record_factory):
    folder = tmp_path / "raw" / "session-x"
    folder.mkdir(parents=True)
    rows = [record_factory({"mempool-transactions": {"added": [{**transaction, "txid": f"{n:064x}"}]}}, number=n)
            for n in range(1, 6)]
    (folder / "messages.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    return folder


def test_compressed_capture_processes_byte_identically(session, tmp_path):
    plain_out = tmp_path / "plain"
    process_capture(session, plain_out)
    original = (session / "messages.jsonl").read_bytes()
    saved = compress_capture(session)
    assert saved > 0 and not (session / "messages.jsonl").exists()
    assert capture_path(session).name == "messages.jsonl.gz"
    with open_capture(session) as handle:
        assert handle.read() == original
    packed_out = tmp_path / "packed"
    process_capture(session / "messages.jsonl", packed_out)  # the old path still resolves
    for name in ("events.jsonl", "features.jsonl", "errors.jsonl", "features.csv"):
        assert (plain_out / name).read_bytes() == (packed_out / name).read_bytes()
    plain_summary = json.loads((plain_out / "summary.json").read_text(encoding="utf-8"))
    packed_summary = json.loads((packed_out / "summary.json").read_text(encoding="utf-8"))
    assert plain_summary["input_sha256"] == packed_summary["input_sha256"]  # same bytes, same checksum
    assert compress_capture(session) == 0  # already compressed: no-op


def test_failed_verification_keeps_the_original(session, monkeypatch):
    import gzip
    real_open = gzip.open

    def corrupting_open(path, mode="rb", **kwargs):
        handle = real_open(path, mode, **kwargs)
        if "r" in mode:
            handle.read = lambda *a: b"corrupted" if not getattr(handle, "_done", False) and not setattr(handle, "_done", True) else b""
        return handle
    monkeypatch.setattr(gzip, "open", corrupting_open)
    with pytest.raises(StorageError, match="original kept"):
        compress_capture(session)
    assert (session / "messages.jsonl").is_file() and not (session / "messages.jsonl.gz").exists()


def test_missing_capture_is_reported(tmp_path):
    with pytest.raises(FileNotFoundError):
        capture_path(tmp_path)
