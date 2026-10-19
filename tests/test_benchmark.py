import json

import pytest

pytest.importorskip("pyarrow")

from blockpulse import benchmark, collector  # noqa: E402
from tests.test_collector import captured_layout  # noqa: E402


def test_batch_benchmark_times_every_stage_and_counts_rows(tmp_path, transaction):
    layout = captured_layout(tmp_path / "src", transaction, sessions=2)
    sessions = [layout.raw / name for name in collector.finished_captures(layout)]
    report = benchmark.benchmark_batch(sessions, tmp_path / "work")
    assert report["input"]["sessions"] == 2 and report["input"]["transactions"] > 0
    assert set(report["seconds"]) == {"process", "screen", "lake", "total"}
    assert report["throughput"]["transactions_per_second"] > 0
    assert len(report["per_session"]) == 2
    path = benchmark.write_report(report, tmp_path / "reports")
    assert json.loads(path.read_text(encoding="utf-8"))["kind"] == "batch"
    with pytest.raises(FileExistsError):  # never reuses a scratch folder
        benchmark.benchmark_batch(sessions, tmp_path / "work")


def test_percentiles_and_offline_fetch():
    stats = benchmark._percentiles([float(v) for v in range(1, 101)])
    assert (stats["count"], stats["p50"], stats["max"]) == (100, 51.0, 100.0)
    assert benchmark._percentiles([]) == {}
    with pytest.raises(RuntimeError, match="never calls the public API"):
        benchmark._offline_fetch("a" * 64)
