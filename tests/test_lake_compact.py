import shutil
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")

from blockpulse.cli import main  # noqa: E402
from blockpulse.lake_compact import COMPACT_NAME, compact_lake  # noqa: E402
from blockpulse.stream_lake import write_batch  # noqa: E402


def _tx(n: int, hour: int) -> dict:
    return {"txid": f"{n:064x}", "event_id": f"e{n}", "session": "run", "received_at": f"2026-01-01T{hour:02d}:00:{n % 60:02d}+00:00",
            "source_mode": "synthetic", "feature_set_version": "transaction-v2", "n_in": 1, "n_out": 2, "value_out_sat": 1000 + n,
            "fee_sat": 100, "vsize": 141, "fee_rate_sat_vb": 0.709, "max_equal_output_count": 1, "round_amount_fraction": 0.0,
            "rbf_signaled": True, "ruleset_version": "rules", "is_flagged": n % 2 == 0, "signal_count": 1 if n % 2 == 0 else 0,
            "rule_ids": ["many_outputs"] if n % 2 == 0 else []}


def _lake(tmp_path: Path) -> Path:
    """Three hours, four small batches in each, as the stream sink would leave them."""
    lake = tmp_path / "lake-stream"
    number = 0
    for hour in (5, 6, 7):
        for batch in range(4):
            rows = [_tx(number + i, hour) for i in range(5)]
            number += 5
            report = write_batch(lake, f"stream-p0-o{hour * 1000 + batch:012d}", {"transactions": rows, "lifecycle_events": []})
            assert report["tables"]["transactions"]["rows_written"] == 5, report
    return lake


def _rows(lake: Path) -> list[tuple]:
    import duckdb
    pattern = (lake / "transactions" / "*" / "*" / "*.parquet").as_posix()
    return duckdb.sql(f"SELECT txid, received_at, value_out_sat, is_flagged, rule_ids FROM read_parquet('{pattern}') ORDER BY txid").fetchall()


def _files(lake: Path) -> list[str]:
    return sorted(p.relative_to(lake).as_posix() for p in lake.glob("transactions/*/*/*.parquet"))


def test_closed_hours_become_one_file_each_and_the_rows_are_unchanged(tmp_path):
    lake = _lake(tmp_path)
    before = _rows(lake)
    summary = compact_lake(lake)
    stats = summary["tables"]["transactions"]
    assert stats == {"partitions": 3, "partitions_compacted": 2, "partitions_skipped_open": 1, "files_before": 8,
                     "files_after": 2, "rows": 40, "duplicate_rows_dropped": 0}
    assert _rows(lake) == before and len(before) == 60
    files = _files(lake)
    assert [f for f in files if "hour=05" in f or "hour=06" in f] == [
        f"transactions/dt=2026-01-01/hour=05/{COMPACT_NAME}", f"transactions/dt=2026-01-01/hour=06/{COMPACT_NAME}"]
    assert len([f for f in files if "hour=07" in f]) == 4  # the newest hour may still be written to: left alone


def test_running_again_changes_nothing(tmp_path):
    lake = _lake(tmp_path)
    compact_lake(lake)
    files, rows = _files(lake), _rows(lake)
    again = compact_lake(lake)
    assert again["tables"]["transactions"]["partitions_compacted"] == 0
    assert _files(lake) == files and _rows(lake) == rows


def test_a_crash_between_rename_and_delete_heals_on_the_next_run(tmp_path):
    lake = _lake(tmp_path)
    before = _rows(lake)
    hour = lake / "transactions" / "dt=2026-01-01" / "hour=05"
    saved = tmp_path / "saved"
    shutil.copytree(hour, saved)
    compact_lake(lake)
    for leftover in saved.glob("*.parquet"):  # the inputs a crashed run failed to delete
        shutil.copy(leftover, hour / leftover.name)
    assert len(_rows(lake)) == 80  # readers would see the hour twice until the next run
    summary = compact_lake(lake)
    assert summary["tables"]["transactions"]["duplicate_rows_dropped"] == 20
    assert _rows(lake) == before


def test_include_latest_compacts_a_lake_nobody_writes_to(tmp_path):
    lake = _lake(tmp_path)
    before = _rows(lake)
    summary = compact_lake(lake, include_latest=True)
    assert summary["tables"]["transactions"]["files_after"] == 3 and len(_files(lake)) == 3
    assert _rows(lake) == before


def test_cli_reports_and_rejects_a_missing_lake(tmp_path, capsys):
    lake = _lake(tmp_path)
    assert main(["lake-compact", "--lake", str(lake)]) == 0
    assert '"partitions_compacted": 2' in capsys.readouterr().out
    assert main(["lake-compact", "--lake", str(tmp_path / "nope")]) != 0
