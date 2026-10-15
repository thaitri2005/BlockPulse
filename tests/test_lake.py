import json
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.dataset as ds  # noqa: E402

from blockpulse import collector, lake  # noqa: E402
from tests.test_collector import captured_layout  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def test_terraform_schema_file_matches_the_python_contract():
    committed = json.loads((ROOT / "infra/aws/terraform/lake_schema.json").read_text(encoding="utf-8"))
    assert committed == lake.glue_columns(), "run: python -m blockpulse lake-schema --output infra/aws/terraform/lake_schema.json"


def test_session_becomes_partitioned_parquet_that_engines_can_prune(tmp_path, transaction):
    layout = captured_layout(tmp_path, transaction, sessions=1)
    collector.process_pending(layout, lake=False)
    name = collector.finished_captures(layout)[0]

    report = lake.build_session(layout.root, name)
    tx = report["tables"]["transactions"]
    assert tx["rows_in"] == tx["rows_written"] == 2 and tx["rows_quarantined"] == 0
    assert all(p.startswith("dt=") and "/hour=" in p for p in tx["partitions"])
    assert report["tables"]["capture_sessions"]["rows_written"] == 1

    dataset = ds.dataset(layout.root / "lake" / "transactions", format="parquet", partitioning="hive")
    table = dataset.to_table()
    assert table.num_rows == 2 and {"dt", "hour"} <= set(table.column_names)
    assert dataset.schema.field("received_at").type == pa.timestamp("ms", tz="UTC")
    assert table.column("rule_ids").type == pa.list_(pa.string())
    one_hour = dataset.to_table(filter=ds.field("hour") == int(tx["partitions"][0].split("hour=")[1]))
    assert one_hour.num_rows >= 1  # partition pruning works on the hour key

    files = sorted((layout.root / "lake").rglob(f"{name}.parquet"))
    before = {p: p.read_bytes() for p in files}
    lake.build_session(layout.root, name)  # rebuild: same files, same bytes
    assert {p: p.read_bytes() for p in sorted((layout.root / "lake").rglob(f"{name}.parquet"))} == before
    assert lake.report_path(layout.root, name).is_file()
    assert set(lake.session_files(layout.root, name)) >= set(files)


def test_quality_gate_quarantines_bad_rows_with_reasons(tmp_path, transaction):
    layout = captured_layout(tmp_path, transaction, sessions=1)
    collector.process_pending(layout, lake=False)
    name = collector.finished_captures(layout)[0]
    screened = layout.screened / name / "screened.jsonl"
    rows = [json.loads(line) for line in screened.read_text(encoding="utf-8").splitlines()]
    good = rows[0]
    bad_txid = {**good, "txid": "NOT-A-TXID"}
    negative_fee = {**good, "txid": "f" * 64, "features": {**good["features"], "fee_sat": -5}}
    lying_flag = {**good, "txid": "e" * 64, "is_flagged": True}  # flagged without any signal
    wrong_type = {**good, "txid": "d" * 64, "features": {**good["features"], "n_in": "1"}}
    duplicate = dict(good)
    screened.write_text("".join(json.dumps(r) + "\n" for r in [good, bad_txid, negative_fee, lying_flag, wrong_type, duplicate]),
                        encoding="utf-8")

    report = lake.build_session(layout.root, name)
    tx = report["tables"]["transactions"]
    assert (tx["rows_in"], tx["rows_written"], tx["rows_quarantined"]) == (6, 1, 5)
    assert tx["failed_checks"] == {"check:txid_format": 1, "check:amounts_nonnegative": 1, "check:flag_matches_signals": 1,
                                   "type:n_in": 1, "duplicate_key": 1}
    quarantined = [json.loads(line) for line in
                   (layout.root / "lake/_quarantine/transactions" / f"{name}.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(quarantined) == 5 and all(item["errors"] for item in quarantined)

    screened.write_text(json.dumps(good) + "\n", encoding="utf-8")  # fixed input: the stale quarantine file goes away
    lake.build_session(layout.root, name)
    assert not (layout.root / "lake/_quarantine/transactions" / f"{name}.jsonl").exists()


def test_events_without_a_txid_are_quarantined_not_dropped():
    good, bad, failures = lake.validate(lake.TABLES["lifecycle_events"], [
        {"event_id": "e1", "session": "s", "run_id": "r", "txid": "a" * 64, "event_type": "added",
         "event_time": "2026-10-15T04:00:00+00:00", "source_sequence": 1, "replacement_txid": None, "source_message_id": "r:1"},
        {"event_id": "e2", "session": "s", "run_id": "r", "txid": None, "event_type": "added",
         "event_time": "2026-10-15T04:00:00+00:00", "source_sequence": 2, "replacement_txid": None, "source_message_id": "r:2"},
        {"event_id": "e3", "session": "s", "run_id": "r", "txid": "b" * 64, "event_type": "teleported",
         "event_time": "2026-10-15T04:00:00", "source_sequence": None, "replacement_txid": None, "source_message_id": "r:3"},
    ])
    assert [r["event_id"] for r in good] == ["e1"]
    assert failures == {"type:txid": 1, "type:event_time": 1}
    assert any("txid is null" in e for e in bad[0]["errors"])


def test_collector_builds_the_lake_and_uploads_it(tmp_path, transaction, monkeypatch):
    import sys
    from tests.test_collector import FAKE_AWS
    layout = captured_layout(tmp_path / "data", transaction, sessions=1)
    result = collector.process_pending(layout)
    assert result["failed"] == [] and result["sessions"][0]["lake"] is True
    assert collector.pending_sessions(layout) == []

    fake = tmp_path / "fake_aws.py"
    fake.write_text(FAKE_AWS, encoding="utf-8")
    calls = tmp_path / "calls.jsonl"
    monkeypatch.setenv("FAKE_AWS_LOG", str(calls))
    collector.sync_pending(layout, bucket="b", aws_command=[sys.executable, str(fake)])
    destinations = [json.loads(line)[-1] for line in calls.read_text().splitlines()]
    name = collector.finished_captures(layout)[0]
    assert f"s3://b/data/lake/_quality/{name}.json" in destinations
    assert any(d.startswith("s3://b/data/lake/transactions/dt=") and d.endswith(f"/{name}.parquet") for d in destinations)
    assert any(d.startswith("s3://b/data/lake/capture_sessions/dt=") for d in destinations)
