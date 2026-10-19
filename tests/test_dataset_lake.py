import json

import pytest

pytest.importorskip("pyarrow")

from blockpulse import collector, dataset  # noqa: E402
from tests.test_collector import captured_layout  # noqa: E402


@pytest.fixture
def lake_layout(tmp_path, transaction):
    layout = captured_layout(tmp_path, transaction, sessions=4)
    assert collector.process_pending(layout)["failed"] == []
    manifest = dataset.build_manifest(layout.screened, min_rows=1)
    assert len(manifest["sessions"]) == 4
    return layout, manifest


@pytest.mark.parametrize("split", dataset.SPLITS)
def test_lake_and_jsonl_give_identical_rows(lake_layout, split):
    layout, manifest = lake_layout
    from_jsonl = dataset.load_split(manifest, split, layout.screened, source="jsonl")
    from_lake = dataset.load_split(manifest, split, layout.screened, source="lake")
    assert from_lake == from_jsonl and from_lake
    assert dataset.load_split(manifest, split, layout.screened) == from_jsonl  # auto


def test_lake_source_reads_no_json(lake_layout, monkeypatch):
    layout, manifest = lake_layout
    monkeypatch.setattr(dataset, "_read_jsonl", lambda *a, **k: pytest.fail("JSONL read in lake mode"))
    assert dataset.load_split(manifest, "train", layout.screened, source="lake")


def test_auto_falls_back_per_session_and_lake_mode_refuses(lake_layout):
    layout, manifest = lake_layout
    first = manifest["sessions"][0]["run_id"]
    report = layout.root / "lake" / "_quality" / f"{first}.json"
    stats = json.loads(report.read_text(encoding="utf-8"))
    stats["tables"]["transactions"]["rows_quarantined"] = 1  # the lake no longer matches the manifest
    report.write_text(json.dumps(stats), encoding="utf-8")
    expected = dataset.load_split(manifest, "train", layout.screened, source="jsonl")
    assert dataset.load_split(manifest, "train", layout.screened) == expected
    with pytest.raises(ValueError, match="no complete lake partition"):
        dataset.load_split(manifest, "train", layout.screened, source="lake")


def test_unknown_source_is_rejected(lake_layout):
    layout, manifest = lake_layout
    with pytest.raises(ValueError, match="source must be"):
        dataset.load_split(manifest, "train", layout.screened, source="csv")


def test_legacy_manifest_keeps_file_order_and_refuses_the_lake(lake_layout):
    layout, manifest = lake_layout
    legacy = {**manifest, "selection": {k: v for k, v in manifest["selection"].items() if k != "row_order"}}
    rows = dataset.load_split(legacy, "train", layout.screened)
    in_file_order = [json.loads(line)["txid"] for line in
                     (layout.screened / legacy["sessions"][0]["screened_file"]).read_text(encoding="utf-8").splitlines()]
    assert [r["txid"] for r in rows][:len(in_file_order)] == in_file_order
    with pytest.raises(ValueError, match="predates canonical row order"):
        dataset.load_split(legacy, "train", layout.screened, source="lake")


def test_verify_manifest_detects_tampering_and_changed_files(lake_layout):
    layout, manifest = lake_layout
    assert dataset.verify_manifest(manifest, layout.screened)["ok"] is True
    assert dataset.verify_manifest({**manifest, "min_rows_note": "edited"}, layout.screened)["content_matches_id"] is False
    target = layout.screened / manifest["sessions"][1]["screened_file"]
    target.write_text(target.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    result = dataset.verify_manifest(manifest, layout.screened)
    assert result["ok"] is False and result["changed_or_missing"] == [manifest["sessions"][1]["run_id"]]
