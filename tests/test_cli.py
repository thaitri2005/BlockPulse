import json

from blockpulse.cli import main
from blockpulse.features import FEATURE_SET_VERSION, extract_features


def test_process_command_reports_output_and_refuses_overwrite(tmp_path, transaction, record_factory, capsys):
    source = tmp_path / "messages.jsonl"
    source.write_text(json.dumps(record_factory({"mempool-transactions": {"added": [transaction]}})) + "\n", encoding="utf-8")
    output = tmp_path / "results"
    args = ["process", str(source), "--output", str(output)]
    assert main(args) == 0
    assert "Saved to:" in capsys.readouterr().out
    features = (output / "features.jsonl").read_bytes()
    assert main(args) == 1
    assert (output / "features.jsonl").read_bytes() == features


def test_empty_capture_is_reported_without_fake_features(tmp_path):
    source = tmp_path / "empty.jsonl"
    source.write_text("", encoding="utf-8")
    output = tmp_path / "results"
    assert main(["process", str(source), "--output", str(output)]) == 3
    assert (output / "features.jsonl").read_bytes() == b""
    assert json.loads((output / "summary.json").read_text())["features_written"] == 0


def test_missing_source_and_invalid_capture_settings_do_not_create_output(tmp_path):
    output = tmp_path / "results"
    assert main(["process", str(tmp_path / "missing.jsonl"), "--output", str(output)]) == 1
    assert not output.exists()
    assert main(["capture", "--duration", "-1", "--output", str(output)]) == 1
    assert not output.exists()


def test_detect_command_screens_feature_file(tmp_path, transaction, capsys):
    source = tmp_path / "features.jsonl"
    row = {
        "schema_version": 1, "txid": "a" * 64, "event_id": "event-1",
        "feature_set_version": FEATURE_SET_VERSION, "source_mode": "synthetic",
        **extract_features(transaction), "n_in": 168,
    }
    source.write_text(json.dumps(row) + "\n", encoding="utf-8")
    output = tmp_path / "screened"
    assert main(["detect", str(source), "--output", str(output)]) == 0
    assert json.loads((output / "summary.json").read_text())["flagged_rows"] == 1
    assert "Saved to:" in capsys.readouterr().out


def test_lab_command_writes_scenario_and_sweep_results(tmp_path, capsys):
    output = tmp_path / "lab"
    assert main(["lab", "--output", str(output)]) == 0
    summary = json.loads((output / "summary.json").read_text())
    assert summary["scenario_count"] == 8
    assert summary["threshold_sweep_rows"] == 48
    assert (output / "threshold_sweep.csv").is_file()
    assert "Saved to:" in capsys.readouterr().out
