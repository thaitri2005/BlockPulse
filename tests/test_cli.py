import json
from unittest.mock import patch

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


def test_kafka_commands_wire_arguments_without_starting_broker(tmp_path, capsys):
    source = tmp_path / "messages.jsonl"
    source.write_text("", encoding="utf-8")
    archive = tmp_path / "archive"
    with patch("blockpulse.cli.create_m1_topics", return_value={"topic": "blockpulse.raw.v1", "created": True}) as init:
        assert main(["kafka-init", "--bootstrap-servers", "broker:9092"]) == 0
        init.assert_called_once_with("broker:9092")
    assert "Saved to:" not in capsys.readouterr().out
    with patch("blockpulse.cli.publish_capture", return_value={"messages_published": 0}) as replay:
        assert main(["kafka-replay", str(source)]) == 0
        replay.assert_called_once_with(source, "127.0.0.1:9092")
    capsys.readouterr()
    with patch("blockpulse.cli.archive_topic", return_value={"messages_archived": 2}) as consume:
        assert main([
            "kafka-archive", "--output", str(archive), "--group-id", "study-1",
            "--max-messages", "2", "--idle-timeout", "1.5",
        ]) == 0
        consume.assert_called_once_with(
            archive, "study-1", "127.0.0.1:9092", max_messages=2, idle_timeout=1.5,
        )


def test_capture_kafka_writes_file_then_uses_kafka_sink(tmp_path, capsys):
    import asyncio
    from unittest.mock import AsyncMock
    from blockpulse.capture import CaptureConfig
    output = tmp_path / "capture"
    output.mkdir()
    publisher = type("Publisher", (), {"__call__": lambda self, row: None,
                                        "close": lambda self: {"messages_queued": 1, "messages_acknowledged": 1},
                                        "summary": lambda self: {"messages_queued": 1}})()
    async def fake_capture(config, path, *, on_message):
        assert isinstance(config, CaptureConfig)
        assert path == output
        assert on_message is publisher
        return {"stop_reason": "message_limit", "event_counts": {"added": 1}, "run_id": "r"}
    with patch("blockpulse.cli.create_m1_topics") as init, \
         patch("blockpulse.cli.RawKafkaPublisher", return_value=publisher), \
         patch("blockpulse.capture.capture", side_effect=fake_capture):
        assert main(["capture-kafka", "--output", str(output), "--bootstrap-servers", "broker:9092"]) == 0
    init.assert_called_once_with("broker:9092")
    saved = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert saved["kafka_publish"]["messages_acknowledged"] == 1
    capsys.readouterr()
