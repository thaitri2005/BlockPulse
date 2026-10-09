import csv
import json

import pytest

from blockpulse.lab import run_rule_lab


def test_rule_lab_checks_cases_and_exposes_benign_counterexamples(tmp_path):
    output = tmp_path / "lab"
    summary = run_rule_lab(output)
    assert summary["scenario_count"] == 8
    assert summary["cases_matching_expected_rules"] == 8
    assert summary["threshold_sweep_rows"] == 48
    assert summary["signal_counts"] == {
        "many_outputs": 3,
        "many_inputs": 2,
        "repeated_output_values": 1,
    }
    rows = [json.loads(line) for line in (output / "cases.jsonl").read_text().splitlines()]
    by_name = {row["case_name"]: row for row in rows}
    batch = by_name["batch_payout_shape"]
    benign_batch = by_name["benign_batch_counterexample"]
    assert batch["features"] == benign_batch["features"]
    assert batch["actual_rule_ids"] == benign_batch["actual_rule_ids"] == ["many_outputs"]
    with (output / "threshold_sweep.csv").open(encoding="utf-8", newline="") as file:
        sweep = list(csv.DictReader(file))
    fanin_nine = [row for row in sweep if row["case_name"] == "fanin_just_below_default"]
    assert {row["threshold"]: row["matched"] for row in fanin_nine if row["rule_id"] == "many_inputs"} == {
        "5": "true", "10": "false", "20": "false",
    }
    with pytest.raises(FileExistsError):
        run_rule_lab(output)
