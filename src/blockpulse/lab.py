"""Run the structural rules against small, labeled teaching scenarios."""

import csv
import hashlib
import json
from pathlib import Path

from blockpulse.detector import DetectorConfig, RULESET_VERSION, screen_transaction
from blockpulse.features import FEATURE_NAMES, FEATURE_SET_VERSION
from blockpulse.storage import JsonlWriter, write_summary


def _case(name, kind, description, n_in, n_out, repeated, expected):
    return {
        "case_name": name,
        "case_kind": kind,
        "description": description,
        "expected_rule_ids": expected,
        "features": {
            "n_in": n_in,
            "n_out": n_out,
            "value_out_sat": 100_000,
            "fee_sat": 1_000,
            "vsize": 500,
            "fee_rate_sat_vb": 2.0,
            "max_equal_output_count": repeated,
            "round_amount_fraction": 0.0,
            "rbf_signaled": None,
        },
    }


def default_cases():
    """Return controlled shapes, including benign counterexamples and boundaries."""
    return [
        _case("ordinary_transfer", "ordinary_control", "One input and two outputs.", 1, 2, 1, []),
        _case("batch_payout_shape", "target_shape", "Few inputs and many outputs.", 1, 12, 1, ["many_outputs"]),
        _case("benign_batch_counterexample", "benign_counterexample", "A routine batch can have the same shape as a flagged fan-out.", 1, 12, 1, ["many_outputs"]),
        _case("consolidation_shape", "target_shape", "Many inputs and one output.", 12, 1, 1, ["many_inputs"]),
        _case("benign_consolidation_counterexample", "benign_counterexample", "A benign consolidation can have the same shape as a flagged fan-in.", 12, 1, 1, ["many_inputs"]),
        _case("repeated_outputs_shape", "target_shape", "Several inputs and three equal positive outputs.", 3, 3, 3, ["repeated_output_values"]),
        _case("fanin_just_below_default", "boundary_control", "Nine inputs is just below the default fan-in threshold.", 9, 1, 1, []),
        _case("fanout_at_default_boundary", "boundary_shape", "Ten outputs is exactly the default fan-out threshold.", 2, 10, 1, ["many_outputs"]),
    ]


def run_rule_lab(output: Path, config: DetectorConfig | None = None) -> dict:
    """Write scenario-level results; these are rule checks, not model metrics."""
    output.mkdir(parents=True, exist_ok=False)
    config = config or DetectorConfig()
    cases = default_cases()
    csv_fields = (
        "case_name", "case_kind", "description", *FEATURE_NAMES,
        "expected_rule_ids", "actual_rule_ids", "matches_expected_rules",
    )
    summary = {
        "schema_version": 1,
        "feature_set_version": FEATURE_SET_VERSION,
        "ruleset_version": RULESET_VERSION,
        "thresholds": config.__dict__,
        "scenario_count": len(cases),
        "cases_matching_expected_rules": 0,
        "cases_with_signals": 0,
        "signal_counts": {},
        "threshold_sweep_rows": 0,
        "purpose": "Controlled rule-mechanics exercise only; not real-world accuracy or risk evaluation.",
    }
    canonical = json.dumps(cases, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with (
        JsonlWriter(output / "cases.jsonl") as records,
        (output / "cases.csv").open("x", encoding="utf-8", newline="") as table_file,
        JsonlWriter(output / "threshold_sweep.jsonl") as sweep_records,
        (output / "threshold_sweep.csv").open("x", encoding="utf-8", newline="") as sweep_file,
    ):
        table = csv.DictWriter(table_file, fieldnames=csv_fields)
        table.writeheader()
        sweep_columns = ("rule_id", "threshold", "case_name", "case_kind", "n_in", "n_out", "matched")
        sweep_table = csv.DictWriter(sweep_file, fieldnames=sweep_columns)
        sweep_table.writeheader()
        for case in cases:
            result = screen_transaction(case["features"], config)
            actual = sorted(signal["rule_id"] for signal in result["signals"])
            expected = sorted(case["expected_rule_ids"])
            matches_expected = actual == expected
            row = {
                "schema_version": 1,
                "feature_set_version": FEATURE_SET_VERSION,
                "ruleset_version": RULESET_VERSION,
                "case_name": case["case_name"],
                "case_kind": case["case_kind"],
                "description": case["description"],
                "features": case["features"],
                "expected_rule_ids": expected,
                "actual_rule_ids": actual,
                "matches_expected_rules": matches_expected,
                "signals": result["signals"],
                "interpretation": result["interpretation"],
            }
            records.append(row)
            table.writerow({
                "case_name": case["case_name"],
                "case_kind": case["case_kind"],
                "description": case["description"],
                **case["features"],
                "expected_rule_ids": ";".join(expected),
                "actual_rule_ids": ";".join(actual),
                "matches_expected_rules": str(matches_expected).lower(),
            })
            summary["cases_matching_expected_rules"] += matches_expected
            summary["cases_with_signals"] += bool(actual)
            for rule_id in actual:
                summary["signal_counts"][rule_id] = summary["signal_counts"].get(rule_id, 0) + 1
        # Change one threshold at a time so learners can see its direct effect.
        for rule_id, field, values in (
            ("many_inputs", "many_inputs_min", (5, 10, 20)),
            ("many_outputs", "many_outputs_min", (5, 10, 20)),
        ):
            for threshold in values:
                sweep_config = DetectorConfig(**{**config.__dict__, field: threshold})
                for case in cases:
                    result = screen_transaction(case["features"], sweep_config)
                    matched = any(signal["rule_id"] == rule_id for signal in result["signals"])
                    sweep_row = {
                        "schema_version": 1,
                        "rule_id": rule_id,
                        "threshold": threshold,
                        "case_name": case["case_name"],
                        "case_kind": case["case_kind"],
                        "n_in": case["features"]["n_in"],
                        "n_out": case["features"]["n_out"],
                        "matched": matched,
                    }
                    sweep_records.append(sweep_row)
                    sweep_table.writerow({key: str(value).lower() if key == "matched" else value for key, value in sweep_row.items() if key != "schema_version"})
                    summary["threshold_sweep_rows"] += 1
    summary["scenario_sha256"] = hashlib.sha256(canonical).hexdigest()
    summary["interpretation_note"] = (
        "Expected rules describe the shape each synthetic case was designed to exercise. "
        "Benign counterexamples intentionally receive the same rule as their shape twin. "
        "The sweep varies one count threshold at a time. This is not a precision, recall, "
        "fraud, or real-world quality score."
    )
    write_summary(output / "summary.json", summary)
    return summary
