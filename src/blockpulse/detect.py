"""Offline screening of transaction-v2 feature records."""

import csv
import hashlib
import json
from pathlib import Path

from blockpulse.detector import DetectorConfig, RULESET_VERSION, screen_transaction
from blockpulse.events import reject_nonfinite
from blockpulse.features import FEATURE_NAMES, FEATURE_SET_VERSION
from blockpulse.storage import JsonlWriter, write_summary


def detect_features(source: Path, output: Path, config: DetectorConfig | None = None) -> dict:
    source = source.resolve()
    if not source.is_file():
        raise ValueError(f"Feature file does not exist: {source}")
    output.mkdir(parents=True, exist_ok=False)
    config = config or DetectorConfig()
    summary = {
        "schema_version": 1,
        "source_file": str(source),
        "feature_set_version": FEATURE_SET_VERSION,
        "ruleset_version": RULESET_VERSION,
        "thresholds": config.__dict__,
        "rows_read": 0,
        "rows_screened": 0,
        "flagged_rows": 0,
        "signal_counts": {},
        "issue_count": 0,
    }
    source_hash = hashlib.sha256()
    seen_txids = {}
    csv_fields = (
        "txid", "event_id", "received_at", "source_mode", "feature_set_version",
        "n_in", "n_out", "value_out_sat", "fee_sat", "vsize", "fee_rate_sat_vb",
        "max_equal_output_count", "round_amount_fraction", "rbf_signaled",
        "ruleset_version", "signal_count", "is_flagged", "signal_ids", "explanations",
    )
    with (
        source.open("rb") as rows_in,
        JsonlWriter(output / "screened.jsonl") as screened_file,
        JsonlWriter(output / "alerts.jsonl") as alerts_file,
        JsonlWriter(output / "errors.jsonl") as errors_file,
        (output / "screened.csv").open("x", encoding="utf-8", newline="") as csv_file,
    ):
        table = csv.DictWriter(csv_file, fieldnames=csv_fields)
        table.writeheader()
        for line_number, raw_line in enumerate(rows_in, start=1):
            summary["rows_read"] += 1
            source_hash.update(raw_line)
            try:
                row = json.loads(raw_line, parse_constant=reject_nonfinite)
                if not isinstance(row, dict):
                    raise ValueError("Feature row must be a JSON object")
                if row.get("feature_set_version") != FEATURE_SET_VERSION:
                    raise ValueError(
                        f"Expected feature set {FEATURE_SET_VERSION}; got {row.get('feature_set_version')!r}. "
                        "Reprocess the raw capture with the current code."
                    )
                txid, event_id = row.get("txid"), row.get("event_id")
                if not isinstance(txid, str) or not txid:
                    raise ValueError("txid is required")
                if not isinstance(event_id, str) or not event_id:
                    raise ValueError("event_id is required")
                features = {name: row.get(name) for name in FEATURE_NAMES}
                result = screen_transaction(features, config)
                prior = seen_txids.get(txid)
                if prior is not None:
                    if prior != features:
                        raise ValueError("Conflicting feature rows for the same txid")
                    continue
                seen_txids[txid] = features
                screened = {
                    "schema_version": 1,
                    "txid": txid,
                    "event_id": event_id,
                    "received_at": row.get("received_at"),
                    "source_mode": row.get("source_mode"),
                    "feature_set_version": FEATURE_SET_VERSION,
                    "features": features,
                    **result,
                }
                screened_file.append(screened)
                flat_row = {
                    **{key: row.get(key) for key in ("txid", "event_id", "received_at", "source_mode")},
                    "feature_set_version": FEATURE_SET_VERSION,
                    **features,
                    "ruleset_version": result["ruleset_version"],
                    "signal_count": result["signal_count"],
                    "is_flagged": str(result["is_flagged"]).lower(),
                    "signal_ids": ";".join(signal["rule_id"] for signal in result["signals"]),
                    "explanations": " | ".join(signal["explanation"] for signal in result["signals"]),
                }
                table.writerow(flat_row)
                summary["rows_screened"] += 1
                summary["flagged_rows"] += result["is_flagged"]
                for signal in result["signals"]:
                    summary["signal_counts"][signal["rule_id"]] = summary["signal_counts"].get(signal["rule_id"], 0) + 1
                if result["is_flagged"]:
                    alerts_file.append(screened)
            except (ValueError, TypeError, ZeroDivisionError, OverflowError) as error:
                errors_file.append({"line": line_number, "kind": "invalid_feature_row", "detail": str(error)})
                summary["issue_count"] += 1
    summary["input_sha256"] = source_hash.hexdigest()
    summary["coverage_note"] = "These rule signals identify structural patterns; they are not calibrated risk probabilities or ground-truth labels."
    write_summary(output / "summary.json", summary)
    return summary
