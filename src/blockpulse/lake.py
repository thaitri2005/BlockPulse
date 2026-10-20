"""Partitioned Parquet lake with a data-quality gate.

Every processed session is written into three analytics tables, partitioned Hive-style so Athena, DuckDB, Spark
and pyarrow can all prune by time without reading everything:

    <root>/lake/transactions/dt=YYYY-MM-DD/hour=HH/<session>.parquet      one row per screened transaction
    <root>/lake/lifecycle_events/dt=YYYY-MM-DD/hour=HH/<session>.parquet  one row per lifecycle observation
    <root>/lake/capture_sessions/dt=YYYY-MM-DD/<session>.parquet          one row per capture (coverage metadata)
    <root>/lake/_quarantine/<table>/<session>.jsonl   rows that failed the contract, each with its reasons
    <root>/lake/_quality/<session>.json               per-session quality report; its presence marks the stage done

Each table has a declared contract (columns, types, nullability, row checks). Rows that break it are quarantined
rather than written or silently fixed, and the report counts every failed check. Files are named after the
session, so rebuilding a session overwrites the same files with the same bytes: the stage is idempotent.
The schema here is the single source of truth; `glue_columns()` exports it for the Athena tables in Terraform.
"""

import gzip
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

LAKE_SCHEMA_VERSION = 1
TXID = re.compile(r"^[0-9a-f]{64}$")
EVENT_TYPES = {"added", "removed", "mined", "replaced"}


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # string | bigint | int | double | boolean | timestamp | array<string>
    nullable: bool = False
    comment: str = ""


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column, ...]
    key: str
    time_column: str
    hourly: bool = True


TABLES = {
    "transactions": Table("transactions", (
        Column("txid", "string", comment="Transaction id"),
        Column("event_id", "string", comment="Lifecycle event the features came from"),
        Column("session", "string", comment="Capture session"),
        Column("received_at", "timestamp", comment="UTC receive time of the observation"),
        Column("source_mode", "string"),
        Column("feature_set_version", "string"),
        Column("n_in", "int"), Column("n_out", "int"),
        Column("value_out_sat", "bigint", comment="Sum of outputs, includes change"),
        Column("fee_sat", "bigint"), Column("vsize", "int"),
        Column("fee_rate_sat_vb", "double"),
        Column("max_equal_output_count", "int"),
        Column("round_amount_fraction", "double"),
        Column("rbf_signaled", "boolean", nullable=True, comment="Null when inputs were incomplete"),
        Column("ruleset_version", "string"),
        Column("is_flagged", "boolean", comment="Structural rule match; not evidence of wrongdoing"),
        Column("signal_count", "int"),
        Column("rule_ids", "array<string>"),
    ), key="txid", time_column="received_at"),
    "lifecycle_events": Table("lifecycle_events", (
        Column("event_id", "string"),
        Column("session", "string"),
        Column("run_id", "string"),
        Column("txid", "string"),
        Column("event_type", "string", comment="added | removed | mined | replaced"),
        Column("event_time", "timestamp"),
        Column("source_sequence", "bigint", nullable=True),
        Column("replacement_txid", "string", nullable=True, comment="For replaced: the transaction that replaced it"),
        Column("source_message_id", "string"),
    ), key="event_id", time_column="event_time"),
    "capture_sessions": Table("capture_sessions", (
        Column("session", "string"),
        Column("run_id", "string", nullable=True),
        Column("source_url", "string", nullable=True),
        Column("started_at", "timestamp"),
        Column("ended_at", "timestamp", nullable=True),
        Column("elapsed_seconds", "double"),
        Column("stop_reason", "string"),
        Column("messages_saved", "bigint"),
        Column("connection_errors", "bigint"),
        Column("recorded_gaps", "bigint"),
        Column("recorded_gap_seconds", "double", comment="Lower bound on missing coverage"),
        Column("events_added", "bigint"), Column("events_removed", "bigint"),
        Column("events_mined", "bigint"), Column("events_replaced", "bigint"),
    ), key="session", time_column="started_at", hourly=False),
}


def _arrow_type(pa, name: str):
    return {"string": pa.string(), "bigint": pa.int64(), "int": pa.int32(), "double": pa.float64(),
            "boolean": pa.bool_(), "timestamp": pa.timestamp("ms", tz="UTC"), "array<string>": pa.list_(pa.string())}[name]


def arrow_schema(table: Table):
    import pyarrow as pa
    return pa.schema([pa.field(c.name, _arrow_type(pa, c.type), nullable=c.nullable) for c in table.columns],
                     metadata={"blockpulse.lake_schema_version": str(LAKE_SCHEMA_VERSION), "blockpulse.table": table.name})


def glue_columns() -> dict:
    """The lake schema in the shape Terraform needs for Glue tables (written to infra/aws/terraform/lake_schema.json)."""
    return {"schema_version": LAKE_SCHEMA_VERSION, "tables": {
        t.name: {"hourly": t.hourly, "columns": [{"name": c.name, "type": c.type, "comment": c.comment} for c in t.columns]}
        for t in TABLES.values()}}


# ---------------------------------------------------------------- contract checks

def _parse_time(value):
    if isinstance(value, str):
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if moment.tzinfo is None:
            raise ValueError("timestamp without time zone")
        return moment.astimezone(timezone.utc)
    raise ValueError("timestamp must be an ISO 8601 string")


def _coerce(column: Column, value):
    """Return the typed value or raise ValueError with a short reason."""
    if value is None:
        if column.nullable:
            return None
        raise ValueError(f"{column.name} is null")
    kind = column.type
    if kind == "string" and isinstance(value, str):
        return value
    if kind in ("bigint", "int") and type(value) is int:
        if kind == "int" and not -2**31 <= value < 2**31:
            raise ValueError(f"{column.name} out of int range")
        return value
    if kind == "double" and type(value) in (int, float):
        return float(value)
    if kind == "boolean" and type(value) is bool:
        return value
    if kind == "timestamp":
        try:
            return _parse_time(value)
        except ValueError as error:
            raise ValueError(f"{column.name}: {error}")
    if kind == "array<string>" and isinstance(value, list) and all(isinstance(v, str) for v in value):
        return value
    raise ValueError(f"{column.name} has type {type(value).__name__}, expected {kind}")


ROW_CHECKS = {
    "transactions": [
        ("txid_format", lambda r: bool(TXID.match(r["txid"]))),
        ("counts_positive", lambda r: r["n_in"] >= 1 and r["n_out"] >= 1 and r["vsize"] >= 1),
        ("amounts_nonnegative", lambda r: r["value_out_sat"] >= 0 and r["fee_sat"] >= 0 and r["fee_rate_sat_vb"] >= 0),
        ("fraction_in_range", lambda r: 0.0 <= r["round_amount_fraction"] <= 1.0),
        ("flag_matches_signals", lambda r: r["signal_count"] == len(r["rule_ids"]) and r["is_flagged"] == (r["signal_count"] > 0)),
    ],
    "lifecycle_events": [
        ("txid_format", lambda r: bool(TXID.match(r["txid"]))),
        ("event_type_known", lambda r: r["event_type"] in EVENT_TYPES),
        ("replacement_txid_format", lambda r: r["replacement_txid"] is None or bool(TXID.match(r["replacement_txid"]))),
    ],
    "capture_sessions": [
        ("counts_nonnegative", lambda r: min(r["messages_saved"], r["connection_errors"], r["recorded_gaps"]) >= 0
         and r["elapsed_seconds"] >= 0 and r["recorded_gap_seconds"] >= 0),
    ],
}


def validate(table: Table, rows: list[dict]) -> tuple[list[dict], list[dict], Counter]:
    """Split rows into (typed good rows, quarantined rows with reasons, failure counts by check)."""
    good, bad, failures, seen = [], [], Counter(), set()
    for row in rows:
        errors, typed = [], {}
        for column in table.columns:
            try:
                typed[column.name] = _coerce(column, row.get(column.name))
            except ValueError as error:
                errors.append(f"type: {error}")
                failures[f"type:{column.name}"] += 1
        if not errors:
            for name, check in ROW_CHECKS[table.name]:
                if not check(typed):
                    errors.append(f"check: {name}")
                    failures[f"check:{name}"] += 1
        if not errors and typed[table.key] in seen:
            errors.append(f"duplicate {table.key}")
            failures["duplicate_key"] += 1
        if errors:
            bad.append({"row": row, "errors": errors})
        else:
            seen.add(typed[table.key])
            good.append(typed)
    return good, bad, failures


# ---------------------------------------------------------------- sources

def _jsonl(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as lines:
        for line in lines:
            if line.strip():
                yield json.loads(line)


def transaction_rows(screened_file: Path, session: str) -> list[dict]:
    rows = []
    for row in _jsonl(screened_file):
        features = row.get("features") or {}
        rows.append({"txid": row.get("txid"), "event_id": row.get("event_id"), "session": session,
                     "received_at": row.get("received_at"), "source_mode": row.get("source_mode"),
                     "feature_set_version": row.get("feature_set_version"), **features,
                     "ruleset_version": row.get("ruleset_version"), "is_flagged": row.get("is_flagged"),
                     "signal_count": row.get("signal_count"),
                     "rule_ids": [s.get("rule_id") for s in row.get("signals") or []]})
    return rows


def event_rows(processed: Path, session: str) -> list[dict]:
    plain, packed = processed / "events.jsonl", processed / "events.jsonl.gz"
    source = plain if plain.is_file() else packed
    return [{"event_id": e.get("event_id"), "session": session, "run_id": e.get("run_id"), "txid": e.get("txid"),
             "event_type": e.get("event_type"), "event_time": e.get("event_time"), "source_sequence": e.get("source_sequence"),
             "replacement_txid": e.get("replacement_txid"), "source_message_id": e.get("source_message_id")}
            for e in _jsonl(source)]


def session_rows(raw: Path, session: str) -> list[dict]:
    summary = json.loads((raw / "summary.json").read_text(encoding="utf-8"))
    counts = summary.get("event_counts") or {}
    gaps = summary.get("gaps") or []
    gap_seconds = 0.0
    for gap in gaps:
        try:
            gap_seconds += (_parse_time(gap["end"]) - _parse_time(gap["start"])).total_seconds()
        except (KeyError, ValueError):
            pass
    config = summary.get("config") or {}
    return [{"session": session, "run_id": summary.get("run_id"), "source_url": config.get("url"),
             "started_at": summary.get("started_at"), "ended_at": summary.get("ended_at"),
             "elapsed_seconds": summary.get("elapsed_seconds"), "stop_reason": summary.get("stop_reason"),
             "messages_saved": summary.get("messages_saved"), "connection_errors": summary.get("connection_errors"),
             "recorded_gaps": len(gaps), "recorded_gap_seconds": round(gap_seconds, 3),
             **{f"events_{kind}": counts.get(kind, 0) for kind in ("added", "removed", "mined", "replaced")}}]


# ---------------------------------------------------------------- writing

def _partition(table: Table, moment: datetime) -> str:
    return f"dt={moment:%Y-%m-%d}/hour={moment:%H}" if table.hourly else f"dt={moment:%Y-%m-%d}"


def _write_parquet(path: Path, table: Table, rows: list[dict]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=arrow_schema(table)), temporary, compression="zstd")
    temporary.replace(path)


def report_path(root: Path, session: str) -> Path:
    return root / "lake" / "_quality" / f"{session}.json"


def build_session(root: Path, session: str) -> dict:
    """Validate and write one session's lake tables, then its quality report (written last, marking completion)."""
    sources = {
        "transactions": transaction_rows(root / "screened" / session / "screened.jsonl", session),
        "lifecycle_events": event_rows(root / "processed" / session, session),
        "capture_sessions": session_rows(root / "raw" / session, session),
    }
    lake = root / "lake"
    report = {"schema_version": LAKE_SCHEMA_VERSION, "session": session, "tables": {}}
    for name, rows in sources.items():
        table = TABLES[name]
        good, bad, failures = validate(table, rows)
        partitions = defaultdict(list)
        for row in good:
            partitions[_partition(table, row[table.time_column])].append(row)
        for partition, members in sorted(partitions.items()):
            members.sort(key=lambda r: (r[table.time_column], r[table.key]))
            _write_parquet(lake / name / partition / f"{session}.parquet", table, members)
        quarantine = lake / "_quarantine" / name / f"{session}.jsonl"
        if bad:
            quarantine.parent.mkdir(parents=True, exist_ok=True)
            quarantine.write_text("".join(json.dumps(item, sort_keys=True, default=str) + "\n" for item in bad),
                                  encoding="utf-8", newline="\n")
        elif quarantine.exists():
            quarantine.unlink()
        report["tables"][name] = {"rows_in": len(rows), "rows_written": len(good), "rows_quarantined": len(bad),
                                  "partitions": sorted(partitions), "failed_checks": dict(sorted(failures.items()))}
    path = report_path(root, session)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    return report


def session_files(root: Path, session: str) -> list[Path]:
    """Every lake file that belongs to a session (for upload)."""
    lake = root / "lake"
    return sorted(p for p in lake.rglob(f"{session}.*") if p.is_file() and not p.name.endswith(".tmp")) if lake.is_dir() else []
