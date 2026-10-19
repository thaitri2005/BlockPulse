"""Dataset manifests: a reproducible, checksummed chronological split of screened sessions.

A *session* is one screened run directory (one capture). Splits are made from whole sessions, in time
order, so rows captured together never straddle train/calibration/test. The manifest records exactly
which files went where, so the same split can be rebuilt and verified later.
"""

from datetime import datetime
import hashlib
import json
from pathlib import Path

from blockpulse.events import reject_nonfinite
from blockpulse.features import FEATURE_SET_VERSION

SPLITS = ("train", "calibration", "test")
DEFAULT_FRACTIONS = (0.6, 0.2, 0.2)
MANIFEST_SCHEMA = 1
CANONICAL_ROW_ORDER = "session order, then received_at, then txid"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def choose_cuts(row_counts: list[int], fractions=DEFAULT_FRACTIONS) -> tuple[int, int]:
    """Pick (i, j): sessions [:i] train, [i:j] calibration, [j:] test.

    Chooses the cut points whose row shares are closest to the target fractions, with every split
    non-empty. Ties go to the earlier cut, so the result is deterministic.
    """
    if len(row_counts) < 3:
        raise ValueError("At least 3 usable sessions are needed for train/calibration/test")
    total = sum(row_counts)
    best = None
    for i in range(1, len(row_counts) - 1):
        for j in range(i + 1, len(row_counts)):
            shares = (sum(row_counts[:i]) / total, sum(row_counts[i:j]) / total, sum(row_counts[j:]) / total)
            cost = sum(abs(share - target) for share, target in zip(shares, fractions))
            if best is None or cost < best[0] - 1e-12:
                best = (cost, i, j)
    return best[1], best[2]


def _read_session(directory: Path) -> dict:
    """Summarize one screened directory, or raise ValueError with the reason it can't be used."""
    screened = directory / "screened.jsonl"
    summary_path = directory / "summary.json"
    if not screened.is_file() or not summary_path.is_file():
        raise ValueError("missing screened.jsonl or summary.json")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    rows = flagged = 0
    first = last = None
    versions, modes, rulesets = set(), set(), set()
    with screened.open("rb") as lines:
        for raw in lines:
            row = json.loads(raw, parse_constant=reject_nonfinite)
            moment = datetime.fromisoformat(row["received_at"])
            first = moment if first is None or moment < first else first
            last = moment if last is None or moment > last else last
            rows += 1
            flagged += bool(row["is_flagged"])
            versions.add(row["feature_set_version"])
            modes.add(row.get("source_mode"))
            rulesets.add(row["ruleset_version"])
    if rows == 0:
        raise ValueError("no rows")
    return {
        "run_id": directory.name, "directory": directory, "rows": rows, "flagged": flagged,
        "first": first, "last": last, "feature_versions": versions, "source_modes": modes,
        "rulesets": rulesets, "summary": summary, "screened_sha256": sha256_file(screened),
    }


def _capture_provenance(session: dict) -> dict:
    """Follow screened -> processed -> raw summaries, tolerating a missing link."""
    info = {"capture_sha256": None, "capture": None}
    try:
        processed_dir = Path(session["summary"]["source_file"]).parent
        processed = json.loads((processed_dir / "summary.json").read_text(encoding="utf-8"))
        info["capture_sha256"] = processed.get("input_sha256")
        raw = json.loads((Path(processed["source_file"]).parent / "summary.json").read_text(encoding="utf-8"))
        gaps = raw.get("gaps", [])
        info["capture"] = {
            "elapsed_seconds": raw.get("elapsed_seconds"), "stop_reason": raw.get("stop_reason"),
            "connection_errors": raw.get("connection_errors"), "recorded_gaps": len(gaps),
            "recorded_gap_seconds": round(sum(
                (datetime.fromisoformat(g["end"]) - datetime.fromisoformat(g["start"])).total_seconds() for g in gaps
            ), 3),
        }
    except (OSError, KeyError, ValueError, TypeError):
        pass
    return info


def build_manifest(root: Path, *, min_rows: int = 100, fractions=DEFAULT_FRACTIONS) -> dict:
    """Build a manifest from every screened run directory under `root`."""
    if not root.is_dir():
        raise ValueError(f"Not a directory: {root}")
    usable, excluded = [], []
    for directory in sorted(p for p in root.iterdir() if p.is_dir()):
        try:
            session = _read_session(directory)
        except (ValueError, KeyError, OSError) as error:
            excluded.append({"run_id": directory.name, "reason": f"unreadable: {error}"})
            continue
        reason = None
        if session["source_modes"] != {"ws"}:
            reason = f"not live data (source modes: {sorted(map(str, session['source_modes']))})"
        elif session["feature_versions"] != {FEATURE_SET_VERSION}:
            reason = f"feature set {sorted(session['feature_versions'])} is not {FEATURE_SET_VERSION}"
        elif len(session["rulesets"]) != 1:
            reason = "mixed rule set versions"
        elif session["rows"] < min_rows:
            reason = f"fewer than {min_rows} rows ({session['rows']})"
        if reason:
            excluded.append({"run_id": session["run_id"], "reason": reason})
        else:
            usable.append(session)
    usable.sort(key=lambda s: (s["first"], s["run_id"]))
    i, j = choose_cuts([s["rows"] for s in usable], fractions)
    assignment = ["train"] * i + ["calibration"] * (j - i) + ["test"] * (len(usable) - j)

    sessions, seen_txids, duplicates = [], set(), {}
    for session, split in zip(usable, assignment):
        run_txids = set()
        with (session["directory"] / "screened.jsonl").open("rb") as lines:
            for raw in lines:
                run_txids.add(json.loads(raw)["txid"])
        duplicates[session["run_id"]] = len(run_txids & seen_txids)
        seen_txids |= run_txids
        sessions.append({
            "run_id": session["run_id"], "split": split, "screened_file": f"{session['run_id']}/screened.jsonl",
            "screened_sha256": session["screened_sha256"], "rows": session["rows"], "flagged_rows": session["flagged"],
            "first_received_at": session["first"].isoformat(), "last_received_at": session["last"].isoformat(),
            "ruleset_version": next(iter(session["rulesets"])),
            "txids_already_seen_in_earlier_sessions": duplicates[session["run_id"]],
            **_capture_provenance(session),
        })
    splits = {}
    for name in SPLITS:
        members = [s for s in sessions if s["split"] == name]
        splits[name] = {
            "sessions": [s["run_id"] for s in members], "rows": sum(s["rows"] for s in members),
            "flagged_rows": sum(s["flagged_rows"] for s in members),
            "first_received_at": members[0]["first_received_at"], "last_received_at": members[-1]["last_received_at"],
        }
    body = {
        "schema_version": MANIFEST_SCHEMA, "feature_set_version": FEATURE_SET_VERSION,
        "selection": {
            "unit": "whole session (one capture)", "order": "first received_at, then run_id",
            "target_fractions": dict(zip(SPLITS, fractions)), "min_rows": min_rows,
            "cut_rule": "cut points whose row shares are closest to the targets; every split non-empty",
            "duplicate_policy": "a txid already seen in an earlier session is dropped from later sessions when loading",
            "random_seed": None, "sampling_fraction": 1.0, "row_order": CANONICAL_ROW_ORDER,
        },
        "splits": splits, "sessions": sessions, "excluded": excluded,
        "limitations": [
            "Splits are chronological and by session; each split covers only a few short capture windows.",
            "Sessions have reconnect gaps; recorded gap seconds are a lower bound on missing coverage.",
            "Rows carry rule outputs but no independent labels; no accuracy claim can be made.",
        ],
    }
    return {"manifest_id": _content_id(body), **body}


def _content_id(body: dict) -> str:
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def verify_manifest(manifest: dict, root: Path) -> dict:
    """Check a recorded manifest is intact (its ID matches its content) and every file it names is unchanged.

    This is the reproducibility check that still works after new sessions arrive: rebuilding a manifest from the
    folder would include them and give a different (correct, but different) manifest.
    """
    body = {k: v for k, v in manifest.items() if k != "manifest_id"}
    changed = [s["run_id"] for s in manifest["sessions"]
               if not (root / s["screened_file"]).is_file() or sha256_file(root / s["screened_file"]) != s["screened_sha256"]]
    return {"manifest_id": manifest["manifest_id"], "content_matches_id": _content_id(body) == manifest["manifest_id"],
            "sessions": len(manifest["sessions"]), "changed_or_missing": changed,
            "ok": _content_id(body) == manifest["manifest_id"] and not changed}


def write_manifest(manifest: dict, path: Path) -> bool:
    """Write the manifest; True if created, False if an identical file already exists. Never overwrites."""
    text = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") == text:
            return False
        raise ValueError(f"{path} already exists with different content; choose a new name")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return True


SPLIT_SOURCES = ("auto", "jsonl", "lake")
LAKE_FEATURE_COLUMNS = ("n_in", "n_out", "value_out_sat", "fee_sat", "vsize", "fee_rate_sat_vb",
                        "max_equal_output_count", "round_amount_fraction", "rbf_signaled")


def load_split(manifest: dict, split: str, root: Path, *, source: str = "auto") -> list[dict]:
    """Read one split's rows and apply the duplicate policy (a txid seen in an earlier session is dropped).

    `root` is the screened folder given to build_manifest; the lake is looked for in its parent.

    source="jsonl" reads every session's screened.jsonl and checks it against the manifest's SHA-256.
    source="lake" reads the session's Parquet partitions: only the needed columns, and for sessions outside the
      split only the txid column. A session is used only if its quality report says every manifest row was written
      and none quarantined. That is a row-count check, not a checksum: use "jsonl" for a strict audit.
    source="auto" (default) uses the lake where that check passes and JSONL otherwise.

    Both sources return identical rows in a canonical order (manifest session order, then received_at, then txid),
    so anything order-sensitive, like training-set subsampling, gives the same result from either.
    """
    if split not in SPLITS:
        raise ValueError(f"split must be one of {SPLITS}")
    if source not in SPLIT_SOURCES:
        raise ValueError(f"source must be one of {SPLIT_SOURCES}")
    if manifest["selection"].get("row_order") != CANONICAL_ROW_ORDER:
        # Manifests built before 2026-10-19 used file order, which only the JSONL files preserve. Keep it, so
        # models and thresholds recorded from them still reproduce bit for bit.
        if source == "lake":
            raise ValueError("this manifest predates canonical row order; it can only be read from JSONL")
        return _load_split_file_order(manifest, split, root)
    lake_files = _lake_index(root.parent / "lake") if source != "jsonl" else {}
    rows, seen = [], set()
    for session in manifest["sessions"]:
        wanted = session["split"] == split
        files = lake_files.get(session["run_id"]) if _lake_matches(root.parent / "lake", session) else None
        if source == "lake" and files is None:
            raise ValueError(f"{session['run_id']} has no complete lake partition matching the manifest")
        if files is not None:
            session_rows = _read_lake(files, full=wanted)
        else:
            session_rows = _read_jsonl(root / session["screened_file"], session["screened_sha256"], full=wanted)
        if not wanted:  # only the txids matter here: they feed the duplicate policy
            seen.update(session_rows)
            continue
        for row in session_rows:
            if row["txid"] not in seen:
                seen.add(row["txid"])
                rows.append(row)
    return rows


def _load_split_file_order(manifest: dict, split: str, root: Path) -> list[dict]:
    rows, seen = [], set()
    for session in manifest["sessions"]:
        path = root / session["screened_file"]
        if sha256_file(path) != session["screened_sha256"]:
            raise ValueError(f"{path} has changed since the manifest was built")
        with path.open("rb") as lines:
            for raw in lines:
                row = json.loads(raw, parse_constant=reject_nonfinite)
                if row["txid"] in seen:
                    continue
                seen.add(row["txid"])
                if session["split"] == split:
                    rows.append(_slim(row))
    return rows


def _slim(row: dict) -> dict:
    """Keep only what training and evaluation read. A full row with its explanation strings is several times
    larger, and a split can hold millions of rows."""
    return {"txid": row["txid"], "features": row["features"], "is_flagged": row["is_flagged"],
            "signals": [{"rule_id": s["rule_id"]} for s in row["signals"]]}


def _read_jsonl(path: Path, expected_sha256: str, *, full: bool) -> list[dict]:
    if sha256_file(path) != expected_sha256:
        raise ValueError(f"{path} has changed since the manifest was built")
    keyed = []
    with path.open("rb") as lines:
        for raw in lines:
            row = json.loads(raw, parse_constant=reject_nonfinite)
            moment = datetime.fromisoformat(row["received_at"])
            keyed.append((moment, row["txid"], _slim(row) if full else row["txid"]))
    keyed.sort(key=lambda item: (item[0], item[1]))
    return [row for _, _, row in keyed]


def _lake_index(lake: Path) -> dict[str, list[Path]]:
    """Map session name to its transactions files, found once with a single directory walk."""
    index: dict[str, list[Path]] = {}
    directory = lake / "transactions"
    if directory.is_dir():
        for path in directory.glob("dt=*/hour=*/*.parquet"):
            index.setdefault(path.stem, []).append(path)
    return index


def _lake_matches(lake: Path, session: dict) -> bool:
    report = lake / "_quality" / f"{session['run_id']}.json"
    if not report.is_file():
        return False
    stats = json.loads(report.read_text(encoding="utf-8"))["tables"]["transactions"]
    return stats["rows_quarantined"] == 0 and stats["rows_written"] == stats["rows_in"] == session["rows"]


def _read_lake(files: list[Path], *, full: bool) -> list[dict]:
    import pyarrow as pa
    import pyarrow.parquet as pq
    columns = ["txid", "received_at"] + (list(LAKE_FEATURE_COLUMNS) + ["is_flagged", "rule_ids"] if full else [])
    table = pa.concat_tables([pq.read_table(path, columns=columns) for path in sorted(files)])
    table = table.sort_by([("received_at", "ascending"), ("txid", "ascending")])
    if not full:
        return table.column("txid").to_pylist()
    names = LAKE_FEATURE_COLUMNS
    features = zip(*(table.column(name).to_pylist() for name in names))
    return [{"txid": txid, "features": dict(zip(names, values)), "is_flagged": flagged,
             "signals": [{"rule_id": rule} for rule in rules]}
            for txid, values, flagged, rules in zip(table.column("txid").to_pylist(), features,
                                                    table.column("is_flagged").to_pylist(),
                                                    table.column("rule_ids").to_pylist())]
