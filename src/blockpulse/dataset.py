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
            "random_seed": None, "sampling_fraction": 1.0,
        },
        "splits": splits, "sessions": sessions, "excluded": excluded,
        "limitations": [
            "Splits are chronological and by session; each split covers only a few short capture windows.",
            "Sessions have reconnect gaps; recorded gap seconds are a lower bound on missing coverage.",
            "Rows carry rule outputs but no independent labels; no accuracy claim can be made.",
        ],
    }
    manifest_id = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return {"manifest_id": manifest_id, **body}


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


def load_split(manifest: dict, split: str, root: Path) -> list[dict]:
    """Read one split's rows, verifying each file's checksum and applying the duplicate policy.

    `root` is the directory holding the session folders (the same one given to build_manifest).
    """
    if split not in SPLITS:
        raise ValueError(f"split must be one of {SPLITS}")
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
                    rows.append(row)
    return rows
