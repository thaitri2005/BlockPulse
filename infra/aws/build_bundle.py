"""Build the app bundle the AWS instance installs: infra/aws/build/blockpulse-app.zip.

The zip holds only what the instance needs (the Python package, its build metadata, and the instance scripts and
systemd units). It is reproducible: same files in, byte-identical zip out, so Terraform re-uploads it only when
something actually changed. Text files are written with Unix line endings regardless of how Git checked them
out on Windows, because a shell script with CRLF line endings fails on Linux.

Usage: python infra/aws/build_bundle.py [--output PATH]
"""

import argparse
import hashlib
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "infra" / "aws" / "build" / "blockpulse-app.zip"
INCLUDE = [
    ("pyproject.toml", None),
    ("README.md", None),
    ("src/blockpulse", "**/*"),
    ("infra/aws/instance", "**/*"),
]
TEXT_SUFFIXES = {".py", ".sql", ".toml", ".md", ".sh", ".service", ".timer", ".txt", ".json", ".cfg"}
SKIP_PARTS = {"__pycache__", ".pytest_cache"}
SKIP_SUFFIXES = {".pyc", ".pyo"}
FIXED_TIME = (2020, 1, 1, 0, 0, 0)


def collect_files(root: Path = ROOT) -> list[tuple[str, Path]]:
    files = []
    for base, pattern in INCLUDE:
        path = root / base
        candidates = [path] if pattern is None else sorted(p for p in path.glob(pattern) if p.is_file())
        for candidate in candidates:
            relative = candidate.relative_to(root)
            if SKIP_PARTS & set(relative.parts) or candidate.suffix in SKIP_SUFFIXES or ".egg-info" in str(relative):
                continue
            files.append((relative.as_posix(), candidate))
    if not files:
        raise SystemExit("nothing to bundle")
    return sorted(files)


def constraints_from_lock(lock_text: str) -> bytes:
    """requirements.lock without hashes, as a pip constraints file (pip rejects hashes in constraints).

    The instance installs with `-c constraints.txt`, so it gets exactly the locked versions.
    """
    lines = []
    for line in lock_text.splitlines():
        if not line or line[0] in " #-":
            continue  # hash continuation lines, comments, options
        lines.append(line.rstrip(" \\").split("  #")[0].rstrip())
    header = "# Generated from requirements.lock by build_bundle.py"
    return "\n".join([header, *lines, ""]).encode("utf-8")


def build(output: Path = DEFAULT_OUTPUT, root: Path = ROOT) -> dict:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".zip.tmp")
    entries = collect_files(root)
    with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as bundle:
        for name, path in entries:
            data = path.read_bytes()
            if path.suffix in TEXT_SUFFIXES:
                data = data.replace(b"\r\n", b"\n")
            info = zipfile.ZipInfo(name, date_time=FIXED_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (0o755 if path.suffix == ".sh" else 0o644) << 16
            bundle.writestr(info, data)
        lock = root / "requirements.lock"
        if lock.is_file():
            info = zipfile.ZipInfo("constraints.txt", date_time=FIXED_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            bundle.writestr(info, constraints_from_lock(lock.read_text(encoding="utf-8")))
    temporary.replace(output)
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    return {"bundle": str(output), "files": len(entries), "bytes": output.stat().st_size, "sha256": digest}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    result = build(args.output)
    print(f"{result['bundle']}: {result['files']} files, {result['bytes']:,} bytes, sha256 {result['sha256']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
