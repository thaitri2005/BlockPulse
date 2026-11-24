"""Compact the streamed lake: merge each closed hour's many small Parquet files into one.

The stream sink writes one file per Kafka partition per micro-batch (every 5 seconds by default), so an hour of
traffic becomes hundreds of small files. Query engines pay per file opened: listing, footers, and in Athena a
minimum charge. Compaction rewrites a partition directory as one sorted file with the same rows.

What makes it safe to run at any time, and to run again:

* Only *closed* hours are compacted: every hour except the newest one in each table. The sink may still be writing
  the newest hour, and a replay after a crash rewrites files in it by name (`stream_lake._remove_replayed`), which a
  merged file would hide from it. `include_latest=True` is for a lake nobody is writing to any more.
* The merged file is written under a temporary name, then renamed, and only then are the inputs deleted. A crash in
  between leaves both; the next run merges them again and drops the exact duplicate rows, so the result converges.
* Rows are never changed: no validation or coercion happens here. The quality gate already ran when they were written.
"""

from pathlib import Path

from blockpulse.lake import TABLES, arrow_schema
from blockpulse.storage import utc_now

COMPACT_NAME = "compact-000.parquet"
STREAM_TABLES = ("transactions", "lifecycle_events")


def _partitions(table_dir: Path) -> list[Path]:
    """Hour directories that contain Parquet files, oldest first (the names sort chronologically)."""
    return sorted({path.parent for path in table_dir.glob("dt=*/hour=*/*.parquet")})


def compact_partition(directory: Path, table) -> dict:
    """Merge every Parquet file in one partition directory into COMPACT_NAME. Returns what it did."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    files = sorted(directory.glob("*.parquet"))
    if len(files) <= 1 and (not files or files[0].name == COMPACT_NAME):
        return {"partition": directory.as_posix(), "files_before": len(files), "rows": None, "compacted": False}
    schema = arrow_schema(table)
    merged = pa.concat_tables([pq.read_table(path, schema=schema) for path in files])
    rows_in = merged.num_rows
    # Exact duplicates only appear when an earlier compaction crashed between renaming its output and deleting its
    # inputs. Group on every column; rule_ids is a list, which can't be grouped, so it is compared as text.
    seen, keep = set(), []
    for index, row in enumerate(merged.to_pylist()):
        identity = tuple(str(row[name]) for name in schema.names)
        if identity not in seen:
            seen.add(identity)
            keep.append(index)
    if len(keep) != rows_in:
        merged = merged.take(pa.array(keep))
    merged = merged.sort_by([(table.time_column, "ascending"), (table.key, "ascending")])
    target = directory / COMPACT_NAME
    temporary = directory / (COMPACT_NAME + ".tmp")
    pq.write_table(merged, temporary, compression="zstd")
    temporary.replace(target)
    for path in files:
        if path != target:
            path.unlink()
    return {"partition": directory.as_posix(), "files_before": len(files), "rows": merged.num_rows,
            "duplicate_rows_dropped": rows_in - merged.num_rows, "compacted": True}


def compact_lake(lake: Path, *, include_latest: bool = False) -> dict:
    """Compact every closed hour of the streamed tables under `lake`. Safe to repeat; a second run does nothing."""
    lake = Path(lake)
    if not lake.is_dir():
        raise ValueError(f"Lake folder does not exist: {lake}")
    summary = {"lake": str(lake), "compacted_at": utc_now(), "include_latest": include_latest, "tables": {}}
    for name in STREAM_TABLES:
        partitions = _partitions(lake / name)
        closed = partitions if include_latest else partitions[:-1]
        results = [compact_partition(directory, TABLES[name]) for directory in closed]
        done = [r for r in results if r["compacted"]]
        summary["tables"][name] = {
            "partitions": len(partitions), "partitions_compacted": len(done),
            "partitions_skipped_open": len(partitions) - len(closed),
            "files_before": sum(r["files_before"] for r in done), "files_after": len(done),
            "rows": sum(r["rows"] for r in done), "duplicate_rows_dropped": sum(r["duplicate_rows_dropped"] for r in done),
        }
    return summary
