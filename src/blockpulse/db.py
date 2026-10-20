"""Postgres connection and forward-only SQL migrations."""

import hashlib
import os
from pathlib import Path

DEFAULT_DSN = "postgresql://blockpulse:blockpulse_dev@127.0.0.1:5432/blockpulse"
MIGRATIONS_DIR = Path(__file__).parent / "migrations"
MIGRATION_LOCK_ID = 7_302_001  # arbitrary constant: stops two db-init runs from racing


def get_dsn() -> str:
    return os.environ.get("BLOCKPULSE_DSN", DEFAULT_DSN)


def connect(dsn: str | None = None):
    try:
        import psycopg
    except ImportError as error:
        raise RuntimeError('psycopg is not installed; run: pip install -e ".[m2]"') from error
    return psycopg.connect(dsn or get_dsn(), connect_timeout=5)


def create_database(dsn: str | None = None) -> bool:
    """Create the DSN's database if it is missing, by connecting to the server's `postgres` database.

    Returns True if it was created. Lets a fresh environment (an orchestrated run, CI) start from an empty server.
    """
    import psycopg
    from psycopg import sql
    info = psycopg.conninfo.conninfo_to_dict(dsn or get_dsn())
    name = info.get("dbname")
    if not name or name == "postgres":
        raise ValueError("the DSN must name an application database, not the server's postgres database")
    with psycopg.connect(psycopg.conninfo.make_conninfo(**{**info, "dbname": "postgres"}), connect_timeout=5,
                         autocommit=True) as connection:  # CREATE DATABASE can't run inside a transaction
        if connection.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
            return False
        try:
            connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        except (psycopg.errors.DuplicateDatabase, psycopg.errors.UniqueViolation):
            return False  # another process created it between the check and the CREATE (found by two DAG runs)
        return True


def list_migrations(directory: Path = MIGRATIONS_DIR) -> list[tuple[str, str, str]]:
    """Return (version, sql, sha256) in filename order, e.g. ('001_initial', ...)."""
    found = []
    for path in sorted(directory.glob("*.sql")):
        sql = path.read_text(encoding="utf-8")
        found.append((path.stem, sql, hashlib.sha256(sql.encode("utf-8")).hexdigest()))
    return found


def apply_migrations(connection, directory: Path = MIGRATIONS_DIR) -> dict:
    """Apply any migration not yet recorded. Each runs in its own transaction.

    A recorded migration whose file has since changed is an error: edit history, and
    databases that already ran it silently disagree with new ones. Add a new file instead.
    """
    applied_now, already = [], []
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,))
        connection.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "version text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        recorded = dict(connection.execute("SELECT version, sha256 FROM schema_migrations").fetchall())
        for version, sql, digest in list_migrations(directory):
            if version in recorded:
                if recorded[version] != digest:
                    raise RuntimeError(f"Migration {version} was already applied but its file has changed")
                already.append(version)
                continue
            connection.execute(sql)
            connection.execute("INSERT INTO schema_migrations (version, sha256) VALUES (%s, %s)", (version, digest))
            applied_now.append(version)
    return {"applied": applied_now, "already_applied": already}
