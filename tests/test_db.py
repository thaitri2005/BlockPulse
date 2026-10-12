import os

import pytest

from blockpulse import db

PG_DSN = os.environ.get("BLOCKPULSE_TEST_DSN")
needs_postgres = pytest.mark.skipif(not PG_DSN, reason="Set BLOCKPULSE_TEST_DSN to a throwaway Postgres database")


def test_migrations_are_ordered_and_hashed():
    migrations = db.list_migrations()
    versions = [version for version, _, _ in migrations]
    assert versions == sorted(versions)
    assert versions[0] == "001_initial"
    assert all(len(digest) == 64 for _, _, digest in migrations)


@pytest.fixture
def clean_connection():
    connection = db.connect(PG_DSN)
    connection.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    connection.commit()
    yield connection
    connection.close()


@needs_postgres
def test_apply_is_idempotent(clean_connection):
    first = db.apply_migrations(clean_connection)
    expected = [version for version, _, _ in db.list_migrations()]
    assert first["applied"] == expected
    second = db.apply_migrations(clean_connection)
    assert second == {"applied": [], "already_applied": expected}


@needs_postgres
def test_changed_applied_migration_is_rejected(clean_connection, tmp_path):
    (tmp_path / "001_a.sql").write_text("CREATE TABLE a (x int);", encoding="utf-8")
    db.apply_migrations(clean_connection, tmp_path)
    (tmp_path / "001_a.sql").write_text("CREATE TABLE a (x int, y int);", encoding="utf-8")
    with pytest.raises(RuntimeError, match="file has changed"):
        db.apply_migrations(clean_connection, tmp_path)


@needs_postgres
def test_scores_unique_key_rejects_duplicates(clean_connection):
    import psycopg
    db.apply_migrations(clean_connection)
    insert = (
        "INSERT INTO scores (run_id, txid, feature_set_version, ruleset_version, features, signals,"
        " signal_count, is_flagged, source_event_id, received_at)"
        " VALUES ('r', 't', 'f', 'v', '{}', '[]', 0, false, 'e', now())"
    )
    clean_connection.execute(insert)
    with pytest.raises(psycopg.errors.UniqueViolation):
        clean_connection.execute(insert)
