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


@needs_postgres
def test_create_database_only_when_missing():
    import psycopg
    assert db.create_database(PG_DSN) is False  # the test database exists
    info = psycopg.conninfo.conninfo_to_dict(PG_DSN)
    scratch = psycopg.conninfo.make_conninfo(**{**info, "dbname": info["dbname"] + "_created"})
    admin = psycopg.conninfo.make_conninfo(**{**info, "dbname": "postgres"})
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(f'DROP DATABASE IF EXISTS "{info["dbname"]}_created"')
    try:
        assert db.create_database(scratch) is True and db.create_database(scratch) is False
    finally:
        with psycopg.connect(admin, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE IF EXISTS "{info["dbname"]}_created"')
    with pytest.raises(ValueError):
        db.create_database(admin)


@needs_postgres
def test_create_database_tolerates_a_concurrent_create():
    """Two DAG runs created the same database at once; the loser must see "already there", not an error."""
    import threading

    import psycopg
    info = psycopg.conninfo.conninfo_to_dict(PG_DSN)
    name = info["dbname"] + "_race"
    scratch = psycopg.conninfo.make_conninfo(**{**info, "dbname": name})
    admin = psycopg.conninfo.make_conninfo(**{**info, "dbname": "postgres"})
    results, errors, start = [], [], threading.Barrier(4)

    def create():
        start.wait()
        try:
            results.append(db.create_database(scratch))
        except Exception as error:  # noqa: BLE001 - collected for the assertion
            errors.append(error)
    try:
        for _ in range(5):
            with psycopg.connect(admin, autocommit=True) as connection:
                connection.execute(f'DROP DATABASE IF EXISTS "{name}"')
            results.clear()
            threads = [threading.Thread(target=create) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            assert errors == [] and results.count(True) == 1, (errors, results)
    finally:
        with psycopg.connect(admin, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE IF EXISTS "{name}"')
