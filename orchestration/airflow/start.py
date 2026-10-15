"""Container start-up for a single-container Airflow: create its database, migrate, add a user, run scheduler + web UI.

This is a laptop setup. The scheduler runs as a child process of the web server's container; if either dies
the container exits and Compose shows it. Not a production layout.
"""

import os
import subprocess
import sys
import time

import psycopg2
from psycopg2 import sql


def ensure_database():
    """Airflow keeps its own metadata in a database called 'airflow' inside the existing Postgres."""
    admin = psycopg2.connect(host="postgres", user="blockpulse", password=os.environ["POSTGRES_PASSWORD"],
                             dbname="blockpulse", connect_timeout=10)
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
        if cursor.fetchone() is None:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier("airflow")))
    admin.close()


def main():
    ensure_database()
    subprocess.run(["airflow", "db", "migrate"], check=True)
    subprocess.run(["airflow", "users", "create", "--username", "admin", "--password", os.environ["AIRFLOW_ADMIN_PASSWORD"],
                    "--firstname", "Blockpulse", "--lastname", "Admin", "--role", "Admin", "--email", "admin@example.invalid"],
                   check=False)  # fails harmlessly when the user already exists
    scheduler = subprocess.Popen(["airflow", "scheduler"])
    webserver = subprocess.Popen(["airflow", "webserver", "--port", "8080"])
    while scheduler.poll() is None and webserver.poll() is None:
        time.sleep(5)
    for process in (scheduler, webserver):
        if process.poll() is None:
            process.terminate()
    sys.exit(1)


if __name__ == "__main__":
    main()
