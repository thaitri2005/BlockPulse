#!/bin/sh
# The blockpulse CLI inside the Airflow image. It runs from its own virtual environment, so BlockPulse's pinned
# dependencies never mix with Airflow's; the source is mounted read-only at /opt/blockpulse/src.
PYTHONPATH=/opt/blockpulse/src exec /opt/blockpulse/venv/bin/python -m blockpulse "$@"
