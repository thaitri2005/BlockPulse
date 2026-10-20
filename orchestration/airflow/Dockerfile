# Airflow 3 (scheduler, API server, DAG processor, triggerer in one `standalone` container) plus BlockPulse.
#
# The orchestrator and the workload are kept apart: BlockPulse is installed from the hashed lock into its own
# virtual environment, and DAG tasks call the `blockpulse` CLI. Airflow's own dependencies are untouched.
# Build from the repository root (compose.yaml does this); .dockerignore keeps data/ out of the build context.
FROM apache/airflow:slim-3.3.2-python3.11

USER root
COPY orchestration/airflow/blockpulse.sh /usr/local/bin/blockpulse
RUN chmod 755 /usr/local/bin/blockpulse  && install -d -o airflow -g 0 /opt/blockpulse /opt/airflow/state

USER airflow
COPY requirements.lock /opt/blockpulse/requirements.lock
# The Airflow image sets PIP_USER=true, which a virtual environment doesn't allow.
RUN python -m venv /opt/blockpulse/venv  && PIP_USER=false /opt/blockpulse/venv/bin/pip install --no-cache-dir --require-hashes -r /opt/blockpulse/requirements.lock
