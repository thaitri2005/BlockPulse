# Airflow plus the two libraries the BlockPulse CLI needs. The project code itself is mounted, not copied,
# so edits to src/ take effect without rebuilding.
FROM apache/airflow:2.10.5-python3.12
RUN pip install --no-cache-dir "websockets==15.0.1" "psycopg[binary]==3.3.4"
