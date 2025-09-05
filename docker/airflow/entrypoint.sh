#!/usr/bin/env bash
set -euo pipefail

# Optional: show important envs for debugging
echo "[entrypoint] Using DB: ${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-<not set>}"

# Wait for Postgres TCP port (airflow-db:5432) to be reachable
echo "[entrypoint] Waiting for airflow-db:5432..."
for i in {1..60}; do
  if (echo > /dev/tcp/airflow-db/5432) >/dev/null 2>&1; then
    echo "[entrypoint] Postgres is reachable."
    break
  fi
  sleep 2
done

# Initialize DB (idempotent) + create admin user (ignore if exists)
echo "[entrypoint] Initializing Airflow DB..."
airflow db init

echo "[entrypoint] Ensuring admin user exists..."
airflow users create \
  --username admin \
  --password admin \
  --firstname a \
  --lastname b \
  --role Admin \
  --email admin@example.com || true

echo "[entrypoint] Starting webserver and scheduler..."
airflow webserver & airflow scheduler

# Keep the container alive on the first process exit
wait -n