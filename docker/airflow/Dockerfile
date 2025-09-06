# Match the Python version from your logs (3.12)
FROM apache/airflow:2.9.2-python3.12

# If you ever need OS deps, do them as root here, then drop back to airflow.
USER airflow

# Use Airflow constraints to keep versions compatible with the base image
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Copy the entrypoint script (needs root to chmod)
USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Drop back to airflow user for runtime (compose uses /entrypoint.sh via command)
USER airflow