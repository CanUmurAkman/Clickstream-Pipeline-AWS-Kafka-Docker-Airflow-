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