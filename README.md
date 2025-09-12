# AWS Clickstream Pipeline Starter

Docker-based demo of a streaming analytics pipeline on AWS.

## Overview

Docker Compose brings up Kafka, ZooKeeper, Kafka UI, Airflow and a Postgres
metadata database. A Python producer sends simulated user events to Kafka, and
Airflow DAGs move the data to Amazon S3 and compute daily KPIs.

* **Event producer** – `kafka/producer/produce_events.py` generates synthetic
  clickstream events.
* **Ingestion DAG** – `airflow/dags/ingest_clickstream_to_s3.py` consumes from
  Kafka and writes JSONL files to
  `s3://clickstream-522814689373-dev-euc1/raw/clickstream/`.
* **KPI DAG** – `airflow/dags/daily_kpis.py` reads the raw data and writes
  aggregated Parquet files back to S3.
* **Healthcheck** – `airflow/dags/ingest_healthcheck.py` checks that new raw
  objects arrive regularly.

See [docs/architecture.md](docs/architecture.md) for a component breakdown.

## Usage

```bash
./scripts/up.sh       # start services
# Airflow: http://localhost:8080, Kafka UI: http://localhost:8081
./scripts/down.sh     # stop and remove services
```
