# Architecture

1. `kafka/producer/produce_events.py` publishes mock clickstream events to the
   Kafka topic `clickstream.events`.
2. The `ingest_clickstream_to_s3` Airflow DAG consumes events, buckets them by
   day/hour, and uploads JSONL batches to
   `s3://clickstream-522814689373-dev-euc1/raw/clickstream/date=YYYY-MM-DD/hour=HH/`.
3. The `daily_kpis` DAG reads those raw files, computes metrics such as total
   events, unique users, and revenue, and writes the results as Parquet files to
   `s3://clickstream-522814689373-dev-euc1/kpis/daily/dt=YYYY-MM-DD/`.
4. `ingest_healthcheck` periodically verifies that new raw files have arrived in
   the last few minutes.

All services—Kafka, ZooKeeper, Kafka UI, Postgres, Airflow webserver and
scheduler, and the event producer—run locally with Docker Compose.
