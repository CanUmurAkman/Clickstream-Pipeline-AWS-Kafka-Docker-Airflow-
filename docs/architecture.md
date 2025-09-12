# Architecture

1. `kafka/producer/produce_events.py` publishes mock clickstream events to the
   Kafka topic `clickstream.events`.
2. The `ingest_clickstream_to_s3` Airflow DAG consumes events, buckets them by
   day/hour, and uploads JSONL batches to
   `eu-central-1.console.aws.amazon.com/s3/buckets/clickstream-522814689373-dev-euc1`.
3. The `daily_kpis` DAG reads those raw files, computes metrics such as total
   events, unique users, and revenue, and writes the results as Parquet files to
   `eu-central-1.console.aws.amazon.com/s3/buckets/clickstream-522814689373-dev-euc1`.
4. `ingest_healthcheck` periodically verifies that new raw files have arrived in
   the last few minutes.

All services—Kafka, ZooKeeper, Kafka UI, Postgres, Airflow webserver and
scheduler, and the event producer—run locally with Docker Compose.
