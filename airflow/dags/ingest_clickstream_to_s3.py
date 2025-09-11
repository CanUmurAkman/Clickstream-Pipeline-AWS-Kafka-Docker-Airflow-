from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import os, io, json, time, boto3
from confluent_kafka import Consumer

def consume_and_upload(**context):
    duration = 30  # seconds per batch
    topic = os.getenv("KAFKA_TOPIC", "clickstream.events")
    BUCKET = os.environ["CLICKSTREAM_S3_BUCKET"]       # fail fast if missing
    AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")

    c = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        "group.id": "airflow-consumer",
        "auto.offset.reset": "earliest"
    })
    c.subscribe([topic])

    buf = io.StringIO()
    first_evt_ts = None
    start = time.time()
    while time.time() - start < duration:
        msg = c.poll(1.0)
        if msg and not msg.error():
            line = msg.value().decode("utf-8")
            buf.write(line + "\n")
            if first_evt_ts is None:
                try:
                    ets = json.loads(line).get("event_ts")
                    if ets:
                        first_evt_ts = datetime.fromisoformat(ets.replace("Z", "+00:00"))
                except Exception:
                    pass
    c.close()

    ts = first_evt_ts or datetime.now(timezone.utc)
    key = f"raw/clickstream/date={ts:%Y-%m-%d}/hour={ts:%H}/batch_{ts:%Y%m%dT%H%M%S}.jsonl"

    s3 = boto3.client("s3", region_name=AWS_REGION)
    print(f"Uploading to s3://{BUCKET}/{key}")
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",  # matches your bucket config
)


with DAG(
    "ingest_clickstream_to_s3",
    default_args={"retries": 0},
    start_date=datetime(2025, 9, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
) as dag:
    PythonOperator(task_id="consume_and_upload", python_callable=consume_and_upload)