from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import os, io, json, time, boto3
from confluent_kafka import Consumer

def consume_and_upload(**context):
    # Allow tuning via env var; default to 600s for fast backfill
    try:
        duration = int(os.getenv("INGEST_WINDOW_SECONDS", "600"))
    except Exception:
        duration = 600
    topic = os.getenv("KAFKA_TOPIC", "clickstream.events")
    BUCKET = os.environ["CLICKSTREAM_S3_BUCKET"]       # fail fast if missing
    AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")

    c = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        "group.id": "airflow-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000
    })
    c.subscribe([topic])

    # Bucketize lines by event hour to ensure correct S3 partitioning
    buckets = {}
    first_evt_ts = None
    start = time.time()
    while time.time() - start < duration:
        msg = c.poll(1.0)
        if msg and not msg.error():
            line = msg.value().decode("utf-8")
            try:
                evt = json.loads(line)
                ets = evt.get("event_ts")
                if ets:
                    ts_obj = datetime.fromisoformat(str(ets).replace("Z", "+00:00"))
                    day = ts_obj.strftime("%Y-%m-%d")
                    hour = ts_obj.strftime("%H")
                    buckets.setdefault((day, hour), []).append(line)
                    if first_evt_ts is None:
                        first_evt_ts = ts_obj
                else:
                    # Fallback bucket by current time if event_ts missing
                    now = datetime.now(timezone.utc)
                    buckets.setdefault((now.strftime("%Y-%m-%d"), now.strftime("%H")), []).append(line)
            except Exception:
                # Non-JSON or malformed lines go to a fallback bucket
                now = datetime.now(timezone.utc)
                buckets.setdefault((now.strftime("%Y-%m-%d"), now.strftime("%H")), []).append(line)
    # Ensure progress persists across runs
    try:
        c.commit(asynchronous=False)
    except Exception:
        pass
    c.close()

    # Nothing consumed
    total_lines = sum(len(v) for v in buckets.values())
    if total_lines == 0:
        print("No messages consumed in window; skipping S3 upload.")
        return "EMPTY_BATCH"

    # Upload one file per (date,hour) bucket with unique suffix
    s3 = boto3.client("s3", region_name=AWS_REGION)
    uploaded = 0
    batch_suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for (day, hour), lines in sorted(buckets.items()):
        body = ("\n".join(lines) + "\n").encode("utf-8")
        key = f"raw/clickstream/date={day}/hour={hour}/batch_{batch_suffix}.jsonl"
        print(f"Uploading {len(lines)} lines to s3://{BUCKET}/{key}")
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=body,
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )
        uploaded += 1
    print(f"Uploaded {uploaded} object(s) across {len(buckets)} bucket(s).")


with DAG(
    "ingest_clickstream_to_s3",
    default_args={"retries": 0},
    start_date=datetime(2025, 9, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    PythonOperator(task_id="consume_and_upload", python_callable=consume_and_upload)