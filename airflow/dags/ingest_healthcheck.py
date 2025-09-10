from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

BUCKET = os.environ.get("CLICKSTREAM_S3_BUCKET") or os.environ.get("S3_BUCKET")
REGION = os.environ.get("AWS_REGION", "eu-central-1")
LOOKBACK_MIN = int(os.environ.get("INGEST_HEALTH_LOOKBACK_MIN", "20"))  # tweak as you like

def check_recent_raw(**_):
    if not BUCKET:
        raise AirflowFailException("Set CLICKSTREAM_S3_BUCKET or S3_BUCKET in env.")

    now = datetime.now(timezone.utc)
    since = now - timedelta(minutes=LOOKBACK_MIN)
    day = now.strftime("%Y-%m-%d")
    prefix = f"raw/clickstream/date={day}/"

    s3 = boto3.client("s3", region_name=REGION)
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []) if isinstance(page, dict) else []:
            # S3 LastModified is tz-aware UTC
            if obj["LastModified"] >= since:
                count += 1
                if count >= 1:
                    return "OK"

    raise AirflowFailException(f"No new raw files in last {LOOKBACK_MIN} minutes under s3://{BUCKET}/{prefix}")

with DAG(
    dag_id="ingest_healthcheck",
    start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
    schedule_interval="*/10 * * * *",  # every 10 minutes
    catchup=False,
    max_active_runs=1,
    tags=["clickstream","health"],
) as dag:
    PythonOperator(task_id="check_recent_raw", python_callable=check_recent_raw)
