from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, json, io, pandas as pd, pyarrow as pa, pyarrow.parquet as pq, boto3

def compute_kpis(execution_date=None, **_):
    s3_bucket = os.getenv("S3_BUCKET")
    region = os.getenv("AWS_REGION", "eu-central-1")
    day = (execution_date or datetime.utcnow()).strftime("%Y-%m-%d")

    s3 = boto3.client("s3", region_name=region)
    prefix = f"raw/clickstream/date={day}/"
    resp = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)

    rows = []
    for obj in resp.get("Contents", []):
        body = s3.get_object(Bucket=s3_bucket, Key=obj["Key"])["Body"].read().decode()
        rows.extend(json.loads(line) for line in body.splitlines() if line.strip())
    if not rows: return

    df = pd.DataFrame(rows)
    kpis = {
        "date": day,
        "events": len(df),
        "users": df["user_id"].nunique(),
        "page_views": (df["event_type"]=="page_view").sum(),
        "add_to_cart": (df["event_type"]=="add_to_cart").sum(),
        "checkout": (df["event_type"]=="checkout").sum(),
        "purchases": (df["event_type"]=="purchase").sum(),
        "revenue": df.loc[df["event_type"]=="purchase","price"].fillna(0).sum()
    }
    kpis["ctr_add_to_cart"] = kpis["add_to_cart"]/max(1,kpis["page_views"])
    kpis["checkout_rate"] = kpis["checkout"]/max(1,kpis["add_to_cart"])
    kpis["purchase_conv"] = kpis["purchases"]/max(1,kpis["checkout"])
    kpis["aov"] = kpis["revenue"]/max(1,kpis["purchases"])

    out = pd.DataFrame([kpis])
    table = pa.Table.from_pandas(out)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    key = f"kpis/daily/dt={day}/kpis.parquet"
    s3.put_object(Bucket=s3_bucket, Key=key, Body=buf.getvalue())

with DAG(
    "daily_kpis",
    default_args={"retries": 0},
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    PythonOperator(task_id="compute_kpis", python_callable=compute_kpis)
