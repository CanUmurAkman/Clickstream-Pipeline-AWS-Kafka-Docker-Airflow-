from __future__ import annotations

import io
import json
import os
from dataclasses import dataclass
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---- Configuration ----
DEFAULT_TZ = os.environ.get("CLICKSTREAM_TZ", "UTC")  # set to "Europe/Istanbul" if you prefer
AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")

# Prefer CLICKSTREAM_S3_BUCKET (used by ingest). Fall back to S3_BUCKET for backward-compat.
S3_BUCKET = os.getenv("CLICKSTREAM_S3_BUCKET")
if not S3_BUCKET:
    raise RuntimeError(
        "Missing env var CLICKSTREAM_S3_BUCKET (or S3_BUCKET). "
        "Set it in your docker-compose or Airflow env."
    )

def _parse_date_any(s: str) -> date:
    s = s.strip().strip('"').strip("'")
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date()
    except Exception as e:
        raise ValueError(f"Could not parse run_date '{s}': {e}")

def resolve_run_date(context: Optional[Dict[str, Any]] = None) -> date:
    # 1) dag_run.conf
    if context:
        dag_run = context.get("dag_run") or context.get("dag_run_obj")
        if dag_run and getattr(dag_run, "conf", None):
            rd = dag_run.conf.get("run_date")
            if rd:
                return _parse_date_any(str(rd))
        # 3) logical/execution date
        logical_date = context.get("logical_date") or context.get("execution_date")
        if logical_date:
            try:
                return logical_date.date()
            except Exception:
                pass
    # 2) env var
    env_rd = os.environ.get("RUN_DATE")
    if env_rd:
        return _parse_date_any(env_rd)
    # 4) fallback: local "today" in DEFAULT_TZ
    tz = ZoneInfo(DEFAULT_TZ)
    return datetime.now(tz).date()

def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)

def iter_keys(bucket: str, prefix: str) -> Iterable[str]:
    s3 = s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) if isinstance(page, dict) else []:
            yield obj["Key"]

def read_jsonl_objects(bucket: str, keys: Iterable[str]) -> Iterable[Dict[str, Any]]:
    s3 = s3_client()
    for key in keys:
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read().decode("utf-8", errors="ignore")
        for line in body.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

def compute_daily_kpis_for(events: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    # Minimal KPIs that work with your simulator; extend as you like.
    total_events = 0
    users, sessions = set(), set()
    pageviews = purchases = 0
    revenue = 0.0

    for e in events:
        total_events += 1
        uid = e.get("user_id");  sid = e.get("session_id")
        if uid: users.add(uid)
        if sid: sessions.add(sid)
        et = (e.get("event_type") or "").lower()
        if et in ("page_view", "pageview", "view"):
            pageviews += 1
        if et in ("purchase", "order", "checkout"):
            purchases += 1
            try:
                revenue += float(e.get("price", 0) or 0)
            except Exception:
                pass

    return {
        "total_events": total_events,
        "unique_users": len(users),
        "unique_sessions": len(sessions),
        "pageviews": pageviews,
        "purchases": purchases,
        "revenue_usd": round(revenue, 2),
    }

def compute_kpis(**kwargs):
    # Get Airflow context if available; fall back to kwargs
    try:
        from airflow.operators.python import get_current_context  # Airflow 2.x
        context = get_current_context()
    except Exception:
        context = kwargs

    run_dt = resolve_run_date(context)
    day = run_dt.strftime("%Y-%m-%d")
    print(f"[daily_kpis] Resolved run date → {day} (DEFAULT_TZ={DEFAULT_TZ})")

    prefix = f"raw/clickstream/date={day}/"
    keys = list(iter_keys(S3_BUCKET, prefix))
    if not keys:
        print(f"[daily_kpis] No raw files under s3://{S3_BUCKET}/{prefix}. Exiting gracefully.")
        return "NO_INPUT"

    events = read_jsonl_objects(S3_BUCKET, keys)
    kpis = compute_daily_kpis_for(events)
    kpis["dt"] = day

    df = pd.DataFrame([kpis])
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    out_key = f"kpis/daily/dt={day}/kpis.parquet"

    s3 = s3_client()
    s3.put_object(Bucket=S3_BUCKET, Key=out_key, Body=buf.getvalue())
    print(f"[daily_kpis] Wrote s3://{S3_BUCKET}/{out_key} → {len(df)} row(s).")
    return out_key

# ---- Airflow DAG ----
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import timedelta

    with DAG(
        dag_id="daily_kpis",
        default_args={"retries": 0},
        start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
        schedule_interval="@daily",
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(minutes=30),
        tags=["clickstream", "kpis"],
    ) as dag:
        PythonOperator(
            task_id="compute_kpis",
            python_callable=compute_kpis,
        )
except Exception:
    # allow importing this module outside Airflow
    pass
