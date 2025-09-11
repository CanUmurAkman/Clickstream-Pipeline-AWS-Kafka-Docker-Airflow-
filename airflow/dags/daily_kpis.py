from __future__ import annotations

import io
import json
import os
from datetime import datetime, date, timezone, timedelta
from typing import Any, Dict, Iterable, Optional
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---- Configuration ----
DEFAULT_TZ = os.environ.get("CLICKSTREAM_TZ", "UTC") or "UTC"  # coerce empty -> "UTC"
AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")

# Prefer CLICKSTREAM_S3_BUCKET (used by ingest). Fall back to S3_BUCKET for backward-compat.
S3_BUCKET = os.environ.get("CLICKSTREAM_S3_BUCKET") or os.environ.get("S3_BUCKET")
if not S3_BUCKET:
    raise RuntimeError(
        "Missing env var CLICKSTREAM_S3_BUCKET (or S3_BUCKET). "
        "Set it in your docker-compose or Airflow env."
    )

def _parse_date_any(s: Optional[str]) -> date:
    """Parse YYYY-MM-DD or ISO8601; returns a date. Defensive against None/empty."""
    if not s:
        raise ValueError("run_date was provided but empty/None.")
    s = str(s).strip().strip('"').strip("'")
    if not s:
        raise ValueError("run_date became empty after trimming.")
    # YYYY-MM-DD first
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        pass
    # ISO 8601
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date()
    except Exception as e:
        raise ValueError(f"Could not parse run_date '{s}': {e}")

def _resolve_run_date(context: Optional[Dict[str, Any]] = None) -> date:
    """
    Order:
      1) dag_run.conf['run_date']
      2) env RUN_DATE
      3) Airflow logical/execution date
      4) "today" in DEFAULT_TZ
    """
    # 1) dag_run.conf
    if context:
        dag_run = context.get("dag_run") or context.get("dag_run_obj")
        conf = getattr(dag_run, "conf", None) if dag_run else None
        if isinstance(conf, dict) and conf.get("run_date") is not None:
            return _parse_date_any(conf.get("run_date"))

    # 2) env RUN_DATE
    env_rd = os.environ.get("RUN_DATE")
    if env_rd is not None and str(env_rd).strip() != "":
        return _parse_date_any(env_rd)

    # 3) logical/execution date (Airflow)
    if context:
        logical_date = context.get("logical_date") or context.get("execution_date")
        if logical_date:
            try:
                return logical_date.date()
            except Exception:
                pass

    # 4) fallback: "today" in DEFAULT_TZ
    tz = ZoneInfo(DEFAULT_TZ)  # DEFAULT_TZ is never None/empty here
    return datetime.now(tz).date()

def _s3():
    return boto3.client("s3", region_name=AWS_REGION)

def _iter_keys(bucket: str, prefix: str):
    """Yield keys under prefix; safe on empty listings."""
    s3 = _s3()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=str(bucket), Prefix=str(prefix)):
        contents = page.get("Contents") or []
        for obj in contents:
            key = obj.get("Key")
            if key:
                yield key

def _read_jsonl_objects(bucket: str, keys):
    s3 = _s3()
    for key in keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        # body can be bytes; decode safely
        text = body.decode("utf-8", errors="ignore") if isinstance(body, (bytes, bytearray)) else str(body)
        for line in text.splitlines():
            line = (line or "").strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

def _compute_daily_kpis(events_iter) -> Dict[str, Any]:
    total_events = 0
    users, sessions = set(), set()
    pageviews = purchases = 0
    revenue = 0.0

    for e in events_iter:
        total_events += 1
        uid = e.get("user_id")
        sid = e.get("session_id")
        if uid is not None:
            users.add(uid)
        if sid is not None:
            sessions.add(sid)
        et = str(e.get("event_type") or "").lower()
        if et in ("page_view", "pageview", "view"):
            pageviews += 1
        if et in ("purchase", "order", "checkout"):
            purchases += 1
            try:
                revenue += float(e.get("price") or 0)
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
    # Try Airflow context (works in-task); fallback to kwargs for ad-hoc runs.
    try:
        from airflow.operators.python import get_current_context  # Airflow 2.x
        context = get_current_context()
    except Exception:
        context = kwargs or {}

    # Resolve day defensively
    run_dt = _resolve_run_date(context)
    day = run_dt.strftime("%Y-%m-%d")
    print(f"[daily_kpis] Resolved run date → {day} (DEFAULT_TZ={DEFAULT_TZ})")

    prefix = f"raw/clickstream/date={day}/"
    keys = list(_iter_keys(S3_BUCKET, prefix))
    if not keys:
        print(f"[daily_kpis] No raw files under s3://{S3_BUCKET}/{prefix}. Exiting gracefully.")
        return "NO_INPUT"

    events = _read_jsonl_objects(S3_BUCKET, keys)
    kpis = _compute_daily_kpis(events)
    kpis["dt"] = day

    df = pd.DataFrame([kpis])
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    out_key = f"kpis/daily/dt={day}/kpis.parquet"

    s3 = _s3()
    s3.put_object(Bucket=S3_BUCKET, Key=out_key, Body=buf.getvalue())
    print(f"[daily_kpis] Wrote s3://{S3_BUCKET}/{out_key} → {len(df)} row(s).")
    return out_key

# ---- Airflow DAG ----
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id="daily_kpis",
        default_args={"retries": 0},
        start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
        schedule_interval="@daily",
        catchup=True,
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