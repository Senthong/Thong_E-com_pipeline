"""
gcs_upload.py  ← NEW
Export validated staging data từ PostgreSQL lên GCS dưới dạng Parquet.
GCS đóng vai trò raw/landing zone trước khi load vào BigQuery.

Flow: PostgreSQL (stg_*) → pandas DataFrame → Parquet → GCS bucket
"""
import os
import io
from datetime import date

import pandas as pd
from google.cloud import storage

from db import get_pg_conn


GCS_BUCKET = os.getenv("GCS_BUCKET", "ecom-pipeline-raw")
GCS_PREFIX = os.getenv("GCS_PREFIX", "staging")   # gs://bucket/staging/...


def _get_gcs_client() -> storage.Client:
    return storage.Client(project=os.getenv("GCP_PROJECT_ID"))


def _upload_df_to_gcs(df: pd.DataFrame, bucket_name: str, blob_path: str) -> str:
    """Upload DataFrame dưới dạng Parquet lên GCS. Trả về gs:// URI."""
    client = _get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type="application/octet-stream")
    uri = f"gs://{bucket_name}/{blob_path}"
    print(f"  [gcs] Uploaded {len(df)} rows → {uri}")
    return uri


def upload_staging_to_gcs(run_date: date = None) -> dict:
    """
    Export 3 staging tables (valid rows only) lên GCS.
    Partition theo date: staging/orders/date=2025-01-01/data.parquet
    Returns dict với GCS URIs.
    """
    run_date = run_date or date.today()
    date_str = run_date.strftime("%Y-%m-%d")
    print(f"[gcs_upload] Exporting staging data for {date_str} → GCS")

    conn = get_pg_conn()
    uris = {}

    tables = {
        "orders": f"""
            SELECT order_id, customer_id, product_id, quantity,
                   unit_price, total_amount, status, order_date
            FROM stg_orders
            WHERE order_date = '{run_date}' AND is_valid = TRUE
        """,
        "customers": f"""
            SELECT customer_id, full_name, email, city, age, age_group
            FROM stg_customers
            WHERE is_valid = TRUE
        """,
        "products": f"""
            SELECT product_id, name, category, cost_price, sell_price, margin_pct
            FROM stg_products
            WHERE is_valid = TRUE
        """,
    }

    for table_name, sql in tables.items():
        df = pd.read_sql(sql, conn)
        if df.empty:
            print(f"  [gcs] No valid rows for {table_name}, skipping")
            continue

        # Partition path: staging/orders/date=2025-01-01/data.parquet
        blob_path = f"{GCS_PREFIX}/{table_name}/date={date_str}/data.parquet"
        uri = _upload_df_to_gcs(df, GCS_BUCKET, blob_path)
        uris[table_name] = uri

    conn.close()
    print(f"[gcs_upload] Done — {len(uris)} tables uploaded")
    return uris


if __name__ == "__main__":
    upload_staging_to_gcs()
