"""
db.py
Connection helpers cho PostgreSQL (local staging) và BigQuery.
"""
import os
import psycopg2
from google.cloud import bigquery


def get_pg_conn():
    """Trả về psycopg2 connection tới PostgreSQL."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "ecom_db"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
        port=os.getenv("DB_PORT", 5432),
    )


def get_bq_client() -> bigquery.Client:
    """Trả về BigQuery client (dùng ADC hoặc service account qua GOOGLE_APPLICATION_CREDENTIALS)."""
    project = os.getenv("GCP_PROJECT_ID")
    return bigquery.Client(project=project)


def get_bq_dataset() -> str:
    """Trả về dataset ID trong BigQuery."""
    return os.getenv("BQ_DATASET", "ecom_warehouse")
