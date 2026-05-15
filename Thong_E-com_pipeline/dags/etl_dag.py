"""
etl_dag.py  — Cloud version
DAG chạy 1:00 AM mỗi ngày.

Flow mới:
  generate → staging (PG) → upload GCS → load BigQuery → dbt run → dbt test

Thêm so với bản gốc:
  - Task upload_to_gcs: export validated staging data lên GCS
  - Task load_to_bigquery: load Parquet từ GCS vào BigQuery raw tables
  - Task dbt_run: chạy tất cả dbt models (staging → warehouse → marts)
  - Task dbt_test: chạy dbt tests để validate data quality
"""
import sys
sys.path.insert(0, "/opt/airflow/scripts")

from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from generator import generate_daily_data
from staging import run_staging
from gcs_upload import upload_staging_to_gcs    # NEW
from bq_load import load_gcs_to_bq              # NEW

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="ecom_etl_pipeline_cloud",
    default_args=default_args,
    description="Daily E-Commerce ETL: PostgreSQL → GCS → BigQuery → dbt",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["ecom", "etl", "daily", "bigquery", "gcs", "dbt"],
) as dag:

    # ── LAYER 1: Generate raw data ──────────────────────────────────────────
    t1 = PythonOperator(
        task_id="generate_daily_data",
        python_callable=generate_daily_data,
        op_kwargs={"num_orders": 250},
        doc_md="Sinh ~250 đơn hàng giả lập vào PostgreSQL raw tables",
    )

    # ── LAYER 2: Staging (validate trong PostgreSQL) ────────────────────────
    t2 = PythonOperator(
        task_id="run_staging",
        python_callable=run_staging,
        op_kwargs={"run_date": date.today()},
        doc_md="Validate + clean raw data → staging layer với is_valid flag",
    )

    # ── LAYER 3a: Upload lên GCS ────────────────────────────────────────────
    t3 = PythonOperator(
        task_id="upload_to_gcs",                # NEW
        python_callable=upload_staging_to_gcs,
        op_kwargs={"run_date": date.today()},
        doc_md="Export validated staging data → GCS dưới dạng Parquet (partitioned by date)",
    )

    # ── LAYER 3b: Load vào BigQuery ─────────────────────────────────────────
    t4 = PythonOperator(
        task_id="load_to_bigquery",             # NEW
        python_callable=load_gcs_to_bq,
        op_kwargs={"run_date": date.today()},
        doc_md="Load Parquet từ GCS → BigQuery raw tables (partitioned, clustered)",
    )

    # ── LAYER 4: dbt transform ──────────────────────────────────────────────
    t5 = BashOperator(
        task_id="dbt_run",                      # NEW
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir . --project-dir . --target dev"
        ),
        doc_md="Chạy dbt models: staging views → fct_orders (incremental) → marts (table)",
    )

    # ── LAYER 5: dbt test ───────────────────────────────────────────────────
    t6 = BashOperator(
        task_id="dbt_test",                     # NEW
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --profiles-dir . --project-dir . --target dev"
        ),
        doc_md="Chạy dbt tests: not_null, unique, accepted_values trên tất cả models",
    )

    # ── Pipeline dependency ─────────────────────────────────────────────────
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
