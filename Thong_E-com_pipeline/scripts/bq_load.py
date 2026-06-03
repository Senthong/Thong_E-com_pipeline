import os
from datetime import date

from google.cloud import bigquery

from db import get_bq_client, get_bq_dataset


GCS_BUCKET = os.getenv("GCS_BUCKET", "ecom-pipeline-raw")
GCS_PREFIX = os.getenv("GCS_PREFIX", "staging")


SCHEMAS = {
    "orders": [
        bigquery.SchemaField("order_id",     "STRING",  mode="REQUIRED"),
        bigquery.SchemaField("customer_id",  "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("product_id",   "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("quantity",     "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("unit_price",   "FLOAT",   mode="NULLABLE"),
        bigquery.SchemaField("total_amount", "FLOAT",   mode="NULLABLE"),
        bigquery.SchemaField("status",       "STRING",  mode="NULLABLE"),
        bigquery.SchemaField("order_date",   "DATE",    mode="NULLABLE"),
    ],
    "customers": [
        bigquery.SchemaField("customer_id",  "STRING", mode="REQUIRED"),
        bigquery.SchemaField("full_name",    "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email",        "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("age",          "INTEGER",mode="NULLABLE"),
        bigquery.SchemaField("age_group",    "STRING", mode="NULLABLE"),
    ],
    "products": [
        bigquery.SchemaField("product_id",   "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name",         "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category",     "STRING", mode="NULLABLE"),
        bigquery.SchemaField("cost_price",   "FLOAT",  mode="NULLABLE"),
        bigquery.SchemaField("sell_price",   "FLOAT",  mode="NULLABLE"),
        bigquery.SchemaField("margin_pct",   "FLOAT",  mode="NULLABLE"),
    ],
}


def _ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    """Tạo dataset nếu chưa tồn tại."""
    dataset_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset_ref.location = os.getenv("BQ_LOCATION", "US")
    client.create_dataset(dataset_ref, exists_ok=True)
    print(f"  [bq] Dataset `{dataset_id}` ready")


def load_gcs_to_bq(run_date: date = None) -> None:
    """
    Load Parquet files từ GCS vào BigQuery.
    Mỗi table được load vào partition theo date (WRITE_TRUNCATE cho partition đó).
    """
    run_date = run_date or date.today()
    date_str = run_date.strftime("%Y-%m-%d")
    print(f"[bq_load] Loading GCS data for {date_str} → BigQuery")

    client = get_bq_client()
    dataset_id = get_bq_dataset()
    _ensure_dataset(client, dataset_id)

    for table_name, schema in SCHEMAS.items():
        gcs_uri = f"gs://{GCS_BUCKET}/{GCS_PREFIX}/{table_name}/date={date_str}/data.parquet"
        table_ref = f"{client.project}.{dataset_id}.raw_{table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            if table_name in ("customers", "products")
            else bigquery.WriteDisposition.WRITE_APPEND,
            # Partition orders theo order_date để query rẻ hơn
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="order_date",
            ) if table_name == "orders" else None,
        )

        try:
            load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
            load_job.result()  # Wait for job to complete
            table = client.get_table(table_ref)
            print(f"  [bq] Loaded {table_name} → `{table_ref}` ({table.num_rows} total rows)")
        except Exception as e:
            print(f"  [bq] WARNING: Could not load {table_name}: {e}")
            # Không raise — nếu không có file (empty day) thì skip gracefully


if __name__ == "__main__":
    load_gcs_to_bq()
