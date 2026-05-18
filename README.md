# E-Commerce Data Pipeline — Cloud Version

Pipeline ETL daily cho e-commerce

## Architecture

```
Faker (Python)
     │
     ▼
PostgreSQL (raw_*)          ← Layer 1: Ingestion
     │
     ▼
PostgreSQL (stg_*)          ← Layer 2: Validate + Clean (is_valid flag)
     │
     ▼
GCS Bucket (Parquet)        ← Layer 3: Landing Zone (partitioned by date)
gs://ecom-pipeline-raw/staging/{table}/date=YYYY-MM-DD/data.parquet
     │
     ▼
BigQuery (raw_*)            ← Layer 4: Raw tables (partitioned + clustered)
     │
     ▼ dbt
BigQuery (stg_*)            ← Views: rename + cast
BigQuery (fct_orders)       ← Incremental table: star schema + profit calc
BigQuery (mart_*)           ← Tables: daily revenue, product perf, segments
```

## Airflow DAG

```
generate_daily_data
       ↓
   run_staging
       ↓
  upload_to_gcs          
       ↓
 load_to_bigquery         
       ↓
    dbt_run               
       ↓
    dbt_test            
```

## Setup

### 1. GCP Prerequisites

```bash
# Tạo GCS bucket
gsutil mb -l US gs://ecom-pipeline-raw

# Tạo BigQuery dataset
bq mk --dataset --location=US your-project:ecom_warehouse

# Tạo Service Account
gcloud iam service-accounts create ecom-pipeline-sa \
  --display-name="E-Commerce Pipeline SA"

# Gán roles
gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:ecom-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:ecom-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:ecom-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Download key
gcloud iam service-accounts keys create secrets/gcp-sa-key.json \
  --iam-account=ecom-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com
```

### 2. Config

```bash
cp .env.example .env
# Chỉnh sửa GCP_PROJECT_ID, GCS_BUCKET, BQ_DATASET trong .env
mkdir -p secrets
# Đặt gcp-sa-key.json vào ./secrets/
```

### 3. Run

```bash
docker-compose up -d
# Truy cập Airflow UI: http://localhost:8080 (admin/admin)
# Enable DAG: ecom_etl_pipeline_cloud
```

### 4. dbt (chạy thủ công để test)

```bash
cd dbt
dbt debug     # Kiểm tra connection
dbt run       # Chạy tất cả models
dbt test      # Chạy data quality tests
dbt docs generate && dbt docs serve  # Xem lineage graph
```

## Files thêm mới so với bản gốc

| File | Mô tả |
|------|-------|
| `scripts/gcs_upload.py` | Export staging → GCS Parquet |
| `scripts/bq_load.py` | Load GCS → BigQuery |
| `scripts/db.py` | Connection helper (PG + BQ) |
| `dbt/models/staging/*.sql` | dbt staging views |
| `dbt/models/warehouse/fct_orders.sql` | Incremental fact table trên BQ |
| `dbt/models/marts/*.sql` | Business metric tables |
| `dbt/models/schema.yml` | dbt tests (not_null, unique, accepted_values) |

## Chi phí ước tính (GCP Free Tier)

- **GCS:** 5 GB free/tháng — Parquet ~250 orders/day ≈ vài KB/ngày → **$0**
- **BigQuery:** 1 TB query free/tháng, 10 GB storage free → **$0** với data nhỏ
- **Chú ý:** Bật partition pruning trong dbt models để tránh full table scan
