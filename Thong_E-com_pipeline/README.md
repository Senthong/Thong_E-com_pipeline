# E-Commerce Data Pipeline — Cloud Version

Pipeline ETL daily cho e-commerce, sử dụng **Kaggle Olist Brazilian E-Commerce Dataset** làm nguồn dữ liệu thực.

## Nguồn dữ liệu: Olist Dataset

Dataset: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Gồm ~100K đơn hàng thực từ 2016–2018 của marketplace Olist (Brazil).

### Files cần thiết

| File Olist | Dùng để |
|---|---|
| `olist_customers_dataset.csv` | raw_customers |
| `olist_products_dataset.csv` | raw_products |
| `product_category_name_translation.csv` | tên danh mục tiếng Anh |
| `olist_orders_dataset.csv` | raw_orders (status, timestamp) |
| `olist_order_items_dataset.csv` | raw_orders (product, price, quantity) |
| `olist_order_payments_dataset.csv` | *(tham chiếu, chưa dùng trong v1)* |

### Schema mapping

```
Olist → Pipeline

olist_customers_dataset:
  customer_unique_id  → raw_customers.customer_id   (deduplicated)
  customer_city       → raw_customers.city

olist_products_dataset + translation:
  product_id          → raw_products.product_id
  category (English)  → raw_products.category  (normalised về 7 nhóm)
  median(price)       → raw_products.sell_price
  sell_price × 0.55   → raw_products.cost_price

olist_orders + order_items:
  order_id            → raw_orders.order_id
  customer_unique_id  → raw_orders.customer_id
  product_id          → raw_orders.product_id
  count(items)        → raw_orders.quantity
  price               → raw_orders.unit_price
  order_status        → raw_orders.status  (mapped → completed/pending/cancelled)
  order_purchase_timestamp → raw_orders.created_at
```

## Architecture

```
Kaggle Olist CSV files  (đặt tại /opt/airflow/data/olist/)
          │
          ▼  generator.py (load + transform + insert)
    PostgreSQL (raw_*)          ← Layer 1: Ingestion
          │
          ▼  staging.py
    PostgreSQL (stg_*)          ← Layer 2: Validate + Clean (is_valid flag)
          │
          ▼  gcs_upload.py
    GCS Bucket (Parquet)        ← Layer 3: Landing Zone (partitioned by date)
    gs://ecom-pipeline-raw/staging/{table}/date=YYYY-MM-DD/data.parquet
          │
          ▼  bq_load.py
    BigQuery (raw_*)            ← Layer 4: Raw tables (partitioned + clustered)
          │
          ▼  dbt
    BigQuery (stg_*)            ← Views: rename + cast
    BigQuery (fct_orders)       ← Incremental table: star schema + profit calc
    BigQuery (mart_*)           ← Tables: daily revenue, product perf, segments
```

## Airflow DAG

```
generate_daily_data   ← Load Olist orders của ngày hôm nay
        ↓
    run_staging        ← Validate + clean (is_valid flag)
        ↓
  upload_to_gcs        ← Export Parquet lên GCS
        ↓
 load_to_bigquery      ← Load Parquet vào BigQuery raw tables
        ↓
      dbt_run          ← Transform: stg → fct → marts
        ↓
      dbt_test         ← Data quality checks
```

## Setup

### 0. Tải Olist Dataset

```bash
# Cách 1: Kaggle CLI
pip install kaggle
kaggle datasets download olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d /opt/airflow/data/olist/

# Cách 2: Tải thủ công tại
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# → Giải nén vào /opt/airflow/data/olist/
```

Hoặc đổi đường dẫn qua biến môi trường:
```
OLIST_DATA_DIR=/path/to/olist/files
```

### 1. Seed lần đầu (chạy 1 lần duy nhất)

```bash
# Load toàn bộ ~100K orders lịch sử vào PostgreSQL
python scripts/generator.py --seed
```

### 2. GCP Prerequisites

```bash
# Tạo GCS bucket
gsutil mb -l US gs://ecom-pipeline-raw

# Tạo BigQuery dataset
bq mk --dataset --location=US your-project:ecom_warehouse

# Tạo Service Account + gán roles
gcloud iam service-accounts create ecom-pipeline-sa \
  --display-name="E-Commerce Pipeline SA"

gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:ecom-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:ecom-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:ecom-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud iam service-accounts keys create secrets/gcp-sa-key.json \
  --iam-account=ecom-pipeline-sa@YOUR_PROJECT.iam.gserviceaccount.com
```

### 3. Config

```bash
cp .env.example .env
# Chỉnh GCP_PROJECT_ID, GCS_BUCKET, BQ_DATASET, OLIST_DATA_DIR trong .env
mkdir -p secrets
# Đặt gcp-sa-key.json vào ./secrets/
```

### 4. Run

```bash
docker-compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)
# Enable DAG: ecom_etl_pipeline_cloud
```

### 5. dbt (chạy thủ công)

```bash
cd dbt
dbt debug
dbt run
dbt test
dbt docs generate && dbt docs serve
```

## Daily mode vs Seed mode

| Mode | Lệnh | Mô tả |
|---|---|---|
| Seed (1 lần đầu) | `python generator.py --seed` | Load toàn bộ ~100K orders lịch sử |
| Daily (Airflow) | `generate_daily_data(num_orders=250)` | Load orders của ngày hôm nay. Nếu thiếu → sample thêm từ dataset |
| Custom date | `python generator.py --date 2017-10-15` | Load orders của ngày cụ thể |
| No limit | `python generator.py --limit 0` | Load toàn bộ orders của ngày đó |

## Chi phí ước tính (GCP Free Tier)

- **GCS:** 5 GB free/tháng — Parquet ~250 orders/day ≈ vài KB/ngày → **$0**
- **BigQuery:** 1 TB query free/tháng, 10 GB storage free → **$0** với data nhỏ
- **Lưu ý:** Bật partition pruning trong dbt models để tránh full table scan
