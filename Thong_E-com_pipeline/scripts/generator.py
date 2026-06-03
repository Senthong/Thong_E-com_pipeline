import os
import argparse
import random
import logging
from datetime import datetime, date
from pathlib import Path

import pandas as pd

from db import get_pg_conn

DATA_DIR = Path(os.getenv("OLIST_DATA_DIR", "/opt/airflow/data/olist"))
RANDOM_SEED = int(os.getenv("RANDOM_SEED", 42))
DEFAULT_DAILY_LIMIT = 250

logging.basicConfig(level=logging.INFO, format="[generator] %(message)s")
log = logging.getLogger(__name__)


FILES = {
    "customers":     "olist_customers_dataset.csv",
    "orders":        "olist_orders_dataset.csv",
    "order_items":   "olist_order_items_dataset.csv",
    "order_payments":"olist_order_payments_dataset.csv",
    "products":      "olist_products_dataset.csv",
    "translation":   "product_category_name_translation.csv",
}

STATUS_MAP = {
    "delivered":    "completed",
    "shipped":      "completed",
    "approved":     "pending",
    "processing":   "pending",
    "invoiced":     "pending",
    "created":      "pending",
    "canceled":     "cancelled",
    "unavailable":  "cancelled",
}


CATEGORY_MAP = {
    "computers_accessories":         "Electronics",
    "electronics":                   "Electronics",
    "computers":                     "Electronics",
    "telephony":                     "Electronics",
    "tablets_printing_image":        "Electronics",
    "audio":                         "Electronics",
    "watches_gifts":                 "Electronics",
    "consoles_games":                "Electronics",
    "fixed_telephony":               "Electronics",
    "small_appliances":              "Electronics",
    "small_appliances_home_oven_and_coffee": "Electronics",
    "air_conditioning":              "Electronics",
    "home_appliances":               "Electronics",
    "home_appliances_2":             "Electronics",
    "pc_gamer":                      "Electronics",
    "security_and_services":         "Electronics",
    "signaling_and_security":        "Electronics",
    # Clothing
    "fashion_bags_accessories":      "Clothing",
    "fashio_female_clothing":        "Clothing",
    "fashion_male_clothing":         "Clothing",
    "fashion_shoes":                 "Clothing",
    "fashion_underwear_beach":       "Clothing",
    "fashion_sport":                 "Clothing",
    "fashion_childrens_clothes":     "Clothing",
    "luggage_accessories":           "Clothing",
    "fashion_bags_accessories":      "Clothing",
    # Food & Beverage
    "food_drink":                    "Food & Beverage",
    "food":                          "Food & Beverage",
    "drinks":                        "Food & Beverage",
    # Books
    "books_general_interest":        "Books",
    "books_technical":               "Books",
    "books_imported":                "Books",
    "cds_dvds_musicals":             "Books",
    "dvds_blu_ray":                  "Books",
    "music":                         "Books",
    # Sports
    "sports_leisure":                "Sports",
    "la_cuisine":                    "Sports",  # kitchen fitness overlap
    # Beauty
    "health_beauty":                 "Beauty",
    "perfumery":                     "Beauty",
    "diapers_and_hygiene":           "Beauty",
    # Home & Living (catch-all)
    "furniture_decor":               "Home & Living",
    "furniture_living_room":         "Home & Living",
    "furniture_bedroom":             "Home & Living",
    "furniture_mattress_and_upholstery": "Home & Living",
    "furniture_office":              "Home & Living",
    "bed_bath_table":                "Home & Living",
    "housewares":                    "Home & Living",
    "garden_tools":                  "Home & Living",
    "construction_tools_construction": "Home & Living",
    "construction_tools_safety":     "Home & Living",
    "construction_tools_lights":     "Home & Living",
    "costruction_tools_garden":      "Home & Living",
    "home_confort":                  "Home & Living",
    "home_comfort_2":                "Home & Living",
    "kitchen_dining_laundry_garden_furniture": "Home & Living",
    "office_furniture":              "Home & Living",
    "christmas_supplies":            "Home & Living",
    "flowers":                       "Home & Living",
    "market_place":                  "Home & Living",
    "industry_commerce_and_business":"Home & Living",
    "agro_industry_and_commerce":    "Home & Living",
    "party_supplies":                "Home & Living",
    "art":                           "Home & Living",
    "arts_and_craftmanship":         "Home & Living",
    "toys":                          "Home & Living",
    "baby":                          "Home & Living",
    "stationery":                    "Home & Living",
    "musical_instruments":           "Home & Living",
    "cool_stuff":                    "Home & Living",
    "auto":                          "Home & Living",
    "pet_shop":                      "Home & Living",
    "portable_kitchen_food_processors": "Home & Living",
}
DEFAULT_CATEGORY = "Home & Living"

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _check_data_dir():
    """Kiểm tra DATA_DIR tồn tại và có đủ file cần thiết."""
    if not DATA_DIR.exists():
        raise FileNotFoundError(
            f"\n[ERROR] Không tìm thấy thư mục dữ liệu Olist: {DATA_DIR}\n"
            "  1. Tải dataset tại: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce\n"
            f"  2. Giải nén vào: {DATA_DIR}\n"
            f"  3. Hoặc đặt biến môi trường OLIST_DATA_DIR=<đường_dẫn>\n"
        )
    missing = [
        name for name, fname in FILES.items()
        if not (DATA_DIR / fname).exists()
    ]
    if missing:
        raise FileNotFoundError(
            f"[ERROR] Thiếu các file Olist: {[FILES[k] for k in missing]}\n"
            f"  Kiểm tra lại thư mục: {DATA_DIR}"
        )


def _load_csv(key: str, **kwargs) -> pd.DataFrame:
    path = DATA_DIR / FILES[key]
    log.info(f"Loading {FILES[key]} ...")
    return pd.read_csv(path, **kwargs)


def _map_status(olist_status: str) -> str:
    return STATUS_MAP.get(str(olist_status).lower(), "pending")


def _map_category(eng_name: str) -> str:
    if pd.isna(eng_name):
        return DEFAULT_CATEGORY
    return CATEGORY_MAP.get(str(eng_name).lower().strip(), DEFAULT_CATEGORY)

# ─── Build DataFrames ────────────────────────────────────────────────────────

def build_customers_df(customers_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Dedup theo customer_unique_id (1 khách hàng có thể có nhiều orders
    với customer_id khác nhau trong Olist).
    """
    rng = random.Random(RANDOM_SEED)
    df = (
        customers_raw[["customer_unique_id", "customer_city"]]
        .drop_duplicates(subset=["customer_unique_id"])
        .rename(columns={"customer_unique_id": "customer_id",
                         "customer_city": "city"})
        .copy()
    )
    df["full_name"] = df["customer_id"].apply(
        lambda x: f"Customer {str(x)[:8].upper()}"
    )
    df["email"] = df["customer_id"].apply(
        lambda x: f"{str(x)[:12]}@olist.example.com"
    )
    df["city"] = df["city"].str.title().fillna("Unknown")
    df["age"] = [rng.randint(18, 65) for _ in range(len(df))]
    df["created_at"] = datetime.now()
    return df[["customer_id", "full_name", "email", "city", "age", "created_at"]]


def build_products_df(
    products_raw: pd.DataFrame,
    order_items: pd.DataFrame,
    translation: pd.DataFrame,
) -> pd.DataFrame:
    """
    Tính sell_price = median price per product từ order_items.
    cost_price = sell_price * 0.55 (giả định gross margin ~45%).
    """
    # Median price per product
    price_agg = (
        order_items[["product_id", "price"]]
        .groupby("product_id")["price"]
        .median()
        .reset_index()
        .rename(columns={"price": "sell_price"})
    )

    # Join translation (English category names)
    trans_clean = translation.rename(
        columns={"product_category_name": "product_category_name",
                 "product_category_name_english": "eng_name"}
    )

    df = (
        products_raw[["product_id", "product_category_name"]]
        .merge(price_agg, on="product_id", how="inner")       # chỉ sản phẩm có giá
        .merge(trans_clean, on="product_category_name", how="left")
    )

    df["category"] = df["eng_name"].apply(_map_category)
    df["name"]     = df["eng_name"].fillna("Unknown").str.replace("_", " ").str.title()
    df["sell_price"] = df["sell_price"].round(2)
    df["cost_price"] = (df["sell_price"] * 0.55).round(2)
    df["created_at"] = datetime.now()

    return df[["product_id", "name", "category", "cost_price", "sell_price", "created_at"]]


def build_orders_df(
    orders_raw: pd.DataFrame,
    order_items: pd.DataFrame,
    customers_raw: pd.DataFrame,
) -> pd.DataFrame:
    """
    Mỗi row = 1 (order_id, product_id) combination.
    Nếu 1 order có nhiều item cùng product → gộp quantity.
    Map customer_id: orders.customer_id → customers.customer_unique_id
    """
    # Map customer_id → customer_unique_id
    cid_map = (
        customers_raw[["customer_id", "customer_unique_id"]]
        .drop_duplicates("customer_id")
        .set_index("customer_id")["customer_unique_id"]
    )

    # Aggregate items: same order + same product → sum quantity
    items_agg = (
        order_items[["order_id", "product_id", "price"]]
        .copy()
        .assign(quantity=1)
        .groupby(["order_id", "product_id"])
        .agg(quantity=("quantity", "sum"), unit_price=("price", "mean"))
        .reset_index()
    )
    items_agg["quantity"]   = items_agg["quantity"].astype(int)
    items_agg["unit_price"] = items_agg["unit_price"].round(2)

    # Join với orders để lấy status + timestamp
    df = items_agg.merge(
        orders_raw[["order_id", "customer_id",
                    "order_status", "order_purchase_timestamp"]],
        on="order_id",
        how="inner",
    )

    df["customer_id"] = df["customer_id"].map(cid_map).fillna(df["customer_id"])
    df["status"]      = df["order_status"].apply(_map_status)
    df["created_at"]  = pd.to_datetime(
        df["order_purchase_timestamp"], errors="coerce"
    ).fillna(datetime.now())

    return df[[
        "order_id", "customer_id", "product_id",
        "quantity", "unit_price", "status", "created_at"
    ]]

# ─── DB Writers ──────────────────────────────────────────────────────────────

def _insert_customers(cur, df: pd.DataFrame):
    rows = list(df.itertuples(index=False, name=None))
    cur.executemany(
        """
        INSERT INTO raw_customers
            (customer_id, full_name, email, city, age, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    log.info(f"  Customers inserted/skipped: {len(rows)}")


def _insert_products(cur, df: pd.DataFrame):
    rows = list(df.itertuples(index=False, name=None))
    cur.executemany(
        """
        INSERT INTO raw_products
            (product_id, name, category, cost_price, sell_price, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    log.info(f"  Products inserted/skipped: {len(rows)}")


def _insert_orders(cur, df: pd.DataFrame):
    rows = list(df.itertuples(index=False, name=None))
    cur.executemany(
        """
        INSERT INTO raw_orders
            (order_id, customer_id, product_id, quantity, unit_price, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    log.info(f"  Orders inserted/skipped: {len(rows)}")

# ─── Main entry points ───────────────────────────────────────────────────────

def seed_all_data():
    """
    Seed lần đầu: load TOÀN BỘ Olist dataset vào PostgreSQL.
    Gọi 1 lần duy nhất khi khởi tạo pipeline.
    """
    _check_data_dir()
    log.info("=== SEEDING full Olist dataset ===")

    customers_raw  = _load_csv("customers")
    orders_raw     = _load_csv("orders")
    order_items    = _load_csv("order_items")
    products_raw   = _load_csv("products")
    translation    = _load_csv("translation")

    customers_df = build_customers_df(customers_raw)
    products_df  = build_products_df(products_raw, order_items, translation)
    orders_df    = build_orders_df(orders_raw, order_items, customers_raw)

    log.info(f"  Customers: {len(customers_df):,}")
    log.info(f"  Products:  {len(products_df):,}")
    log.info(f"  Orders:    {len(orders_df):,}")

    conn = get_pg_conn()
    cur  = conn.cursor()
    _insert_customers(cur, customers_df)
    _insert_products(cur, products_df)
    _insert_orders(cur, orders_df)
    conn.commit()
    cur.close()
    conn.close()
    log.info("=== Seed complete ===")


def generate_daily_data(num_orders: int = DEFAULT_DAILY_LIMIT, run_date: date = None):
    """
    Load orders Olist của ngày run_date từ dataset thực.
    Nếu không đủ orders thực → lấy sample ngẫu nhiên từ toàn dataset
    (giả lập 1 ngày vận hành mới).

    Interface giống hệt bản cũ (Faker) để DAG không cần thay đổi.
    """
    _check_data_dir()
    run_date = run_date or date.today()
    log.info(f"Starting for {run_date} — target {num_orders} orders")

    # Load raw files
    customers_raw  = _load_csv("customers")
    orders_raw     = _load_csv("orders")
    order_items    = _load_csv("order_items")
    products_raw   = _load_csv("products")
    translation    = _load_csv("translation")

    # Build full DataFrames
    customers_df = build_customers_df(customers_raw)
    products_df  = build_products_df(products_raw, order_items, translation)
    orders_full  = build_orders_df(orders_raw, order_items, customers_raw)

    # Lọc theo ngày thực nếu có
    orders_full["order_date"] = pd.to_datetime(
        orders_full["created_at"]
    ).dt.date
    daily = orders_full[orders_full["order_date"] == run_date].copy()

    if len(daily) < num_orders:
        # Không có đủ dữ liệu thực cho ngày đó → sample từ toàn dataset
        n_real = len(daily)
        n_sample = num_orders - n_real
        log.info(
            f"  {n_real} real orders for {run_date}; "
            f"sampling {n_sample} more from full dataset"
        )
        rest = orders_full[orders_full["order_date"] != run_date]
        sample = rest.sample(
            n=min(n_sample, len(rest)),
            random_state=RANDOM_SEED,
            replace=False,
        ).copy()
        # Đặt lại created_at thành run_date để staging không bị lọc nhầm
        sample["created_at"] = datetime.combine(run_date, datetime.min.time())
        daily = pd.concat([daily, sample], ignore_index=True)

    daily_orders = daily.head(num_orders)
    log.info(f"  Final orders to insert: {len(daily_orders)}")

    conn = get_pg_conn()
    cur  = conn.cursor()

    # Upsert customers + products liên quan đến batch này
    related_cids = set(daily_orders["customer_id"].unique())
    related_pids = set(daily_orders["product_id"].unique())

    cust_batch = customers_df[customers_df["customer_id"].isin(related_cids)]
    prod_batch = products_df[products_df["product_id"].isin(related_pids)]

    _insert_customers(cur, cust_batch)
    _insert_products(cur, prod_batch)
    _insert_orders(cur, daily_orders[[
        "order_id", "customer_id", "product_id",
        "quantity", "unit_price", "status", "created_at"
    ]])

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Done — {len(daily_orders)} orders inserted for {run_date}")


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load Olist data into pipeline raw tables."
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Seed toàn bộ dataset (chạy 1 lần đầu)",
    )
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="Ngày cần load orders (YYYY-MM-DD). Mặc định: hôm nay.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_DAILY_LIMIT,
        help=f"Số orders tối đa mỗi ngày (mặc định: {DEFAULT_DAILY_LIMIT}, 0=không giới hạn)",
    )
    args = parser.parse_args()

    if args.seed:
        seed_all_data()
    else:
        limit = args.limit if args.limit > 0 else 999_999
        generate_daily_data(num_orders=limit, run_date=args.date)
