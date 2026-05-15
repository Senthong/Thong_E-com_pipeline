"""
staging.py
Validate + clean raw data vào staging layer (PostgreSQL).
Đánh flag is_valid / invalid_reason thay vì xóa data xấu.
Không thay đổi so với bản gốc.
"""
from datetime import date
from db import get_pg_conn


def _age_group(age_col: str) -> str:
    return f"""
        CASE
            WHEN {age_col} BETWEEN 18 AND 24 THEN '18-24'
            WHEN {age_col} BETWEEN 25 AND 34 THEN '25-34'
            WHEN {age_col} BETWEEN 35 AND 44 THEN '35-44'
            WHEN {age_col} BETWEEN 45 AND 54 THEN '45-54'
            ELSE '55+'
        END
    """


def stage_customers(cur, run_date: date):
    age_group_sql = _age_group("age")
    sql = f"""
        INSERT INTO stg_customers
            (customer_id, full_name, email, city, age, age_group, is_valid, invalid_reason)
        SELECT
            customer_id,
            TRIM(full_name),
            LOWER(TRIM(email)),
            city,
            age,
            {age_group_sql} AS age_group,
            CASE
                WHEN full_name IS NULL OR TRIM(full_name) = '' THEN FALSE
                WHEN email NOT LIKE '%%@%%' THEN FALSE
                WHEN age < 16 OR age > 100 THEN FALSE
                ELSE TRUE
            END,
            CASE
                WHEN full_name IS NULL OR TRIM(full_name) = '' THEN 'Missing or empty full name'
                WHEN email NOT LIKE '%%@%%' THEN 'Invalid email format'
                WHEN age < 16 OR age > 100 THEN 'Age out of range (16-100)'
                ELSE NULL
            END
        FROM raw_customers
        WHERE created_at::date = %s
        ON CONFLICT (customer_id) DO NOTHING
    """
    cur.execute(sql, (run_date,))
    print(f"  [staging] customers staged for {run_date}: {cur.rowcount} rows")


def stage_products(cur, run_date: date):
    sql = """
        INSERT INTO stg_products
            (product_id, name, category, cost_price, sell_price, margin_pct, is_valid, invalid_reason)
        SELECT
            product_id,
            TRIM(name),
            category,
            cost_price,
            sell_price,
            ROUND((sell_price - cost_price) / NULLIF(sell_price, 0) * 100, 2),
            CASE
                WHEN cost_price <= 0  THEN FALSE
                WHEN sell_price <= 0  THEN FALSE
                WHEN sell_price < cost_price THEN FALSE
                ELSE TRUE
            END,
            CASE
                WHEN cost_price <= 0  THEN 'Invalid cost price'
                WHEN sell_price <= 0  THEN 'Invalid sell price'
                WHEN sell_price < cost_price THEN 'Sell price below cost'
                ELSE NULL
            END
        FROM raw_products
        WHERE created_at::date = %s
        ON CONFLICT (product_id) DO NOTHING
    """
    cur.execute(sql, (run_date,))
    print(f"  [staging] products staged for {run_date}: {cur.rowcount} rows")


def stage_orders(cur, run_date: date):
    sql = """
        INSERT INTO stg_orders
            (order_id, customer_id, product_id, quantity, unit_price,
             total_amount, status, order_date, is_valid, invalid_reason)
        SELECT
            o.order_id, o.customer_id, o.product_id, o.quantity, o.unit_price,
            o.quantity * o.unit_price,
            o.status,
            o.created_at::date,
            CASE
                WHEN o.quantity <= 0   THEN FALSE
                WHEN o.unit_price <= 0 THEN FALSE
                WHEN o.status NOT IN ('completed','pending','cancelled') THEN FALSE
                WHEN c.customer_id IS NULL THEN FALSE
                WHEN p.product_id IS NULL  THEN FALSE
                ELSE TRUE
            END,
            CASE
                WHEN o.quantity <= 0   THEN 'Invalid quantity'
                WHEN o.unit_price <= 0 THEN 'Invalid unit price'
                WHEN o.status NOT IN ('completed','pending','cancelled') THEN 'Unknown status'
                WHEN c.customer_id IS NULL THEN 'Customer not found'
                WHEN p.product_id IS NULL  THEN 'Product not found'
                ELSE NULL
            END
        FROM raw_orders o
        LEFT JOIN raw_customers c ON o.customer_id = c.customer_id
        LEFT JOIN raw_products  p ON o.product_id  = p.product_id
        WHERE o.created_at::date = %s
        ON CONFLICT (order_id) DO NOTHING
    """
    cur.execute(sql, (run_date,))
    print(f"  [staging] orders staged for {run_date}: {cur.rowcount} rows")


def run_staging(run_date: date = None):
    run_date = run_date or date.today()
    print(f"[staging] Running for {run_date}")
    conn = get_pg_conn()
    cur = conn.cursor()
    stage_customers(cur, run_date)
    stage_products(cur, run_date)
    stage_orders(cur, run_date)
    conn.commit()
    cur.close()
    conn.close()
    print("[staging] Done")


if __name__ == "__main__":
    run_staging()
