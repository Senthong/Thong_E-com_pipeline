-- models/warehouse/fct_orders.sql
-- Fact table với incremental load: chỉ process order_date mới.
-- Tính profit ngay tại đây thay vì ở marts.

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['status', 'order_date'],
        incremental_strategy='merge'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.total_amount,
    ROUND(p.cost_price * o.quantity, 2)              AS cost_amount,
    ROUND(o.total_amount - p.cost_price * o.quantity, 2) AS profit,
    o.status,
    c.city,
    c.age_group,
    p.category,
    p.name                                           AS product_name,
    CURRENT_TIMESTAMP()                              AS loaded_at

FROM orders o
LEFT JOIN customers c ON c.customer_id = o.customer_id
LEFT JOIN products  p ON p.product_id  = o.product_id

{% if is_incremental() %}
    -- Chỉ process partition mới để tiết kiệm chi phí BigQuery
    WHERE o.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
{% endif %}
