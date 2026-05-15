-- models/staging/stg_orders.sql
-- View trên raw_orders trong BigQuery.
-- Rename + cast columns, không transform business logic.

{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    CAST(unit_price   AS NUMERIC) AS unit_price,
    CAST(total_amount AS NUMERIC) AS total_amount,
    LOWER(status)                 AS status,
    order_date
FROM {{ source('raw', 'raw_orders') }}
WHERE order_id IS NOT NULL
  AND quantity > 0
  AND unit_price > 0
