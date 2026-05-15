-- models/staging/stg_products.sql

{{ config(materialized='view') }}

SELECT
    product_id,
    TRIM(name)                AS name,
    category,
    CAST(cost_price AS NUMERIC) AS cost_price,
    CAST(sell_price AS NUMERIC) AS sell_price,
    CAST(margin_pct AS NUMERIC) AS margin_pct
FROM {{ source('raw', 'raw_products') }}
WHERE product_id IS NOT NULL
  AND cost_price > 0
  AND sell_price > cost_price
