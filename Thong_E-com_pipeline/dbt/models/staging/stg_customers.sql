-- models/staging/stg_customers.sql

{{ config(materialized='view') }}

SELECT
    customer_id,
    INITCAP(TRIM(full_name))  AS full_name,
    LOWER(TRIM(email))        AS email,
    city,
    age,
    age_group
FROM {{ source('raw', 'raw_customers') }}
WHERE customer_id IS NOT NULL
  AND email LIKE '%@%'
  AND age BETWEEN 16 AND 100
