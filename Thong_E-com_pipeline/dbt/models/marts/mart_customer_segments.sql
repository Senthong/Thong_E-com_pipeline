-- models/marts/mart_customer_segments.sql

{{
    config(
        materialized='table',
        partition_by={
            'field': 'report_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['city', 'age_group']
    )
}}

SELECT
    order_date                                  AS report_date,
    city,
    age_group,
    COUNT(DISTINCT customer_id)                 AS total_customers,
    COUNT(order_id)                             AS total_orders,
    ROUND(AVG(IF(status='completed', total_amount, NULL)), 2) AS avg_order_value,
    ROUND(SUM(IF(status='completed', total_amount, 0)), 2)    AS total_revenue,
    ROUND(SUM(IF(status='completed', profit, 0)), 2)          AS total_profit

FROM {{ ref('fct_orders') }}
GROUP BY order_date, city, age_group
