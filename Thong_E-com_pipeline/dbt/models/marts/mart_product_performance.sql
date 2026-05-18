-- models/marts/mart_product_performance.sql

{{
    config(
        materialized='table',
        partition_by={
            'field': 'report_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['category']
    )
}}

SELECT
    order_date                                                              AS report_date,
    product_id,
    product_name,
    category,
    SUM(quantity)                                                           AS units_sold,
    ROUND(SUM(IF(status = 'completed', total_amount, 0)), 2)                AS revenue,
    ROUND(SUM(IF(status = 'completed', profit, 0)), 2)                      AS profit,
    ROUND(
        SUM(IF(status = 'completed', profit, 0))
        / NULLIF(SUM(IF(status = 'completed', total_amount, 0)), 0) * 100, 2
    )                                                                       AS profit_margin_pct,
    RANK() OVER (PARTITION BY order_date ORDER BY SUM(IF(status = 'completed', total_amount, 0)) DESC) AS revenue_rank

FROM {{ ref('fct_orders') }}
GROUP BY order_date, product_id, product_name, category
