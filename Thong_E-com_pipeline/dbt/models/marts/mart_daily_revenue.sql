-- models/marts/mart_daily_revenue.sql
-- Business metric: doanh thu theo ngày.
-- Materialized = table + partitioned by report_date → BI query nhanh, rẻ.

{{
    config(
        materialized='table',
        partition_by={
            'field': 'report_date',
            'data_type': 'date',
            'granularity': 'day'
        }
    )
}}

SELECT
    order_date                                                        AS report_date,
    COUNT(*)                                                          AS total_orders,
    COUNTIF(status = 'completed')                                     AS completed_orders,
    COUNTIF(status = 'cancelled')                                     AS cancelled_orders,
    COUNTIF(status = 'pending')                                       AS pending_orders,
    ROUND(SUM(total_amount), 2)                                       AS gross_revenue,
    ROUND(SUM(IF(status = 'completed', total_amount, 0)), 2)          AS net_revenue,
    ROUND(SUM(IF(status = 'completed', profit, 0)), 2)                AS total_profit,
    ROUND(
        COUNTIF(status = 'cancelled') * 100.0 / NULLIF(COUNT(*), 0), 2
    )                                                                 AS cancel_rate,
    ROUND(AVG(IF(status = 'completed', total_amount, NULL)), 2)       AS avg_order_value,
    -- Thêm mới so với bản PostgreSQL: profit margin %
    ROUND(
        SUM(IF(status = 'completed', profit, 0))
        / NULLIF(SUM(IF(status = 'completed', total_amount, 0)), 0) * 100, 2
    )                                                                 AS profit_margin_pct

FROM {{ ref('fct_orders') }}
GROUP BY order_date
