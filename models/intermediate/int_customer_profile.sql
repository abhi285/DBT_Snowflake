{{ config(materialized="table") }}

-- Final model combining customer sales and metadata

SELECT
    m.customer_sk,
    m.income_range,
    m.customer_region,
    m.active_since_days,
    m.avg_days_between_purchases,
    s.order_date,
    s.total_orders,
    s.total_quantity,
    s.total_sales_amount,
    s.avg_order_value
FROM {{ ref('int_customer_metadata') }} m
LEFT JOIN {{ ref('int_customer_sales') }} s
  ON m.customer_sk = s.customer_sk
