-- int_customer_returns.sql
-- Aggregates returns per customer and calculates return rate

WITH all_returns AS (
    select customer_sk, return_quantity, return_amt as return_amount,  dd.DATE as return_date
        from {{ ref("fact_store_returns") }} sales_table
        LEFT JOIN {{ ref("dim_date") }} dd
         ON sales_table.RETURNED_DATE_SK = dd.date_sk

        union all

        select REFUNDED_CUSTOMER_SK as customer_sk, return_quantity, return_amount, dd.DATE as return_date
        from {{ ref("fact_catalog_returns") }} sales_table
        LEFT JOIN {{ ref("dim_date") }}  dd
         ON sales_table.RETURNED_DATE_SK = dd.date_sk

        union all

        select REFUNDED_CUSTOMER_SK as customer_sk, return_quantity, return_amt as return_amount, dd.DATE as return_date
        from {{ ref("fact_web_returns") }} sales_table
        LEFT JOIN {{ ref("dim_date") }} dd
         ON sales_table.RETURNED_DATE_SK = dd.date_sk
)

SELECT
    customer_sk,
    return_date,
    COUNT(*) AS num_returns,
    SUM(return_amount) AS total_return_amount,
    AVG(return_quantity) AS avg_return_quantity,
    DIV0(SUM(return_amount), NULLIF(SUM(return_amount) + SUM(return_quantity), 0)) AS return_rate
FROM all_returns
GROUP BY customer_sk, return_date