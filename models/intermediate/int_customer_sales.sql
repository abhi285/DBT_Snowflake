{{ config(materialized="table") }}
{# Aggregates total sales, total orders, quantity, and AOV per customer #}
with
    all_sales as (
        select customer_sk, ticket_number as order_number, quantity, sales_price,  dd.DATE as order_date
        from {{ ref("fact_store_sales") }} sales_table
        LEFT JOIN {{ ref("dim_date") }} dd
         ON sales_table.sold_date_sk = dd.date_sk

        union all

        select BILL_CUSTOMER_SK as customer_sk, order_number, quantity, sales_price, dd.DATE as order_date
        from {{ ref("fact_catalog_sales") }} sales_table
        LEFT JOIN {{ ref("dim_date") }}  dd
         ON sales_table.sold_date_sk = dd.date_sk

        union all

        select BILL_CUSTOMER_SK as customer_sk, order_number, quantity, sales_price, dd.DATE as order_date
        from {{ ref("fact_web_sales") }} sales_table
        LEFT JOIN {{ ref("dim_date") }} dd
         ON sales_table.sold_date_sk = dd.date_sk
    )

select
    c.customer_sk,
    s.order_date,
    count(distinct s.order_number) as total_orders,
    sum(s.quantity) as total_quantity,
    sum(s.sales_price) as total_sales_amount,
    DIV0(sum(s.sales_price), count(distinct s.order_number)) as avg_order_value
from all_sales s
join {{ ref("dim_customer") }} c on c.customer_sk = s.customer_sk
group by all

--test