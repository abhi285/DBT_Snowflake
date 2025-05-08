-- int_customer_promos.sql
-- Identifies most frequent purchase channel and call center engagement

WITH channel_sales AS (
    select customer_sk, 'store' AS channel,  dd.DATE as sold_date
        from {{ ref("fact_store_sales") }} sales_table
        LEFT JOIN {{ ref("dim_date") }} dd
         ON sales_table.sold_date_sk = dd.date_sk

        union all

        select BILL_CUSTOMER_SK as customer_sk, 'catalog' AS channel, dd.DATE as sold_date
        from {{ ref("fact_catalog_sales") }} sales_table
        LEFT JOIN {{ ref("dim_date") }}  dd
         ON sales_table.sold_date_sk = dd.date_sk

        union all

        select BILL_CUSTOMER_SK as customer_sk, 'web' AS channel, dd.DATE as sold_date
        from {{ ref("fact_web_sales") }} sales_table
        LEFT JOIN {{ ref("dim_date") }} dd
         ON sales_table.sold_date_sk = dd.date_sk
),

ranked_channels AS (
    SELECT
        customer_sk,
        channel,
        COUNT(*) AS channel_count,
        ROW_NUMBER() OVER (PARTITION BY customer_sk ORDER BY COUNT(*) DESC) AS rnk
    FROM channel_sales
    GROUP BY customer_sk, channel
)

SELECT
    customer_sk,
    MAX(CASE WHEN rnk = 1 THEN channel END) AS most_freq_channel,
    COUNT(DISTINCT cc.call_center_sk) AS call_center_engagements
FROM ranked_channels rc
LEFT JOIN {{ ref('dim_call_center') }} cc ON rc.customer_sk IS NOT NULL
GROUP BY customer_sk