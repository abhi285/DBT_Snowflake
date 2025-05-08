-- int_customer_metadata.sql
-- Joins demographics and purchase timeline metrics

WITH first_last_purchase AS (
    SELECT
        customer_sk,
        MIN(d.date) AS first_purchase_date,
        MAX(d.date) AS last_purchase_date,
        COUNT(DISTINCT d.date) AS active_days
    FROM {{ ref('fact_store_sales') }} s
    JOIN {{ ref('dim_date') }} d ON s.sold_date_sk = d.date_sk
    GROUP BY customer_sk
),

purchase_gaps AS (
    SELECT
        customer_sk,
        AVG(days_between) AS avg_days_between_purchases
    FROM (
        SELECT
            customer_sk,
            DATEDIFF(
                'day',
                LEAD(date) OVER (PARTITION BY customer_sk ORDER BY date),
                date
            ) AS days_between
        FROM (
            SELECT
                s.customer_sk,
                d.date
            FROM {{ ref('fact_store_sales') }} s
            JOIN {{ ref('dim_date') }} d ON s.sold_date_sk = d.date_sk
        ) sub
    ) inner_sub
    GROUP BY customer_sk
)

SELECT
    c.customer_sk,
    hd.income_band_sk AS income_range,
    ca.state AS customer_region,
    DATEDIFF('day', fl.first_purchase_date, CURRENT_DATE) AS active_since_days,
    pg.avg_days_between_purchases
FROM {{ ref('dim_customer') }} c
LEFT JOIN {{ ref('dim_customer_addr') }} ca ON c.current_addr_sk = ca.address_sk
LEFT JOIN {{ ref('dim_customer_demo') }} cd ON c.current_cdemo_sk = cd.demo_sk
LEFT JOIN {{ ref('dim_household_demo') }} hd ON cd.demo_sk = hd.demo_sk
LEFT JOIN first_last_purchase fl ON c.customer_sk = fl.customer_sk
LEFT JOIN purchase_gaps pg ON c.customer_sk = pg.customer_sk
