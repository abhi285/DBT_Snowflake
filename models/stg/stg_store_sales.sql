{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
        SS_SOLD_DATE_SK as SOLD_DATE_SK
    ,SS_SOLD_TIME_SK as SOLD_TIME_SK
    ,SS_ITEM_SK as ITEM_SK
    ,SS_CUSTOMER_SK as CUSTOMER_SK
    ,SS_CDEMO_SK as CDEMO_SK
    ,SS_HDEMO_SK as HDEMO_SK
    ,SS_ADDR_SK as ADDR_SK
    ,SS_STORE_SK as STORE_SK
    ,SS_PROMO_SK as PROMO_SK
    ,SS_TICKET_NUMBER as TICKET_NUMBER
    ,SS_QUANTITY as QUANTITY
    ,SS_WHOLESALE_COST as WHOLESALE_COST
    ,SS_LIST_PRICE as LIST_PRICE
    ,SS_SALES_PRICE as SALES_PRICE
    ,SS_EXT_DISCOUNT_AMT as EXT_DISCOUNT_AMT
    ,SS_EXT_SALES_PRICE as EXT_SALES_PRICE
    ,SS_EXT_WHOLESALE_COST as EXT_WHOLESALE_COST
    ,SS_EXT_LIST_PRICE as EXT_LIST_PRICE
    ,SS_EXT_TAX as EXT_TAX
    ,SS_COUPON_AMT as COUPON_AMT
    ,SS_NET_PAID as NET_PAID
    ,SS_NET_PAID_INC_TAX as NET_PAID_INC_TAX
    ,SS_NET_PROFIT as NET_PROFIT
FROM {{ source('TPCDS_SF10TCL', 'STORE_SALES') }}
     where  SS_CUSTOMER_SK in ('64850438'
                            ,'64783728'
                            ,'64777728'
                            ,'64768365'
                            ,'64767740'
                            ,'64743398'
                            ,'64716327'
                            ,'64688212'
                            ,'64655213'
                            ,'64652376')
    
)

SELECT
    *
    {{add_current_timestamp()}}
FROM source_data



 