{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
        WS_SOLD_DATE_SK as SOLD_DATE_SK
        ,WS_SOLD_TIME_SK as SOLD_TIME_SK
        ,WS_SHIP_DATE_SK as SHIP_DATE_SK
        ,WS_ITEM_SK as ITEM_SK
        ,WS_BILL_CUSTOMER_SK as BILL_CUSTOMER_SK
        ,WS_BILL_CDEMO_SK as BILL_CDEMO_SK
        ,WS_BILL_HDEMO_SK as BILL_HDEMO_SK
        ,WS_BILL_ADDR_SK as BILL_ADDR_SK
        ,WS_SHIP_CUSTOMER_SK as SHIP_CUSTOMER_SK
        ,WS_SHIP_CDEMO_SK as SHIP_CDEMO_SK
        ,WS_SHIP_HDEMO_SK as SHIP_HDEMO_SK
        ,WS_SHIP_ADDR_SK as SHIP_ADDR_SK
        ,WS_WEB_PAGE_SK as WEB_PAGE_SK
        ,WS_WEB_SITE_SK as WEB_SITE_SK
        ,WS_SHIP_MODE_SK as SHIP_MODE_SK
        ,WS_WAREHOUSE_SK as WAREHOUSE_SK
        ,WS_PROMO_SK as PROMO_SK
        ,WS_ORDER_NUMBER as ORDER_NUMBER
        ,WS_QUANTITY as QUANTITY
        ,WS_WHOLESALE_COST as WHOLESALE_COST
        ,WS_LIST_PRICE as LIST_PRICE
        ,WS_SALES_PRICE as SALES_PRICE
        ,WS_EXT_DISCOUNT_AMT as EXT_DISCOUNT_AMT
        ,WS_EXT_SALES_PRICE as EXT_SALES_PRICE
        ,WS_EXT_WHOLESALE_COST as EXT_WHOLESALE_COST
        ,WS_EXT_LIST_PRICE as EXT_LIST_PRICE
        ,WS_EXT_TAX as EXT_TAX
        ,WS_COUPON_AMT as COUPON_AMT
        ,WS_EXT_SHIP_COST as EXT_SHIP_COST
        ,WS_NET_PAID as NET_PAID
        ,WS_NET_PAID_INC_TAX as NET_PAID_INC_TAX
        ,WS_NET_PAID_INC_SHIP as NET_PAID_INC_SHIP
        ,WS_NET_PAID_INC_SHIP_TAX as NET_PAID_INC_SHIP_TAX
        ,WS_NET_PROFIT as NET_PROFIT
FROM {{ source('TPCDS_SF10TCL', 'WEB_SALES') }}
    where  WS_BILL_CUSTOMER_SK in ('64850438'
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