{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT  
    CS_SOLD_DATE_SK as SOLD_DATE_SK
    ,CS_SOLD_TIME_SK as SOLD_TIME_SK
    ,CS_SHIP_DATE_SK as SHIP_DATE_SK
    ,CS_BILL_CUSTOMER_SK as BILL_CUSTOMER_SK
    ,CS_BILL_CDEMO_SK as BILL_CDEMO_SK
    ,CS_BILL_HDEMO_SK as BILL_HDEMO_SK
    ,CS_BILL_ADDR_SK as BILL_ADDR_SK
    ,CS_SHIP_CUSTOMER_SK as SHIP_CUSTOMER_SK
    ,CS_SHIP_CDEMO_SK as SHIP_CDEMO_SK
    ,CS_SHIP_HDEMO_SK as SHIP_HDEMO_SK
    ,CS_SHIP_ADDR_SK as SHIP_ADDR_SK
    ,CS_CALL_CENTER_SK as CALL_CENTER_SK
    ,CS_CATALOG_PAGE_SK as CATALOG_PAGE_SK
    ,CS_SHIP_MODE_SK as SHIP_MODE_SK
    ,CS_WAREHOUSE_SK as WAREHOUSE_SK
    ,CS_ITEM_SK as ITEM_SK
    ,CS_PROMO_SK as PROMO_SK
    ,CS_ORDER_NUMBER as ORDER_NUMBER
    ,CS_QUANTITY as QUANTITY
    ,CS_WHOLESALE_COST as WHOLESALE_COST
    ,CS_LIST_PRICE as LIST_PRICE
    ,CS_SALES_PRICE as SALES_PRICE
    ,CS_EXT_DISCOUNT_AMT as EXT_DISCOUNT_AMT
    ,CS_EXT_SALES_PRICE as EXT_SALES_PRICE
    ,CS_EXT_WHOLESALE_COST as EXT_WHOLESALE_COST
    ,CS_EXT_LIST_PRICE as EXT_LIST_PRICE
    ,CS_EXT_TAX as EXT_TAX
    ,CS_COUPON_AMT as COUPON_AMT
    ,CS_EXT_SHIP_COST as EXT_SHIP_COST
    ,CS_NET_PAID as NET_PAID
    ,CS_NET_PAID_INC_TAX as NET_PAID_INC_TAX
    ,CS_NET_PAID_INC_SHIP as NET_PAID_INC_SHIP
    ,CS_NET_PAID_INC_SHIP_TAX as NET_PAID_INC_SHIP_TAX
    ,CS_NET_PROFIT as NET_PROFIT
FROM {{ source('TPCDS_SF10TCL', 'catalog_sales') }}
    
     where  CS_BILL_CUSTOMER_SK in ('64850438'
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