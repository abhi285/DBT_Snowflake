{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
        WR_RETURNED_DATE_SK	as RETURNED_DATE_SK,
        WR_RETURNED_TIME_SK	as RETURNED_TIME_SK,
        WR_ITEM_SK as ITEM_SK,
        WR_REFUNDED_CUSTOMER_SK	as REFUNDED_CUSTOMER_SK,
        WR_REFUNDED_CDEMO_SK as REFUNDED_CDEMO_SK,
        WR_REFUNDED_HDEMO_SK as REFUNDED_HDEMO_SK,
        WR_REFUNDED_ADDR_SK	as REFUNDED_ADDR_SK,
        WR_RETURNING_CUSTOMER_SK as RETURNING_CUSTOMER_SK,
        WR_RETURNING_CDEMO_SK as RETURNING_CDEMO_SK,
        WR_RETURNING_HDEMO_SK as RETURNING_HDEMO_SK,
        WR_RETURNING_ADDR_SK as RETURNING_ADDR_SK,
        WR_WEB_PAGE_SK as WEB_PAGE_SK,
        WR_REASON_SK as REASON_SK,
        WR_ORDER_NUMBER	as ORDER_NUMBER,
        WR_RETURN_QUANTITY as RETURN_QUANTITY,
        WR_RETURN_AMT as RETURN_AMT,
        WR_RETURN_TAX as RETURN_TAX,
        WR_RETURN_AMT_INC_TAX as RETURN_AMT_INC_TAX,
        WR_FEE as FEE,
        WR_RETURN_SHIP_COST	as RETURN_SHIP_COST,
        WR_REFUNDED_CASH as REFUNDED_CASH,
        WR_REVERSED_CHARGE as REVERSED_CHARGE,
        WR_ACCOUNT_CREDIT as ACCOUNT_CREDIT,
        WR_NET_LOSS	as NET_LOSS
FROM {{ source('TPCDS_SF10TCL', 'WEB_RETURNS') }}
    WHERE  WR_RETURNING_CUSTOMER_SK in ('64850438'
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