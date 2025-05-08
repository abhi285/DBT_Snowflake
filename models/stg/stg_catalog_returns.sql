{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
        CR_RETURNED_DATE_SK as RETURNED_DATE_SK,
        CR_RETURNED_TIME_SK	as RETURNED_TIME_SK,
        CR_ITEM_SK as ITEM_SK,
        CR_REFUNDED_CUSTOMER_SK	as REFUNDED_CUSTOMER_SK,
        CR_REFUNDED_CDEMO_SK as REFUNDED_CDEMO_SK,
        CR_REFUNDED_HDEMO_SK as REFUNDED_HDEMO_SK,
        CR_REFUNDED_ADDR_SK	as REFUNDED_ADDR_SK,
        CR_RETURNING_CUSTOMER_SK as RETURNING_CUSTOMER_SK,
        CR_RETURNING_CDEMO_SK as RETURNING_CDEMO_SK,
        CR_RETURNING_HDEMO_SK as RETURNING_HDEMO_SK,
        CR_RETURNING_ADDR_SK as RETURNING_ADDR_SK,
        CR_CALL_CENTER_SK as CALL_CENTER_SK,
        CR_CATALOG_PAGE_SK as CATALOG_PAGE_SK,
        CR_SHIP_MODE_SK	as SHIP_MODE_SK,
        CR_WAREHOUSE_SK	as WAREHOUSE_SK,
        CR_REASON_SK as REASON_SK,
        CR_ORDER_NUMBER	as ORDER_NUMBER,
        CR_RETURN_QUANTITY as RETURN_QUANTITY,
        CR_RETURN_AMOUNT as RETURN_AMOUNT,
        CR_RETURN_TAX as RETURN_TAX,
        CR_RETURN_AMT_INC_TAX as RETURN_AMT_INC_TAX,
        CR_FEE as FEE,
        CR_RETURN_SHIP_COST	as RETURN_SHIP_COST,
        CR_REFUNDED_CASH as REFUNDED_CASH,
        CR_REVERSED_CHARGE as REVERSED_CHARGE,
        CR_STORE_CREDIT	as STORE_CREDIT,
        CR_NET_LOSS	as NET_LOSS
FROM {{ source('TPCDS_SF10TCL', 'CATALOG_RETURNS') }}
    WHERE  CR_RETURNING_CUSTOMER_SK in ('64850438'
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



 