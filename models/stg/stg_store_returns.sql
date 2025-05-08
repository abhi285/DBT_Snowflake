{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
        SR_RETURNED_DATE_SK	as RETURNED_DATE_SK,
        SR_RETURN_TIME_SK	as RETURN_TIME_SK,
        SR_ITEM_SK	as ITEM_SK,
        SR_CUSTOMER_SK	as CUSTOMER_SK,
        SR_CDEMO_SK	as CDEMO_SK,
        SR_HDEMO_SK	as HDEMO_SK,
        SR_ADDR_SK	as ADDR_SK,
        SR_STORE_SK	as STORE_SK,
        SR_REASON_SK	as REASON_SK,
        SR_TICKET_NUMBER	as TICKET_NUMBER,
        SR_RETURN_QUANTITY	as RETURN_QUANTITY,
        SR_RETURN_AMT	as RETURN_AMT,
        SR_RETURN_TAX	as RETURN_TAX,
        SR_RETURN_AMT_INC_TAX	as RETURN_AMT_INC_TAX,
        SR_FEE	as FEE,
        SR_RETURN_SHIP_COST	as RETURN_SHIP_COST,
        SR_REFUNDED_CASH	as REFUNDED_CASH,
        SR_REVERSED_CHARGE	as REVERSED_CHARGE,
        SR_STORE_CREDIT	as STORE_CREDIT,
        SR_NET_LOSS	as NET_LOSS
FROM {{ source('TPCDS_SF10TCL', 'STORE_RETURNS') }}
    WHERE  SR_CUSTOMER_SK in ('64850438'
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



 