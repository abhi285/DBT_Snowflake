{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
    CD_DEMO_SK as DEMO_SK
    ,CD_GENDER as GENDER
    ,CD_MARITAL_STATUS as MARITAL_STATUS
    ,CD_EDUCATION_STATUS as EDUCATION_STATUS
    ,CD_PURCHASE_ESTIMATE as PURCHASE_ESTIMATE
    ,CD_CREDIT_RATING as CREDIT_RATING
    ,CD_DEP_COUNT as DEP_COUNT
    ,CD_DEP_EMPLOYED_COUNT as DEP_EMPLOYED_COUNT
    ,CD_DEP_COLLEGE_COUNT as DEP_COLLEGE_COUNT
    FROM {{ source('TPCDS_SF10TCL', 'CUSTOMER_DEMOGRAPHICS') }}
)

SELECT
    *
    {{add_current_timestamp()}}
FROM source_data



 