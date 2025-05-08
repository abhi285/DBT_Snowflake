{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
    IB_INCOME_BAND_SK as INCOME_BAND_SK,
    IB_LOWER_BOUND as LOWER_BOUND,
    IB_UPPER_BOUND as UPPER_BOUND
    FROM {{ source('TPCDS_SF10TCL', 'INCOME_BAND') }}
)

SELECT
    *
    {{add_current_timestamp()}}
FROM source_data



 