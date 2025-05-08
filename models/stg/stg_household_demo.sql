{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
        HD_DEMO_SK AS DEMO_SK, 
        HD_BUY_POTENTIAL AS BUY_POTENTIAL, 
        HD_INCOME_BAND_SK AS INCOME_BAND_SK, 
        HD_VEHICLE_COUNT AS VEHICLE_COUNT
    FROM {{ source('TPCDS_SF10TCL', 'HOUSEHOLD_DEMOGRAPHICS') }}
)

SELECT
    *
    {{add_current_timestamp()}}
FROM source_data



 