{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
    CA_ADDRESS_SK as ADDRESS_SK
    ,CA_ADDRESS_ID as ADDRESS_ID
    ,CA_STREET_NUMBER as STREET_NUMBER
    ,CA_STREET_NAME as STREET_NAME
    ,CA_STREET_TYPE as STREET_TYPE
    ,CA_SUITE_NUMBER as SUITE_NUMBER
    ,CA_CITY as CITY
    ,CA_COUNTY as COUNTY
    ,CA_STATE as STATE
    ,CA_ZIP as ZIP
    ,CA_COUNTRY as COUNTRY
    ,CA_GMT_OFFSET as GMT_OFFSET
    ,CA_LOCATION_TYPE as LOCATION_TYPE
    FROM {{ source('TPCDS_SF10TCL', 'CUSTOMER_ADDRESS') }}
)

SELECT
    *
    {{add_current_timestamp()}}
FROM source_data