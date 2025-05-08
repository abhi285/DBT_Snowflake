{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
        D_DATE_SK	as DATE_SK,
        D_DATE_ID	as DATE_ID,
        D_DATE	as DATE,
        D_MONTH_SEQ	as MONTH_SEQ,
        D_WEEK_SEQ	as WEEK_SEQ,
        D_QUARTER_SEQ	as QUARTER_SEQ,
        D_YEAR	as YEAR,
        D_DOW	as DOW,
        D_MOY	as MOY,
        D_DOM	as DOM,
        D_QOY	as QOY,
        D_FY_YEAR	as FY_YEAR,
        D_FY_QUARTER_SEQ	as FY_QUARTER_SEQ,
        D_FY_WEEK_SEQ	as FY_WEEK_SEQ,
        D_DAY_NAME	as DAY_NAME,
        D_QUARTER_NAME	as QUARTER_NAME,
        D_HOLIDAY	as HOLIDAY,
        D_WEEKEND	as WEEKEND,
        D_FOLLOWING_HOLIDAY	as FOLLOWING_HOLIDAY,
        D_FIRST_DOM	as FIRST_DOM,
        D_LAST_DOM	as LAST_DOM,
        D_SAME_DAY_LY	as SAME_DAY_LY,
        D_SAME_DAY_LQ	as SAME_DAY_LQ,
        D_CURRENT_DAY	as CURRENT_DAY,
        D_CURRENT_WEEK	as CURRENT_WEEK,
        D_CURRENT_MONTH	as CURRENT_MONTH,
        D_CURRENT_QUARTER	as CURRENT_QUARTER,
        D_CURRENT_YEAR	as CURRENT_YEAR
    FROM {{ source('TPCDS_SF10TCL', 'DATE_DIM') }}
)

SELECT
    *
    {{add_current_timestamp()}}
FROM source_data