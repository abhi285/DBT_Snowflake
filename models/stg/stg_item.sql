{{ config(
    materialized='table' )
}}

WITH source_data AS (
    SELECT 
        I_ITEM_SK	AS ITEM_SK,
        I_ITEM_ID	AS ITEM_ID,
        I_REC_START_DATE	AS REC_START_DATE,
        I_REC_END_DATE	AS REC_END_DATE,
        I_ITEM_DESC	AS ITEM_DESC,
        I_CURRENT_PRICE	AS CURRENT_PRICE,
        I_WHOLESALE_COST	AS WHOLESALE_COST,
        I_BRAND_ID	AS BRAND_ID,
        I_BRAND	AS BRAND,
        I_CLASS_ID	AS CLASS_ID,
        I_CLASS	AS CLASS,
        I_CATEGORY_ID	AS CATEGORY_ID,
        I_CATEGORY	AS CATEGORY,
        I_MANUFACT_ID	AS MANUFACT_ID,
        I_MANUFACT	AS MANUFACT,
        I_SIZE	AS SIZE,
        I_FORMULATION	AS FORMULATION,
        I_COLOR	AS COLOR,
        I_UNITS	AS UNITS,
        I_CONTAINER	AS CONTAINER,
        I_MANAGER_ID	AS MANAGER_ID,
        I_PRODUCT_NAME	AS PRODUCT_NAME
FROM {{ source('TPCDS_SF10TCL', 'ITEM') }}
)

SELECT
    *
    {{add_current_timestamp()}}
FROM source_data



 