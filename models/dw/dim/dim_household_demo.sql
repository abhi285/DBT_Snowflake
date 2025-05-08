{{ config( 
    materialized='incremental',
    unique_key='demo_sk'
) }}

SELECT
    {{ get_columns('stg_household_demo') }}
FROM {{ ref('stg_household_demo') }}

{% if is_incremental() %}
    WHERE CREATE_DTTM > (
        SELECT MAX(CREATE_DTTM)
        FROM {{ this }}
    )
{% endif %}
