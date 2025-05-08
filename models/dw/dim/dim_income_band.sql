{{ config( 
    materialized='incremental',
    unique_key='income_band_sk'
) }}

SELECT
    {{ get_columns('stg_income_band') }}
FROM {{ ref('stg_income_band') }}

{% if is_incremental() %}
    WHERE CREATE_DTTM > (
        SELECT MAX(CREATE_DTTM)
        FROM {{ this }}
    )
{% endif %}
