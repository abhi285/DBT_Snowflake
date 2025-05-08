{{ config( 
    materialized='incremental',
    unique_key='call_center_sk'
) }}

SELECT
    {{ get_columns('stg_call_center') }}
FROM {{ ref('stg_call_center') }}

{% if is_incremental() %}
    WHERE CREATE_DTTM > (
        SELECT MAX(CREATE_DTTM)
        FROM {{ this }}
    )
{% endif %}
