{{ config( 
    materialized='incremental',
    unique_key='item_sk'
) }}

SELECT
    {{ get_columns('stg_item') }}
FROM {{ ref('stg_item') }}

{% if is_incremental() %}
    WHERE CREATE_DTTM > (
        SELECT MAX(CREATE_DTTM)
        FROM {{ this }}
    )
{% endif %}