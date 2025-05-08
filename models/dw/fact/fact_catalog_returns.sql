{{ config( 
    materialized='incremental',
    unique_key=['order_number', 'item_sk']
) }}

SELECT
    {{ get_columns('stg_catalog_returns') }}
FROM {{ ref('stg_catalog_returns') }}

{% if is_incremental() %}
    WHERE CREATE_DTTM > (
        SELECT MAX(CREATE_DTTM)
        FROM {{ this }}
    )
{% endif %}
