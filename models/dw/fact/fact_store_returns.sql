{{ config( 
    materialized='incremental',
    unique_key=['ticket_number', 'item_sk']
) }}

SELECT
    {{ get_columns('stg_store_returns') }}
FROM {{ ref('stg_store_returns') }}

{% if is_incremental() %}
    WHERE CREATE_DTTM > (
        SELECT MAX(CREATE_DTTM)
        FROM {{ this }}
    )
{% endif %}