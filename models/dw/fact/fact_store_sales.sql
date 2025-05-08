{{ config(
    materialized='incremental',
    unique_key=['item_sk', 'ticket_number']
) }}


SELECT
    {{get_columns('stg_store_sales')}}
FROM {{ ref('stg_store_sales') }}

{% if is_incremental() %}
WHERE CREATE_DTTM > (
    SELECT MAX(CREATE_DTTM)
    FROM {{ this }}
)
{% endif %}
