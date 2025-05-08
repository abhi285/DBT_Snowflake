{{ config(
    materialized='incremental',
    unique_key=['item_sk', 'order_number']
) }}


SELECT
    {{get_columns('stg_web_sales')}}
FROM {{ ref('stg_web_sales') }}

{% if is_incremental() %}
WHERE CREATE_DTTM > (
    SELECT MAX(CREATE_DTTM)
    FROM {{ this }}
)
{% endif %}
