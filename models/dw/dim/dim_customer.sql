{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_SK',
) }}



SELECT
    {{get_columns('stg_cust')}}
FROM {{ ref('stg_cust') }}

{% if is_incremental() %}
WHERE CREATE_DTTM > (
    SELECT MAX(CREATE_DTTM)
    FROM {{ this }}
)
{% endif %}