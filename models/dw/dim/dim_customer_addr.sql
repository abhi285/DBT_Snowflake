{{ config(
    materialized='incremental',
    unique_key='ADDRESS_SK'
) }}


SELECT
    {{get_columns('stg_cust_addr')}}
FROM {{ ref('stg_cust_addr') }}

{% if is_incremental() %}
    WHERE CREATE_DTTM > (
        SELECT MAX(CREATE_DTTM)
        FROM {{ this }}
    )
{% endif %}