{{ config(
    materialized='incremental',
    unique_key='DEMO_SK'
) }}


SELECT
    {{get_columns('stg_cust_demo')}}
FROM {{ ref('stg_cust_demo') }}

{% if is_incremental() %}
    WHERE CREATE_DTTM > (
        SELECT MAX(CREATE_DTTM)
        FROM {{ this }}
    )
{% endif %}