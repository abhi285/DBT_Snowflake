{{ config( 
    materialized='incremental',
    unique_key='date_sk'
) }}

SELECT
    {{ get_columns('stg_date') }}
FROM {{ ref('stg_date') }}

{% if is_incremental() %}
    WHERE CREATE_DTTM > (
        SELECT MAX(CREATE_DTTM)
        FROM {{ this }}
    )
{% endif %}