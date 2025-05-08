
{% set src_table = var('full_load_src_table') %}
{% set tgt_table = var('full_load_tgt_table') %}
 
{{ config(
    materialized='table',
    alias=tgt_table,
    post_hook=["{{ apply_dynamic_masking()}}"]
) }}
 
SELECT
        {{get_columns(src_table)}}
FROM {{ ref(src_table) }}
