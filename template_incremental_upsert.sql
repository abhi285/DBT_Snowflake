{% set src_table = var('incremental_src_table') %}
{% set tgt_table = var('incremental_tgt_table') %}
{% set src_schema = var('incremental_src_table') %}
{% set tgt_schema = var('incremental_tgt_table') %}
{% set key_column = var('key_column') %}
 
{{ config(
    materialized='incremental',
    alias=tgt_table,
	unique_key= key_column,
    post_hook=["{{ apply_dynamic_masking()}}"]
) }}
 
SELECT
 
    {%- if check_if_table_exist(tgt_schema,tgt_table)-%}
 
        {{generate_select_columns(src_schema,src_table,tgt_schema,tgt_table)}}
        
    {%- else -%}    
        
        {{get_columns(src_table)}}
 
    {%- endif -%}
    
 
FROM {{ ref(src_table) }}

{% if is_incremental() %}
 
    WHERE CREATE_DTTM > (
        SELECT MAX(CREATE_DTTM)
        FROM {{ this }}
    )
 
{% endif %}
