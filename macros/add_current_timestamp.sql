{% macro add_current_timestamp() -%}
    {{ log("Adding current timestamp:" , info=true) }}
    ,CURRENT_TIMESTAMP AS CREATE_DTTM

{%- endmacro %}