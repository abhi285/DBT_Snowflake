{% macro get_columns(table_name) -%}

{{ log("Fetch columns for " ~ table_name ~" table:", info=true) }}    

    {% set model_name = table_name|string %}
    {%- set relation = adapter.Relation.create(identifier=model_name) -%}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {%- set column_list = [] -%}
    {%- for column in columns -%}
        {%- do column_list.append(column.column ) -%}
    {%- endfor -%}

    {% set joined_columns = column_list | join(', ') %}
        {{joined_columns}} 
            
{%- endmacro %}