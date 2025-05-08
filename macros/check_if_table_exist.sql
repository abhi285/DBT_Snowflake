{% macro check_if_table_exist(schema_name,table_name) -%}
{{ log("check if table " ~ schema_name  ~ "." ~table_name ~ "exist:", info=true) }}    
    -- Check if the table exists
    {% set relation = adapter.get_relation(database=None, schema=schema_name, identifier=table_name) %}

    {% if relation is none %}
         {{ log("Table " ~ schema_name  ~ "." ~table_name ~ "does not exist.", info=true) }}    
        {{return(false)}}
    {% else %}
        {{ log("Table " ~ schema_name  ~ "." ~table_name ~ "exists.", info=true) }}    
        {{return(true)}}   
    {% endif %}
{% endmacro %}
