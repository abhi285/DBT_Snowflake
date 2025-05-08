{% macro generate_select_columns(source_schema,source_table,target_schema,target_table) -%}

{{ log("Generate columns for " ~ target_table ~" target table and " ~ source_table ~ " source table", info=true) }} 


    {% set source_model_name = source_table |string %}
    {%- set source_relation = adapter.Relation.create(identifier=source_table,schema=source_schema) -%}
    {% set source_columns = adapter.get_columns_in_relation(source_relation) %}
  
    {% set target_model_name = target_table |string %}
    {%- set target_relation = adapter.Relation.create(identifier=target_table,schema=target_schema) -%}


    {% set target_columns = adapter.get_columns_in_relation(target_relation) %}


    {%- set source_column_list = [] -%}
    {%- for column in source_columns -%}
        {%- do source_column_list.append(column.column ) -%}
    {%- endfor -%}

    {%- set target_column_list = [] -%}
   
    {%- for trg_column in target_columns -%}
        
        

        {%- if trg_column.dtype in ["FLOAT","INTEGER","BIGINT","SMALLINT","NUMBER","DECIMAL"]-%}
            {%- if trg_column.column in source_column_list -%} 
              {%- do target_column_list.append("COALESCE (" ~trg_column.column ~"::"~ trg_column.dtype ~", 0 ) as "~ trg_column.column) -%}
            {%- else -%}
              {%- do target_column_list.append("0 as " ~trg_column.column) -%}
            {% endif%}

        {%- elif trg_column.dtype in ["STRING","VARCHAR","TEXT","CHAR"]-%}
          {%- if trg_column.column in source_column_list -%} 
              {%- do target_column_list.append("COALESCE (" ~trg_column.column ~"::"~ trg_column.dtype ~", 'N/A' ) as "~ trg_column.column) -%}
            {%- else -%}
              {%- do target_column_list.append(" 'N/A' as " ~trg_column.column) -%}
            {% endif%}

        {%- elif trg_column.dtype in ["DATETIME","TIMESTAMP"]-%}
          {%- if trg_column.column in source_column_list -%} 
              {%- do target_column_list.append("COALESCE (" ~trg_column.column ~"::"~ trg_column.dtype ~", '1900-01-01 00:00:00' ) as "~ trg_column.column) -%}
            {%- else -%}
              {%- do target_column_list.append(" '1900-01-01 00:00:00' as " ~trg_column.column) -%}
            {% endif%}

        {%- elif trg_column.dtype == "DATE"-%}
          {%- if trg_column.column in source_column_list -%} 
              {%- do target_column_list.append("COALESCE (" ~trg_column.column ~"::"~ trg_column.dtype ~", '1900-01-01' ) as "~ trg_column.column) -%}
            {%- else -%}
              {%- do target_column_list.append(" '1900-01-01' as " ~trg_column.column) -%}
            {% endif%}

        {%- elif trg_column.dtype == "TIME"-%}
           {%- if trg_column.column in source_column_list -%} 
              {%- do target_column_list.append("COALESCE (" ~trg_column.column ~"::"~ trg_column.dtype ~", '00:00:00' ) as "~ trg_column.column) -%}
            {%- else -%}
              {%- do target_column_list.append(" '00:00:00' as " ~trg_column.column) -%}
            {% endif%}
        {%- endif -%}
        
    {%- endfor -%}

{{target_column_list | join(', ')}}


{%- endmacro %}