{% macro incremental_changes_condition( source_table_time_col, current_table_time_col2 ) -%}

    {{ source_table_time_col }} > (select coalesce(max({{ current_table_time_col2 }}), '1980-01-01') from {{ this }})

{%- endmacro %}
