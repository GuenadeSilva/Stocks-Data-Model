{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
      Prevent dbt from concatenating target.dataset with schema folder.
      If a schema is defined (e.g. staging/intermediate/marts), use it directly.
      Otherwise, fall back to the target dataset.
      More info on that here -> https://www.youtube.com/watch?v=AvrVQr5FHwk
    #}
    {{ custom_schema_name }}
{%- endmacro %}
