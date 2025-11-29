{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {# 1. DEV: Flatten everything to the default dataset #}
    {%- if target.name == 'dev' -%}
        {{ default_schema }}

    {# 2. CI: Concatenate (Standard dbt behavior) #}
    {#    This ensures dbt_ci_123 + staging = dbt_ci_123_staging #}
    {%- elif target.name == 'ci' -%}

        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}

    {# 3. PROD: Clean names (Override default behavior) #}
    {#    This gives you just 'staging' or 'marts' #}
    {%- else -%}

        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
