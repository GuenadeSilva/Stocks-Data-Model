{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {# 1. In DEV: Ignore custom schemas entirely. #}
    {#    Everything goes to 'stocks_model_dev' defined in profiles.yml #}
    {%- if target.name == 'dev' -%}

        {{ default_schema }}

    {# 2. In PROD: Use your custom schema (e.g., 'staging') directly. #}
    {#    This keeps the 'clean' names you wanted from the video. #}
    {%- else -%}

        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
