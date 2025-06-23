{% macro generate_schema_name(custom_schema_name, node) -%}

    {# ALWAYS use clean schema names - no prefixes! #}
    {%- if custom_schema_name is none -%}
        {# If no custom schema, use the default from profiles.yml #}
        {{ target.schema }}
    {%- else -%}
        {# Always use the custom schema name in UPPERCASE #}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}

{%- endmacro %}