{#
    Use a model's custom schema as-is ("staging"), instead of dbt's default
    "<target schema>_<custom schema>" ("main_staging").
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
