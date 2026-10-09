{#-
    Use the configured schema name as-is (MARTS, STAGING) instead of dbt's default
    "<target_schema>_<custom_schema>" (which would give MARTS_STAGING).
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim | upper }}
{%- endmacro %}
