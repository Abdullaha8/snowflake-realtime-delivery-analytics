{#-
    Governance for the MARTS_SECURE secure views (Standard edition: no masking / row access
    policies, so the rules live in the views).

    Clear-text PII: FOODDEL_PII_READER (and FOODDEL_TRANSFORM, the engineers who build it).
    All zones:      FOODDEL_BI (inherited by PII_READER) and FOODDEL_TRANSFORM.
    Zone managers:  only the zones mapped to their CURRENT_ROLE() in GOVERNANCE.ZONE_ACCESS.

    IS_ROLE_IN_SESSION follows the role hierarchy and secondary roles; test with
    `USE SECONDARY ROLES NONE` (scripts/governance_check.py does).
-#}

{% macro can_see_pii() -%}
    (IS_ROLE_IN_SESSION('FOODDEL_PII_READER') OR IS_ROLE_IN_SESSION('FOODDEL_TRANSFORM'))
{%- endmacro %}

{% macro mask_email(col) -%}
    IFF({{ can_see_pii() }}, {{ col }}, LEFT({{ col }}, 1) || '***@' || SPLIT_PART({{ col }}, '@', 2))
{%- endmacro %}

{% macro mask_phone(col) -%}
    IFF({{ can_see_pii() }}, {{ col }}, REPEAT('*', GREATEST(LENGTH({{ col }}) - 4, 0)) || RIGHT({{ col }}, 4))
{%- endmacro %}

{% macro mask_name(col) -%}
    IFF({{ can_see_pii() }}, {{ col }}, LEFT({{ col }}, 1) || '***')
{%- endmacro %}

{% macro zone_filter(col='ZONE_ID') -%}
    (
        IS_ROLE_IN_SESSION('FOODDEL_BI')
        OR IS_ROLE_IN_SESSION('FOODDEL_TRANSFORM')
        OR {{ col }} IN (
            SELECT ZONE_ID FROM {{ target.database }}.GOVERNANCE.ZONE_ACCESS
            WHERE ROLE_NAME = CURRENT_ROLE()
        )
    )
{%- endmacro %}
