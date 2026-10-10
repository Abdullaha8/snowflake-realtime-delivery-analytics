-- {{ ref('fct_orders') }} filtered to the zones the reader may see (macros/governance.sql).
SELECT *
FROM {{ ref('fct_orders') }}
WHERE {{ zone_filter('ZONE_ID') }}
