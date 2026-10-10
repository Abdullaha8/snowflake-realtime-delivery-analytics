-- {{ ref('mart_daily_kpis') }} filtered to the zones the reader may see (macros/governance.sql).
SELECT *
FROM {{ ref('mart_daily_kpis') }}
WHERE {{ zone_filter('ZONE_ID') }}
