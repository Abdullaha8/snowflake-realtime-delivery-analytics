-- {{ ref('mart_restaurant_daily') }} filtered to the zones the reader may see (macros/governance.sql).
SELECT *
FROM {{ ref('mart_restaurant_daily') }}
WHERE {{ zone_filter('ZONE_ID') }}
