-- The 12 Pune delivery zones (seed generated from simulator/data/pune_zones.yaml).
SELECT
    ZONE_ID,
    ZONE_NAME,
    ZONE_PROFILE,
    DEMAND_WEIGHT,
    CENTER_LAT,
    CENTER_LNG,
    H3_CENTER_R8,
    H3_CELL_COUNT
FROM {{ ref('zones') }}
