-- Every RESTAURANT_UPDATED event: the initial SNAPSHOT a run emits for each restaurant, then
-- individual UPDATEs (rating, prep time, open/closed).
SELECT
    EVENT_ID,
    EVENT_TS AS CHANGED_AT,
    ENTITY_ID AS RESTAURANT_ID,
    PAYLOAD:name::STRING AS RESTAURANT_NAME,
    PAYLOAD:zone_id::STRING AS ZONE_ID,
    PAYLOAD:h3_r8::STRING AS H3_R8,
    PAYLOAD:lat::FLOAT AS LAT,
    PAYLOAD:lng::FLOAT AS LNG,
    PAYLOAD:cuisine::STRING AS CUISINE,
    PAYLOAD:price_band::NUMBER AS PRICE_BAND,
    PAYLOAD:avg_prep_min::FLOAT AS AVG_PREP_MIN,
    PAYLOAD:rating::FLOAT AS RATING,
    PAYLOAD:is_active::BOOLEAN AS IS_ACTIVE,
    PAYLOAD:change_type::STRING AS CHANGE_TYPE,
    ARRAY_TO_STRING(PAYLOAD:changed_fields, ',') AS CHANGED_FIELDS
FROM {{ source('silver', 'REFERENCE_EVENTS') }}
WHERE EVENT_TYPE = 'RESTAURANT_UPDATED'
