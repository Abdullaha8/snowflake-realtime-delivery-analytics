-- City-wide weather changes (CLEAR <-> RAIN), as intervals: valid until the next change.
SELECT
    EVENT_TS AS VALID_FROM,
    LEAD(EVENT_TS) OVER (ORDER BY EVENT_TS) AS VALID_TO,
    PAYLOAD:condition::STRING AS WEATHER_CONDITION,
    PAYLOAD:intensity::STRING AS RAIN_INTENSITY,
    PAYLOAD:demand_multiplier::FLOAT AS DEMAND_MULTIPLIER,
    PAYLOAD:supply_multiplier::FLOAT AS SUPPLY_MULTIPLIER,
    PAYLOAD:travel_time_multiplier::FLOAT AS TRAVEL_TIME_MULTIPLIER
FROM {{ source('silver', 'REFERENCE_EVENTS') }}
WHERE EVENT_TYPE = 'WEATHER_UPDATED'
