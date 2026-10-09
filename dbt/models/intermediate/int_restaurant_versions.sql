-- Event-sourced SCD2: one row per distinct restaurant state, valid until the next change.
--
-- Why not only a dbt snapshot? A snapshot records state each time dbt runs, so loading 100
-- days of history in one go would collapse every change into a single version. Our source is
-- an event log that already timestamps each change, so history comes from the events.
--
-- Each simulator run re-announces every restaurant (change_type = SNAPSHOT) with its baseline
-- state. Consecutive identical states are collapsed, so a re-announcement only becomes a new
-- version if something actually changed.
WITH CHANGES AS (
    SELECT
        *,
        HASH(RESTAURANT_NAME, ZONE_ID, CUISINE, PRICE_BAND, AVG_PREP_MIN, RATING, IS_ACTIVE) AS STATE_HASH
    FROM {{ ref('stg_restaurant_changes') }}
),

STATE_CHANGES AS (
    SELECT *
    FROM CHANGES
    QUALIFY
        LAG(STATE_HASH) OVER (PARTITION BY RESTAURANT_ID ORDER BY CHANGED_AT, EVENT_ID)
            IS DISTINCT FROM STATE_HASH
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['RESTAURANT_ID', 'CHANGED_AT']) }} AS RESTAURANT_SK,
    RESTAURANT_ID,
    RESTAURANT_NAME,
    ZONE_ID,
    H3_R8,
    LAT,
    LNG,
    CUISINE,
    PRICE_BAND,
    AVG_PREP_MIN,
    RATING,
    IS_ACTIVE,
    CHANGED_FIELDS,
    ROW_NUMBER() OVER (PARTITION BY RESTAURANT_ID ORDER BY CHANGED_AT) AS VERSION_NUMBER,
    -- The first version is open-ended to the past so every order finds a version.
    IFF(
        ROW_NUMBER() OVER (PARTITION BY RESTAURANT_ID ORDER BY CHANGED_AT) = 1,
        '1900-01-01'::TIMESTAMP_NTZ,
        CHANGED_AT
    ) AS VALID_FROM,
    LEAD(CHANGED_AT) OVER (PARTITION BY RESTAURANT_ID ORDER BY CHANGED_AT) AS VALID_TO,
    VALID_TO IS NULL AS IS_CURRENT,
    CHANGED_AT AS SOURCE_CHANGED_AT
FROM STATE_CHANGES
