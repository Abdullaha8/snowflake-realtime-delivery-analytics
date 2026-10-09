-- 90_verify_bronze.sql: is BRONZE complete and fresh? Run with `make sf-verify`.
-- Uses WH_INGEST for a few seconds (auto-suspends after 60 s).

USE ROLE SYSADMIN;
USE WAREHOUSE WH_INGEST;
USE SCHEMA ${SNOWFLAKE_DATABASE}.BRONZE;

-- 1. Rows per table and source. For a single run, duplicate_rows ~2% is expected (injected).
--    Re-running the simulator with the same --seed replays the same event_ids, which also
--    shows up here as duplicates.
WITH ALL_ROWS AS (
    SELECT
        'ORDERS_RAW' AS TABLE_NAME,
        RECORD_METADATA,
        RECORD_CONTENT
    FROM ORDERS_RAW
    UNION ALL
    SELECT
        'DRIVER_LOCATIONS_RAW' AS TABLE_NAME,
        RECORD_METADATA,
        RECORD_CONTENT
    FROM DRIVER_LOCATIONS_RAW
    UNION ALL
    SELECT
        'REFERENCE_RAW' AS TABLE_NAME,
        RECORD_METADATA,
        RECORD_CONTENT
    FROM REFERENCE_RAW
)

SELECT
    TABLE_NAME,
    COALESCE(RECORD_METADATA:source::STRING, 'kafka') AS SOURCE,
    COUNT(*) AS ROW_COUNT,
    COUNT(DISTINCT RECORD_CONTENT:event_id::STRING) AS UNIQUE_EVENTS,
    COUNT(*) - COUNT(DISTINCT RECORD_CONTENT:event_id::STRING) AS DUPLICATE_ROWS
FROM ALL_ROWS
GROUP BY 1, 2
ORDER BY 1, 2;

-- 2. No loss: per topic-partition, Kafka offsets must be contiguous (gaps = 0).
--    Compare max_offset + 1 with the topic's end offset from `make topics`.
WITH KAFKA_ROWS AS (
    SELECT RECORD_METADATA FROM ORDERS_RAW
    UNION ALL
    SELECT RECORD_METADATA FROM DRIVER_LOCATIONS_RAW
    UNION ALL
    SELECT RECORD_METADATA FROM REFERENCE_RAW
)

SELECT
    RECORD_METADATA:topic::STRING AS TOPIC,
    RECORD_METADATA:partition::NUMBER AS KAFKA_PARTITION,
    MIN(RECORD_METADATA:offset::NUMBER) AS MIN_OFFSET,
    MAX(RECORD_METADATA:offset::NUMBER) AS MAX_OFFSET,
    COUNT(DISTINCT RECORD_METADATA:offset::NUMBER) AS DISTINCT_OFFSETS,
    MAX(RECORD_METADATA:offset::NUMBER) - MIN(RECORD_METADATA:offset::NUMBER) + 1
    - COUNT(DISTINCT RECORD_METADATA:offset::NUMBER) AS GAPS
FROM KAFKA_ROWS
WHERE RECORD_METADATA:topic IS NOT NULL AND RECORD_METADATA:source IS NULL
GROUP BY 1, 2
ORDER BY 1, 2;

-- 3. Freshness: Kafka append -> pushed into the connector's ingest buffer (seconds), only for
--    records produced after the connector started consuming (its first push). A backlog that
--    sat in Kafka before deployment would otherwise dominate. Snowflake commit adds a few
--    seconds on top.
WITH RECENT AS (
    SELECT O.RECORD_METADATA
    FROM ORDERS_RAW AS O
    WHERE
        O.RECORD_METADATA:SnowflakeConnectorPushTime IS NOT NULL
        AND O.RECORD_METADATA:CreateTime::NUMBER >= (
            SELECT MIN(L.RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER)
            FROM ORDERS_RAW AS L
        )
)

SELECT
    COUNT(*) AS KAFKA_ROWS,
    MEDIAN(
        (RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER - RECORD_METADATA:CreateTime::NUMBER)
        / 1000
    ) AS P50_KAFKA_TO_PUSH_S,
    APPROX_PERCENTILE(
        (RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER - RECORD_METADATA:CreateTime::NUMBER)
        / 1000,
        0.95
    ) AS P95_KAFKA_TO_PUSH_S,
    MAX(TO_TIMESTAMP_LTZ(RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER, 3)) AS LAST_PUSH
FROM RECENT;

-- 4. Event mix on the orders topic.
SELECT
    RECORD_CONTENT:event_type::STRING AS EVENT_TYPE,
    COUNT(*) AS ROW_COUNT
FROM ORDERS_RAW
GROUP BY 1
ORDER BY 2 DESC;

-- 5. One streamed row, to eyeball the shape.
SELECT
    RECORD_METADATA,
    RECORD_CONTENT
FROM ORDERS_RAW
WHERE RECORD_METADATA:topic IS NOT NULL AND RECORD_METADATA:source IS NULL
ORDER BY RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER DESC
LIMIT 1;
