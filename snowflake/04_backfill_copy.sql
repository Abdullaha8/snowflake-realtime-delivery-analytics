-- 04_backfill_copy.sql: load staged backfill files into the BRONZE tables.
-- Run by `make load-backfill` after it PUTs data/backfill to @BACKFILL_STAGE.
-- Safe to re-run: COPY load metadata skips files already loaded (for 64 days).
-- RECORD_METADATA marks these rows as backfill, so they can be told apart from Kafka rows.

USE ROLE SYSADMIN;
USE WAREHOUSE WH_INGEST;
USE SCHEMA ${SNOWFLAKE_DATABASE}.BRONZE;

COPY INTO ORDERS_RAW (RECORD_CONTENT, RECORD_METADATA)
FROM (
    SELECT
        $1 AS RECORD_CONTENT,
        OBJECT_CONSTRUCT(
            'source', 'backfill',
            'topic', 'fooddel.orders.v1',
            'file', METADATA$FILENAME,
            'file_row', METADATA$FILE_ROW_NUMBER
        ) AS RECORD_METADATA
    FROM @BACKFILL_STAGE/fooddel.orders.v1/
)
FILE_FORMAT = (FORMAT_NAME = FF_JSONL)
ON_ERROR = ABORT_STATEMENT;

COPY INTO REFERENCE_RAW (RECORD_CONTENT, RECORD_METADATA)
FROM (
    SELECT
        $1 AS RECORD_CONTENT,
        OBJECT_CONSTRUCT(
            'source', 'backfill',
            'topic', 'fooddel.reference.v1',
            'file', METADATA$FILENAME,
            'file_row', METADATA$FILE_ROW_NUMBER
        ) AS RECORD_METADATA
    FROM @BACKFILL_STAGE/fooddel.reference.v1/
)
FILE_FORMAT = (FORMAT_NAME = FF_JSONL)
ON_ERROR = ABORT_STATEMENT;

-- Backfill runs without driver pings by default; this loads them only if they were generated.
COPY INTO DRIVER_LOCATIONS_RAW (RECORD_CONTENT, RECORD_METADATA)
FROM (
    SELECT
        $1 AS RECORD_CONTENT,
        OBJECT_CONSTRUCT(
            'source', 'backfill',
            'topic', 'fooddel.driver_locations.v1',
            'file', METADATA$FILENAME,
            'file_row', METADATA$FILE_ROW_NUMBER
        ) AS RECORD_METADATA
    FROM @BACKFILL_STAGE/fooddel.driver_locations.v1/
)
FILE_FORMAT = (FORMAT_NAME = FF_JSONL)
ON_ERROR = ABORT_STATEMENT;
