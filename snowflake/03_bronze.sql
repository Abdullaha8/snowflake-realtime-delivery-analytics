-- 03_bronze.sql: BRONZE landing tables (one per Kafka topic) + the backfill stage.
-- Tables are pre-created (connector setting snowflake.autocreate.table.type=none) so their
-- definition lives in git, not in whatever the connector decided to create.
-- Shape = connector v4 with schematization OFF: RECORD_METADATA + RECORD_CONTENT, both VARIANT.
-- Kafka-streamed (Avro, decoded by the AvroConverter) and COPY-loaded (JSONL backfill) rows
-- have identical RECORD_CONTENT. Duplicates are kept on purpose: dedup happens in SILVER (P3).

USE ROLE SYSADMIN;
USE SCHEMA ${SNOWFLAKE_DATABASE}.BRONZE;

CREATE TABLE IF NOT EXISTS ORDERS_RAW (
    RECORD_METADATA VARIANT COMMENT 'Kafka: topic, partition, offset, key, CreateTime, SnowflakeConnectorPushTime. Backfill: source, file, file_row',
    RECORD_CONTENT VARIANT COMMENT 'Event envelope + data, exactly as produced'
)
CHANGE_TRACKING = TRUE
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Topic fooddel.orders.v1: order lifecycle events';

CREATE TABLE IF NOT EXISTS DRIVER_LOCATIONS_RAW (
    RECORD_METADATA VARIANT COMMENT 'Kafka metadata (see ORDERS_RAW)',
    RECORD_CONTENT VARIANT COMMENT 'Event envelope + data, exactly as produced'
)
CHANGE_TRACKING = TRUE
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Topic fooddel.driver_locations.v1: driver GPS pings every 30 s';

CREATE TABLE IF NOT EXISTS REFERENCE_RAW (
    RECORD_METADATA VARIANT COMMENT 'Kafka metadata (see ORDERS_RAW)',
    RECORD_CONTENT VARIANT COMMENT 'Event envelope + data, exactly as produced'
)
CHANGE_TRACKING = TRUE
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Topic fooddel.reference.v1: restaurant updates, customer signups, weather';

-- CHANGE_TRACKING lets P3 Dynamic Tables refresh incrementally. Re-applied for idempotency.
ALTER TABLE ORDERS_RAW SET CHANGE_TRACKING = TRUE;
ALTER TABLE DRIVER_LOCATIONS_RAW SET CHANGE_TRACKING = TRUE;
ALTER TABLE REFERENCE_RAW SET CHANGE_TRACKING = TRUE;

-- Backfill (history since launch) is bulk-loaded instead of replayed through Kafka.
CREATE FILE FORMAT IF NOT EXISTS FF_JSONL
TYPE = JSON
COMPRESSION = AUTO
COMMENT = 'One JSON event per line, optionally gzipped';

CREATE STAGE IF NOT EXISTS BACKFILL_STAGE
    FILE_FORMAT = FF_JSONL
    COMMENT = 'Simulator backfill files: <topic>/dt=YYYY-MM-DD/part-<run_id>.jsonl.gz';
