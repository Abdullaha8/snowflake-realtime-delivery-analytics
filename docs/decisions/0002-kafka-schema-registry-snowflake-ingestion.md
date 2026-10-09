# ADR 0002: Kafka topics, Schema Registry and Snowflake ingestion

- **Status:** Accepted (Phase 2, 2026-10-10)
- **Scope:** `infra/`, `simulator/avro.py`, `simulator/registry.py`, `snowflake/01-04`, `scripts/`

## Context

P2 moves events from the simulator into Snowflake BRONZE. It has to do this:

- reliably, with no silent loss, and with duplicates kept for P3 to remove
- under an enforced schema contract
- at near-zero cost on a trial account
- in a way that can be explained in an interview

## Decisions

### 1. Snowflake Connector for Kafka **v4**, pinned to 4.1.0
v4 runs on high-performance Snowpipe Streaming:
- 5–10 s latency.
- Billed per GB ingested, with no warehouse involved.
- Snowflake recommends it for new builds; v3 is planned for deprecation.

4.2.x was released less than two weeks before this decision, so 4.1.0 is pinned.

The Connect image is `cp-kafka-connect:7.8.2`, which Snowflake tests against. The broker,
Schema Registry and Connect all run Confluent 7.8.2, because the docs ask for matching broker and
Connect versions.

*Rejected:*
- v3 (Snowpipe micro-batches): higher latency, and it's on its way out.
- Snowpipe from S3: file-based with minute-level latency, and it isn't streaming.

### 2. Avro + Confluent Schema Registry, one record per event type
- The v4 docs list only `AvroConverter` for schema-registry-backed data. `JsonSchemaConverter`
  isn't documented, so I didn't rely on it.
- Each event type gets its own Avro record (11 total), registered under
  **TopicRecordNameStrategy** (`<topic>-com.fooddel.events.<Record>`). This lets multiple event
  types share the three topics from ADR 0001.
  *Rejected:* one Avro union per topic. Connect turns unions into awkward
  `io.confluent.connect.avro.Union` structs, which would end up in BRONZE.
- Schemas are **generated from the pydantic models**, which stay the single source of truth.
  - The `.avsc` files are committed.
  - A unit test fails if they go stale.
  - Another test round-trips one event of every type through Avro.
- Strings stay strings: timestamps are ISO-8601 and enums are plain strings, since Avro enums
  are painful to evolve. As a result, a record decoded by the AvroConverter produces the
  **same RECORD_CONTENT JSON** as a COPY-loaded JSONL line.

### 3. The registry is a gate, not documentation
- Compatibility is **BACKWARD** per subject: new readers must be able to read old data.
  - Adding an optional field with a default passes.
  - Adding a required field is rejected.
  - Both cases are covered by integration tests.
- The producer runs with `auto.register.schemas=false`. Schemas are registered deliberately with
  `make schemas-register`, and CI can run `make schemas-check` first. A producer with an
  unregistered schema fails before anything reaches Kafka; this is also tested.

### 4. Explicit topics, auto-create off
- A one-shot `kafka-init` container creates the topics and re-applies their config, so it's
  idempotent.
- The broker has `auto.create.topics.enable=false`, so producing to a typo'd topic fails
  (tested).

| Topic | Partitions | Retention | Note |
|---|---|---|---|
| `fooddel.orders.v1` | 3 | 7 d | Replay window for lifecycle |
| `fooddel.driver_locations.v1` | 3 | 1 d | ~95% of volume, only useful fresh |
| `fooddel.reference.v1` | 3 | 7 d | `delete`, **not** `compact` (see below) |
| `fooddel.dlq.v1` | 1 | 7 d | Records the sink could not convert |

The reference topic uses `delete` because compaction would keep only the latest version of
each restaurant and erase the history that P4's SCD2 needs.

Replication factor is 1 because there's a single local broker. Production would use RF=3 with
`min.insync.replicas=2`.

### 5. BRONZE = raw VARIANT, pre-created, duplicates kept
- `snowflake.enable.schematization=false` gives two columns: `RECORD_METADATA`
  (topic/partition/offset/key/CreateTime/SnowflakeConnectorPushTime) and `RECORD_CONTENT`.
- Tables are created by `03_bronze.sql` with `autocreate.table.type=none`, so their definition
  lives in git.
- `CHANGE_TRACKING=TRUE` is set up front for the P3 Dynamic Tables.
- No dedup or typing happens in BRONZE; that's P3's job. BRONZE stays a faithful, replayable
  copy of what arrived.

*Rejected:* schematized BRONZE (one column per field). Every schema change would alter the
landing table, and the nested `data` payload doesn't flatten cleanly.

### 6. Backfill is bulk-loaded, not replayed through Kafka
`make load-backfill` PUTs the ~100 days of gzipped JSONL to an internal stage, then runs
`COPY INTO` the same `*_RAW` tables, tagging rows `RECORD_METADATA.source = 'backfill'`.
- COPY load metadata skips files that were already loaded, so re-running is safe.
- Replaying 1.3 M events through Kafka would cost more ingestion and add nothing.

### 7. Security and least privilege
- **Connector user:** `SVC_KAFKA_CONNECTOR` is `TYPE=SERVICE` and authenticates with a key
  pair only (no password, no UI login). Its role `FOODDEL_INGEST` has exactly what the v4 docs
  require: USAGE on the database and schema, `CREATE PIPE` on BRONZE (default pipe mode), and
  `INSERT` on the BRONZE tables.
- **Other roles:** `FOODDEL_TRANSFORM` and `FOODDEL_BI` are created now, so P3 and P8 inherit
  the model. All roles roll up to SYSADMIN.
- **Private key:** it stays out of git, the connector JSON, and Connect's config topic. The
  worker resolves `${env:SNOWFLAKE_CONNECTOR_PRIVATE_KEY}` at runtime through
  `EnvVarConfigProvider`, from `.env`.

### 8. Cost controls
- Warehouses are XSMALL with `AUTO_SUSPEND=60`, created `INITIALLY_SUSPENDED`.
- Resource monitors: 5 credits/month per warehouse, 20 account-wide. Each notifies, then
  suspends, then suspends immediately.
- Resource monitors don't cover serverless Snowpipe Streaming. That cost is per GB: ~15 MB/hour
  of live traffic, and it only runs while the connector is deployed.
- `make stop` deletes the connector, stops the containers and suspends the warehouses.
  `make up` never starts the connector by itself.

## Trade-offs
- **Avro vs JSON.** The cost is a generation step and a binary wire format: kafka-ui decodes it
  through the registry, but `kafka-console-consumer` won't. In return we get a supported
  converter, compact messages and an enforced contract.
- **Single broker and RF=1.** Fine for local development, but it can't demonstrate broker
  failover.
- **Kafka Connect runs on a laptop**, not near the Snowflake region, which the docs recommend.
  Latency figures include that network hop.
- **The DLQ only catches conversion errors** (e.g. an unknown schema id). Rows that Snowflake
  rejects are handled by v4's server-side validation and error tables, which are a P5 data
  quality topic.
