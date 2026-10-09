# Interview Q&A

## P1: Simulator

**Why build a simulator instead of using a public dataset?**
Public delivery datasets are static CSVs. I needed a *stream* that contains the problems a
real pipeline faces: late, duplicated and out-of-order events. I also needed a ground-truth
record of exactly which events were tampered with, so I can prove the pipeline handles them.

**How do you generate realistic out-of-order events?**
Random jitter of a few seconds almost never reorders lifecycle events that are minutes apart.
Instead, I hold a chosen event until the next event with the same Kafka key is sent, then send
it right after. That guarantees a real inversion, e.g. `DELIVERED` arriving before
`PICKED_UP`.

**How do you know your dedup or late-data handling works?**
Every injected anomaly is logged to a manifest (`event_id`, type, delay) and never flagged in
the event itself. Tests compare the observed stream to the manifest: duplicate copies equal
manifest duplicates exactly, and late and out-of-order rates match the configuration within
tolerance. Downstream DQ checks can be scored the same way.

**Why two timestamps?**
`event_ts` is when it happened (business logic, windows, SLA). `emitted_ts` is when the
producer sent it. The gap between them is the lateness, and the file partitions are by
`emitted_ts`, like ingestion-time partitions.

**Why H3?**
It gives uniform hexagonal cells with cheap neighbour queries (`grid_ring`) for driver
search, and Snowflake has native H3 functions. Zones are named groups of res-8 cells, so
dashboards can say "Koregaon Park" rather than a cell ID.

**Why is the promised ETA deliberately naive?**
It models today's rule-based ETA. It knows traffic but ignores weather and kitchen load, which
gives a 30% breach rate in rain vs 17% in clear weather. That's the gap the P6 ML model has to
close.

**How is it reproducible?**
A fixed `world_seed` keeps restaurants, customers and drivers the same in every run. A run
`seed` controls traffic. No set iteration happens anywhere, so the same seed gives
byte-identical output even across Python hash seeds; a test checks this.

## P2: Kafka → Snowflake BRONZE

**Why connector v4 / Snowpipe Streaming instead of Snowpipe on S3 files?**
Streaming gives rows in about 5–10 s instead of minutes. It's billed per GB, with no warehouse
and no file sizing to tune. v4 is Snowflake's recommended version for new builds; v3 is being
retired.

**Why Avro and not JSON?**
The v4 docs support schema-registry data only through the Avro converter. Avro also gives
compact messages and an enforced contract. I keep timestamps and enums as strings, so the
decoded record is byte-for-byte the same JSON as my backfill files. That means BRONZE looks the
same however a row arrived.

**How do several event types share one topic with Avro?**
TopicRecordNameStrategy: each event type is its own record with its own subject and its own
compatibility history. A union per topic would turn into an ugly `Union` struct in Snowflake.

**What does BACKWARD compatibility mean here?**
Consumers using the new schema must be able to read old data. So I can add an optional field
with a default, but adding a required field is rejected. I have integration tests for both
cases. The producer can't auto-register schemas, so an unreviewed schema change fails at the
producer instead of reaching Snowflake.

**How do you prove nothing was lost between Kafka and Snowflake?**
Every BRONZE row carries its Kafka topic, partition and offset. For each partition, offsets
must be contiguous (`max - min + 1 = count distinct`) and must match Kafka's end offset.
Duplicates are expected at ~2%, because I injected them, and SILVER removes them by `event_id`.

**Why keep duplicates in BRONZE?**
BRONZE is a faithful, replayable record of what arrived. Dedup is business logic: which copy
wins, and how late is too late. That belongs in SILVER, where it's versioned and testable.

**How is the connector's private key protected?**
- It never lives in git, in the connector JSON, or in Connect's config topic.
- Kafka Connect's `EnvVarConfigProvider` resolves `${env:...}` inside the worker at runtime.
- The Snowflake user is `TYPE=SERVICE` with key-pair auth only.
- Its role can only INSERT into BRONZE (plus CREATE PIPE, which the docs require for the
  default pipe).

**How do you control cost?**
- XSMALL warehouses with 60 s auto-suspend, created suspended.
- Resource monitors per warehouse and for the account. These don't cover serverless
  ingestion, which is per GB and tiny for this volume.
- The connector is never auto-started, and `make stop` tears everything down.

**Tell me about a bug you hit.**
On the first Snowflake run, the connector said RUNNING but BRONZE had 0 rows. The producer
registered schemas per record type (TopicRecordNameStrategy), but Connect's Avro converter
defaults to per-topic subjects. Every record failed to deserialize, and because
`errors.tolerance=all`, all 36 k records quietly went to the dead-letter queue. I fixed the
converter config. Then I added a local integration test that runs the real converter settings
through a file sink, which fails on the old config and passes on the new one. I also made the
status command surface DLQ depth. The lesson: error tolerance without monitoring is data loss
you don't see.

## P3: SILVER/GOLD Dynamic Tables

**Why Dynamic Tables instead of Streams + Tasks?**
The logic is declarative SQL, and Snowflake handles incremental refresh, dependency ordering
and scheduling. With Streams + Tasks I'd hand-write MERGEs and own their ordering and retries.
I set `REFRESH_MODE = INCREMENTAL` explicitly, so if a query can't refresh incrementally,
`CREATE` fails. Otherwise it would silently become a full refresh and cost far more.

**How do you handle duplicates, late and out-of-order events?**
- **Duplicates:** `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ...) = 1`.
- **Out-of-order:** order status comes from *which* events exist, using a fixed precedence,
  never from arrival order.
- **Late:** there's no watermark. Every refresh folds new BRONZE rows into the affected
  orders and buckets, so a 6-hour-late event still corrects the numbers.

**Why is "orders at risk" a view and not a Dynamic Table?**
It depends on the current time, which an incremental Dynamic Table can't maintain. In the
view, "now" is stream time (the latest event seen), because my simulated runs replay past
time. In production it would be `CURRENT_TIMESTAMP()`.

**How do you know your GOLD numbers are right?**
I reconcile against ground truth. One seeded run goes to Kafka while the same events stay in
memory. Python computes the expected numbers independently, then I compare 37 metrics in
Snowflake: duplicates removed = duplicates injected, every order status and cancel reason,
SLA breaches, GMV to the rupee, per-zone counts, and every GOLD table summing back to SILVER.
37/37 match. Data-quality checks (no impossible lifecycles, H3 agreement) are all 0.

**Explain TARGET_LAG = DOWNSTREAM.**
The table refreshes only when a table that reads it needs fresh data, so SILVER never costs
more than GOLD's lag. Gotcha: a DOWNSTREAM table that nothing reads *never* refreshes. I hit
that with REFERENCE_EVENTS, which has no consumer until P4, so it has its own lag for now.

**How do you control Dynamic Table cost?**
- An XSMALL warehouse.
- A 10-minute lag by default, with 1 minute only for demos.
- Refreshes are skipped when the base tables haven't changed.
- `make stop` suspends every Dynamic Table before suspending the warehouses.
- The initial build plus a full reconciliation cost well under 0.1 credits.
