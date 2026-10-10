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

## P4: dbt historical layer

**Why does dbt read SILVER instead of BRONZE?**
Dedup and order lifecycle are solved and reconciled once, in SILVER. If dbt re-implemented
them, I'd have two versions of the truth that could drift. dbt does what it's best at:
dimensional modelling, history, tests and documentation.

**How did you build SCD2?**
From the event log. Each RESTAURANT_UPDATED event starts a version: `valid_from` is the
change time and `valid_to` is the next change (`LEAD`). Consecutive identical states are
collapsed. A dbt snapshot records state *when dbt runs*, so loading 100 days at once would
have squashed every change into one version. I still include a check-strategy snapshot,
because that's the right tool when a source only exposes current state. Facts join
point-in-time, so an order placed before a rating change sees the old rating.

**How does your incremental model handle late data?**
`fct_orders` merges on `order_id` and re-processes every order whose latest event was emitted
within 24 h of the newest one loaded. Late events arrive up to 6 h late, so 24 h covers them
with margin. After a live session, Snowflake reported 7 rows inserted and 2,414 updated:
exactly the new orders plus the lookback window. A backfill of *older* dates needs
`--full-refresh`, and I documented that in the model.

**How do you know the marts are right?**
- 39 dbt tests: keys, relationships, accepted values, a money identity, SCD2 integrity, marts
  adding back up to the fact table, cohort month 0 = 100%.
- A reconciliation that recomputes 35 metrics from the raw backfill files in plain Python:
  orders, GMV, AOV, cancel reasons, SLA breaches, per month, per zone, cohort sizes,
  month+1 retention. 35/35 match.

**What's your cohort definition?**
The IST month of a customer's first order, not the signup month. Retention in month N means
at least one order N months after the cohort month. Using order activity avoids counting
people who signed up and never ordered.

## P5: Orchestration, data quality, alerts, latency

**Walk me through your DAG.**
`check_new_data → refresh_dynamic_tables → dbt_build → data_quality → record_latency →
record_run`. The first task compares BRONZE's row count, read from metadata with `SHOW TABLES`
(no warehouse), with the count at the last successful run. If nothing changed, the run
short-circuits and an idle hour costs zero credits. A `force` parameter covers recovery runs,
because a count can't see a fix that deletes rows.

**Why doesn't the data-quality task retry?**
Retries are for transient problems like network blips or warehouse hiccups. Re-running the
same checks on the same data gives the same answer, so the task fails fast with
`AirflowFailException`. `record_run` uses `trigger_rule=all_done`, so even failed runs are
recorded in `OPS.PIPELINE_RUNS`.

**How do you do data quality, and why store the results?**
Seven SQL checks, four critical and three warnings, defined as plain data and unit-tested.
Every result from every run goes to `OPS.DQ_RESULTS`. That gives trends for dashboards, an
audit trail, and the input for a Snowflake Alert. I added a minimum sample size after a 13-row
window produced a meaningless 7.7% duplicate warning.

**How does alerting work?**
- Three serverless Snowflake Alerts: a DQ failure, an SLA breach spike, ingest latency.
- They're created suspended and resumed only for demos, because they bill per evaluation.
- Each alert calls one procedure that logs to `OPS.ALERT_LOG` and emails if configured; an
  email failure can't break the alert.
- I proved it by injecting a "delivered without pickup" event: the DAG failed exactly at
  `data_quality`, the alert triggered and logged, and a forced recovery run went green after I
  removed the row.

**What's your end-to-end latency?**
At real-time speed, producer → Snowflake p50 is 0.21 s and p95 is 0.22 s, from Snowpipe
Streaming's push timestamp. GOLD trailed real time by 29 s, against a 10-minute target lag. I
exclude rows from accelerated simulations, whose timestamps are in simulated time, instead of
letting them produce negative latencies.

**How did you secure Airflow?**
It has its own service user (key-pair, transform role). A script generates `airflow/.env` with
only that connection, and only that one key file is mounted read-only. The Kafka connector's
key and my personal key never enter the Airflow containers.

**Airflow 3 gotchas you hit?**
- Task code can't query the metadata DB, so I derive run status from XCom plus Snowflake.
- `DagBag.get_dag()` hits the DB, so the tests use `dagbag.dags`.
- Imports from `include/` are `include.fooddel...`, because Astro puts the project root on the
  path.
- Astro's reverse proxy took port 8080, so I pinned the API server to 8082 with `--no-proxy`.

## P6: ETA model, Model Registry, MAE tracking

**How did you avoid leakage?**
Every input is known when the order is placed: time, weather, zone, surge fee, the
restaurant's attributes *as of that moment* (SCD2), basket, straight-line distance from
coordinates, and orders placed in the zone or restaurant in the previous 30 minutes. A unit
test rejects any post-placement column. A dbt test recomputes the 30-minute load feature with
a strict "earlier orders only" self-join on 500 samples and must match exactly. The split is by
time, not random.

**How good is the model?**
On the October test week (20k delivered orders), MAE is 4.29 minutes against 6.45 for today's
promise: 33% better. It also beats a zone × hour average (7.53). On live orders the next day
it scored 4.49 against 6.12. Distance and restaurant prep time carry most of the signal.

**Why train locally but serve in Snowflake?**
The training set is a few MB, so local training is free and simple. The model is logged to the
Snowflake Model Registry with its metrics, and inference runs in the warehouse from SQL with
`MODEL(ML.ETA_XGB, V1)!PREDICT(...)`. The orchestrator needs no ML libraries. I proved the two
agree: the in-warehouse MAE matched the offline MAE to four decimals. One gotcha: package
versions must exist in Snowflake's Anaconda channel, so I pinned xgboost 3.2.0, not PyPI's
latest.

**How do you avoid deploying a worse model?**
Champion/challenger. Every training run logs a new registry version, but it only becomes the
default if its test MAE is strictly better. Scoring always uses the default. I demonstrated it
with a weaker V2 (MAE 5.57): logged, not promoted.

**How do you track accuracy in production?**
Every prediction is stored with its model version. A daily view compares model MAE with the
promised-ETA MAE on delivered orders. A drift alert fires when a day's MAE exceeds 1.3× the
version's test MAE. It evaluated to false on live data, which is correct.

**Why is the bias negative?**
I optimise absolute error, which predicts the median. Delivery times are right-skewed, so the
median sits slightly below the mean. For a customer-facing promise you'd predict a higher
quantile (say p80) to keep SLA breaches down. For accuracy, the median is the right target.

## P7: Governance and cost

**How do you protect PII?**
My trial account is Standard edition, which has no masking or row access policies. So the
only way consumers reach MARTS is a layer of secure views built by dbt. Shared macros mask
email, phone and name unless the reader holds the PII role, and filter rows to the zones
mapped to a zone manager's role in a governance table. Consumer roles have no privileges on
the underlying tables at all. A check script runs every role and proves it: BI sees masked
PII across all zones, support sees clear text, west and east managers each see their 6 zones,
and their row counts add up to the total. All 6 checks still pass after a full rebuild.

**What would you do on Enterprise?**
Tag-based masking: one masking policy per data type attached to a PII tag, branching on the
column's tag value, plus a row access policy on ZONE_ID backed by the same mapping table. Then
the tables themselves are protected at every layer, not just the views. One gotcha: a tag can
carry only one masking policy per data type, so the policy has to read the column's tag value.

**Why a mapping table instead of hardcoding zones?**
Access becomes data. Giving a manager a new zone is an INSERT, auditable and needing no
deployment.

**How did you use zero-copy cloning?**
The whole DEV database clones in about 10 seconds with no data copied, for safe testing. I
suspend cloned Dynamic Tables and alerts immediately, because they would otherwise start
refreshing on their own. To show Time Travel, I deleted 19k orders in the clone and restored
them with `BEFORE (STATEMENT => '<query id>')`, leaving DEV untouched.

**What did the whole platform cost?**
About 1 credit across all seven phases: warehouses 0.85, Snowpipe Streaming 0.0003 (per GB,
serverless), alerts 0.002. The levers:
- XSMALL warehouses with 60 s auto-suspend and resource monitors
- incremental-only Dynamic Tables with skipped refreshes
- an Airflow DAG that short-circuits on metadata when there's no new data
- query tags, so every credit can be attributed to a workload
