# Real-Time Food Delivery Analytics Platform

## About me
Data Engineer, ~1 yr experience (Snowflake, dbt, Airflow, AWS). SnowPro certified.
This is a resume portfolio project. I must be able to explain every design choice in interviews.

## Problem
A food delivery company (Swiggy/Zomato-style, simulated in Pune) needs:
- Near-real-time SLA breach detection and a surge pricing signal per zone (demand vs. driver supply)
- ETA prediction plus accuracy tracking
- Restaurant prep-time and cancellation monitoring
- Historical KPIs: GMV, AOV, cohort retention

## Architecture
Python simulator → Kafka (Docker, KRaft) → Snowflake Kafka Connector (Snowpipe Streaming) → BRONZE (VARIANT)
→ SILVER/GOLD Dynamic Tables (real-time) + dbt star schema (historical)
→ Snowpark ML ETA model → Streamlit in Snowflake dashboards.
Airflow (Astro CLI) orchestrates batch work. Terraform manages infra. GitHub Actions runs CI.

## Repo structure
simulator/      Python event generator (Faker, H3 zones, configurable EPS)
infra/          docker-compose (Kafka, Airflow), terraform/ (Snowflake + S3)
snowflake/      numbered SQL scripts: 01_setup, 02_rbac, 03_bronze, 04_dynamic_tables, ...
dbt/            dbt project (staging → intermediate → marts, snapshots)
airflow/dags/   DAGs
ml/             Snowpark ETA model training + scoring
streamlit/      dashboards
docs/           architecture.md (Mermaid), decisions/ (ADRs), interview_qa.md
.github/        CI workflows

## Conventions
- Python 3.11, type hints, ruff + black. SQL: sqlfluff (snowflake dialect), UPPER keywords.
- Snowflake naming: DB `FOODDEL_{ENV}`, schemas BRONZE/SILVER/GOLD/MARTS, warehouses WH_INGEST/WH_TRANSFORM/WH_BI.
- All SQL is idempotent (CREATE OR REPLACE / IF NOT EXISTS). All secrets come from `.env` (never commit it); keep `.env.example` updated.
- Every significant design choice gets a short ADR in docs/decisions/ (context, options, decision, trade-offs).

## Cost rules (Snowflake trial: protect credits)
- All warehouses XSMALL, AUTO_SUSPEND=60, with resource monitors.
- Dynamic Tables default TARGET_LAG='10 minutes'; drop to 1 minute only for demos.
- Never leave the simulator or Kafka connector running. Provide `make stop` to suspend everything.

## Workflow rules
- Work ONE phase at a time. Plan first, then implement. Stop at the end of each phase and wait for me.
- After each phase: run the verification commands, show the outputs, update README + docs, then summarize what was built and "what to say in an interview."
- Commit after each working step with conventional commits (feat:, fix:, docs:).
- Ask me before: creating cloud resources, running anything that costs credits, or adding new major tools.
- Do NOT run commands that need my Snowflake/AWS credentials. Write them and tell me to run them, unless .env is configured.
- Prefer simple, explainable solutions over clever ones.

## Commands
make up        # start Kafka, Schema Registry, Connect (+ Airflow from P5)
make sim       # run simulator
make dbt       # dbt build
make test      # lint + sqlfluff + pytest (+ dbt test from P4); make test-integration needs `make up`
make stop      # stop containers + suspend warehouses

## Phases
P1 Repo scaffold, Makefile, docker-compose, simulator (with late/duplicate/out-of-order events) + tests
P2 Kafka topics + schema registry + Snowflake Kafka Connector → BRONZE
P3 SILVER/GOLD Dynamic Tables: dedup, order lifecycle, zone demand/supply, SLA breaches, surge signal, H3 geo
P4 dbt historical layer: star schema, SCD2 snapshots, incremental marts, tests, docs
P5 Airflow DAGs + data quality + Snowflake Alerts + latency metric
P6 Snowpark ML ETA model + Model Registry + MAE tracking
P7 Governance (RBAC, masking, row access policy, clones) + cost tuning report
P8 Streamlit dashboards, Terraform, GitHub Actions, final README, resume bullets, interview Q&A

## Current status
<!-- Claude updates this after each phase -->
- [x] P1 (2026-10-10): simulator, Makefile, tests, ADR 0001. Kafka path verified (7,715 produced = 7,715 in topics).
  - Chaos ground truth: `<out>/_manifest/<run_id>.jsonl` (jsonl sink) or `data/manifests/` (stdout/kafka).
  - Env notes: Windows, `.venv` = Python 3.11, GNU make via winget (ezwinports.make), Docker Desktop. Makefile recipes must stay single commands (cmd.exe from PowerShell). In Git Bash, prefix `docker exec` with `MSYS_NO_PATHCONV=1`.
- [x] P2 (2026-10-10): Kafka → Snowflake BRONZE verified end-to-end. ADR 0002.
  - Stack: Confluent 7.8.2 (KRaft broker, Schema Registry, Connect) + Snowflake connector v4 **4.1.0** (pinned; 4.2.x < 2 weeks old at the time). Auto-create OFF; topics from `infra/kafka/create_topics.sh` (+ `fooddel.dlq.v1`).
  - Contract: Avro generated from pydantic (`simulator/avro.py`), TopicRecordNameStrategy on producer AND Connect converter, BACKWARD, producer auto-register OFF. `make schemas` + `make schemas-register`.
  - Connector config gotchas (fixed): `snowflake.streaming.validate.compatibility.with.classic=false`; `value.converter.value.subject.name.strategy=TopicRecordNameStrategy`. `make connector-status` shows DLQ depth (36,058 old failed records from the first run stay there until retention).
  - BRONZE: `ORDERS_RAW`, `DRIVER_LOCATIONS_RAW`, `REFERENCE_RAW` (RECORD_METADATA/RECORD_CONTENT VARIANT, change tracking on). Default pipes `<TABLE>-STREAMING` auto-created. Backfill rows (if loaded) have `RECORD_METADATA:source = 'backfill'`; backfill NOT loaded yet.
  - Results: BRONZE rows = Kafka messages per topic (56,173 / 1,587 / 4,944); offset GAPS = 0 on all 9 partitions; fresh run 3,896 emitted = 3,896 rows, 1.82% dups; Kafka→push p50 0.06 s. Current BRONZE has many cross-run duplicates (same seeds replayed in tests) – fine for P3 dedup.
  - Roles: FOODDEL_INGEST (connector), FOODDEL_TRANSFORM, FOODDEL_BI; user SVC_KAFKA_CONNECTOR (TYPE=SERVICE, key-pair). Resource monitors 5 cr/warehouse, 20 account (~0.1 used).
- [x] P3 (2026-10-10): SILVER/GOLD Dynamic Tables, reconciled 37/37 against simulator ground truth. ADR 0003.
  - SILVER (DOWNSTREAM, INCREMENTAL): ORDER_EVENTS, DRIVER_PINGS (+H3 recompute/H3_MATCH), ORDERS (1 row/order, status by precedence, HAS_PLACED). REFERENCE_EVENTS has its own '10 minutes' lag until P4 reads it (DOWNSTREAM with no consumer never refreshes); switch it to DOWNSTREAM in P4.
  - GOLD ('10 minutes', INCREMENTAL): ZONE_DEMAND_SUPPLY_5M (surge bands), SLA_BREACHES, RESTAURANT_PERF_HOURLY, HEX_DEMAND_HOURLY; view V_ORDERS_AT_RISK (stream time = MAX(event_ts)). Buckets in IST.
  - Ops: `make sf-dt`, `dt-status`, `dt-refresh` (refreshes own-lag tables), `dt-demo`/`dt-normal`, `dt-suspend`/`dt-resume`, `sf-verify-dt`, `sf-reset-bronze` (DEV only), `reconcile` (`--no-produce` to re-check). `make stop` suspends DTs. `make up` no longer rebuilds (`make build` does).
  - BRONZE currently = reconcile run seed 77 only (47,406 rows). Backfill not loaded (P4).
- [ ] P4
