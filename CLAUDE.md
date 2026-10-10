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
- [x] P4 (2026-10-10): dbt historical layer, 58/58 dbt checks, reconciled 35/35 vs backfill ground truth. ADR 0004.
  - dbt Core 1.12.5 + dbt-snowflake 1.12.1 (`make dbt`, `dbt-docs`, `dbt-freshness`; `make test` runs offline `dbt parse`). Profile = .env key-pair, role FOODDEL_TRANSFORM, WH_TRANSFORM.
  - Sources = SILVER DTs. STAGING schema (new, in 01/02) for stg/int + snapshot; MARTS for dims/fact/marts/seeds. `generate_schema_name` override.
  - fct_orders incremental merge on ORDER_ID, lookback var `incremental_lookback_hours`=24 (by LAST_EMITTED_TS); older bulk loads need ARGS="--full-refresh". dbt's "SUCCESS N" = rows_inserted only.
  - dim_restaurant = event-sourced SCD2 (int_restaurant_versions, collapses identical re-announced states, first version from 1900); snap_restaurants = standard check snapshot. PIT join verified.
  - Seeds zones/zone_cells generated by scripts/gen_zone_seeds.py (unit test guards drift).
  - Data now: BRONZE = backfill seed 2026 (1,354,413 events, 230,840 orders, Jul 1–Oct 10) + 1 short live run (7 orders).
- [x] P5 (2026-10-10): Airflow 3 orchestration + DQ + Snowflake Alerts + latency, all paths verified. ADR 0005.
  - Astro CLI 1.45.0 (full path if not on PATH: C:/Users/PC/AppData/Local/Microsoft/WinGet/Packages/Astronomer.Astro_Microsoft.Winget.Source_8wekyb3d8bbwe/astro.exe; Makefile `ASTRO ?= astro`), Runtime 3.3-8 (Airflow 3.3.2), providers-snowflake 6.17.0, dbt venv in image. UI http://localhost:8082 (`api-server.port`, `--no-proxy`), Postgres 5433.
  - DAG fooddel_pipeline: check_new_data (SHOW TABLES vs Variable `fooddel_bronze_rows_at_last_success`; param force) → refresh_dynamic_tables → dbt_build (--target airflow) → data_quality (0 retries) → record_latency → record_run (all_done). Imports `from include.fooddel ...`.
  - OPS schema: PIPELINE_RUNS, DQ_RESULTS, LATENCY_METRICS, ALERT_LOG, V_INGEST_LATENCY (excludes negative = accelerated runs), V_PIPELINE_HEALTH. Alerts ALERT_DQ_FAILED / ALERT_SLA_SPIKE / ALERT_INGEST_LATENCY (serverless, suspended by default) → OPS.RAISE_ALERT. Email only if SNOWFLAKE_ALERT_EMAIL set (currently empty → log only).
  - SVC_AIRFLOW (key secrets/airflow_rsa_key.p8); airflow/.env generated by `make airflow-env` (only SVC_AIRFLOW conn).
  - Measured: emit→push p50 0.21 s / p95 0.22 s at 1x; GOLD lag 29 s. `make stop` also stops Airflow + suspends Alerts.
- [x] P6 (2026-10-10): ETA model + Model Registry + MAE tracking, all verified. ADR 0006.
  - Libs (must ALSO exist in Snowflake Anaconda channel): snowflake-ml-python 2.2.0, snowpark 1.55.0, xgboost 3.2.0 (not 3.4.1), scikit-learn 1.9.1 (`.[ml]` extra). Anaconda terms already acknowledged.
  - Features: dbt `ML.ML_ETA_FEATURES` (known-at-placement only; SPLIT TRAIN<=09-15, VALID<=09-30, TEST<=10-09, LIVE after; dbt vars eta_*). Leakage test assert_eta_load_features_use_only_earlier_orders. stg_orders now has DELIVERY_LAT/LNG.
  - ml/train.py: Snowpark pull → sklearn Pipeline(OneHot→XGB, L1) grid on VALID → refit TRAIN+VALID → TEST → registry `ML.ETA_XGB` V<n> + ML.MODEL_VERSIONS; promote only if TEST MAE better. Output field `output_feature_0`.
  - Results: V1 test MAE 4.293 (promised 6.446, rule 7.615, zone×hour 7.530); backtest in-warehouse 4.2934 = offline; V2 (dropped features) 5.574 not promoted; LIVE Oct 10 MAE 4.49 vs promised 6.12.
  - Scoring: DAG task score_eta (after data_quality) via include/fooddel/eta_scoring.py (pure SQL); ML.ETA_PREDICTIONS, V_ETA_ACCURACY_DAILY, ML.ALERT_ETA_DRIFT (cron 06:00 IST, suspended). sf.py alerts covers OPS.* and ML.*.
- [x] P7 (2026-10-10): governance + cost, verified. ADR 0007, docs/cost_report.md.
  - Account is STANDARD edition (masking/row access policies = "Unsupported feature" 391811) → governance via dbt secure views in MARTS_SECURE (models/secure, macros/governance.sql: mask_email/phone/name, zone_filter). Consumers (BI, PII_READER, OPS_WEST/EAST) have NO MARTS grants (02 no longer grants BI on MARTS; 10 revokes). GOVERNANCE.ZONE_ACCESS (role→zone, CURRENT_ROLE()).
  - Roles: FOODDEL_PII_READER (inherits BI), FOODDEL_OPS_WEST/EAST; all granted to admin user. Test with USE SECONDARY ROLES NONE. `make sf-governance` → `make dbt` → `make governance-check` (6/6).
  - `make qa-clone` (clone + suspend DTs/alerts + Time Travel restore) / `make qa-drop`. `make cost-report` → docs/cost_report.md (1.03 credits total so far). Airflow conn sets QUERY_TAG=airflow.
  - Skipped by choice: SYSTEM$CLASSIFY, dbt build on clone.
- [ ] P8
