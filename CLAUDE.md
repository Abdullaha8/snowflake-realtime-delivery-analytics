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
make up        # start Kafka + Airflow
make sim       # run simulator
make dbt       # dbt build
make test      # pytest + dbt test + sqlfluff
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
- [x] P1 (2026-10-10): simulator + 42 tests passing, Makefile, Kafka docker-compose (not yet run: Docker not installed), ADR 0001
  - Topics: `fooddel.orders.v1` (key order_id), `fooddel.driver_locations.v1` (key driver_id), `fooddel.reference.v1` (key entity id). Schemas in `simulator/schemas/`.
  - Chaos ground truth: `<out>/_manifest/<run_id>.jsonl` (jsonl sink) or `data/manifests/` (stdout/kafka).
  - Env notes: Windows, `.venv` = Python 3.11, GNU make via winget (ezwinports.make). Makefile recipes must stay single commands (they run under cmd.exe from PowerShell).
  - Carry-overs to P2: install Docker Desktop and verify `make up` + `make sim SINK=kafka`; disable topic auto-create; `make stop` must also suspend warehouses.
- [ ] P2