# Architecture

```mermaid
flowchart LR
    SIM["Python simulator<br/>(simulator/)"] -->|Avro, keyed<br/>auto-register OFF| K[("Kafka (KRaft)<br/>orders / driver_locations / reference")]
    SR[(Schema Registry<br/>BACKWARD)] -.schema ids.- SIM
    SR -.decode.- KC
    K --> KC["Kafka Connect<br/>Snowflake sink v4<br/>(Snowpipe Streaming)"]
    KC -->|unconvertible| DLQ[(fooddel.dlq.v1)]
    KC --> B[BRONZE<br/>RECORD_METADATA + RECORD_CONTENT]
    SIM -. backfill JSONL .-> ST1[(internal stage)]
    ST1 -. COPY INTO .-> B
    B --> DT[SILVER / GOLD<br/>Dynamic Tables]
    B --> DBT[dbt star schema<br/>MARTS]
    DT --> ML[Snowpark ML<br/>ETA model]
    DBT --> ML
    DT --> STL[Streamlit in Snowflake]
    DBT --> STL
    AF[Airflow] -.orchestrates.-> DBT
    AF -.-> ML
```

| Phase | Status | Components |
|---|---|---|
| P1 | done | simulator, Makefile, tests |
| P2 | done, verified end-to-end | topics, Schema Registry (Avro), Kafka Connect + Snowflake sink v4, BRONZE DDL, RBAC, backfill COPY |
| P3 | done, reconciled 37/37 | SILVER Dynamic Tables (dedup, typing, order lifecycle), GOLD Dynamic Tables (demand/supply + surge, SLA breaches, restaurant perf, H3 hex demand), at-risk view |
| P4 | done, reconciled 35/35 | dbt star schema on SILVER: fct_orders (incremental), SCD2 dim_restaurant, dim_customer/zone/driver/date, daily KPIs, cohort retention, restaurant daily |
| P5 | done | Airflow 3 DAG (skip-if-no-data, DT refresh, dbt, DQ, latency), OPS schema, 3 Snowflake Alerts, SVC_AIRFLOW |
| P6 | done | ETA model: dbt features, XGBoost pipeline in Snowflake Model Registry, in-warehouse scoring in the DAG, daily MAE + drift alert (test MAE 4.29 vs promised 6.45) |
| P7 | done | Secure-view governance (Standard edition): PII masking + zone row filtering by role, zero-copy clone + Time Travel, cost report |
| P8 | done | Streamlit in Snowflake dashboard (owned by FOODDEL_BI), Terraform environment module (validate-only), GitHub Actions CI, final docs |

## Local stack (`make up`)

| Service | Port | Purpose |
|---|---|---|
| kafka (cp-kafka 7.8.2, KRaft) | 9092 | broker; auto-create OFF |
| kafka-init | - | creates topics + retention, then exits |
| schema-registry | 8081 | 11 Avro subjects, BACKWARD |
| connect (+ Snowflake connector 4.1.0) | 8083 | sink to BRONZE; deployed only by `make connector-up` |
| kafka-ui | 8080 | browse topics (Avro-decoded), schemas, connector |

## Snowflake objects

| Object | Notes |
|---|---|
| `FOODDEL_DEV` | schemas BRONZE / SILVER / GOLD / MARTS |
| `BRONZE.ORDERS_RAW`, `DRIVER_LOCATIONS_RAW`, `REFERENCE_RAW` | VARIANT landing tables, change tracking on |
| `BRONZE.BACKFILL_STAGE`, `FF_JSONL` | internal stage for history |
| `WH_INGEST`, `WH_TRANSFORM`, `WH_BI` | XSMALL, AUTO_SUSPEND=60, resource monitors (5 cr each, 20 account) |
| `FOODDEL_INGEST` / `_TRANSFORM` / `_BI` | functional roles under SYSADMIN |
| `SVC_KAFKA_CONNECTOR` | TYPE=SERVICE, key-pair only |

## Dynamic Tables (P3)

```mermaid
flowchart LR
    OR[BRONZE.ORDERS_RAW] --> OE[SILVER.ORDER_EVENTS<br/>dedup]
    DR[BRONZE.DRIVER_LOCATIONS_RAW] --> DP[SILVER.DRIVER_PINGS<br/>dedup + H3]
    RR[BRONZE.REFERENCE_RAW] --> RE[SILVER.REFERENCE_EVENTS<br/>dedup]
    OE --> O[SILVER.ORDERS<br/>1 row/order]
    O --> ZD[GOLD.ZONE_DEMAND_SUPPLY_5M<br/>surge signal]
    DP --> ZD
    O --> SB[GOLD.SLA_BREACHES]
    O --> RP[GOLD.RESTAURANT_PERF_HOURLY]
    O --> HX[GOLD.HEX_DEMAND_HOURLY]
    O --> AR[GOLD.V_ORDERS_AT_RISK<br/>view, stream time]
    OE --> AR
```

| Layer | Target lag | Refresh mode |
|---|---|---|
| SILVER (ORDER_EVENTS, DRIVER_PINGS, ORDERS) | DOWNSTREAM | INCREMENTAL |
| SILVER.REFERENCE_EVENTS | 10 minutes (no consumers until P4) | INCREMENTAL |
| GOLD | 10 minutes (`make dt-demo` -> 1 minute) | INCREMENTAL |

## dbt historical layer (P4)

```mermaid
flowchart LR
    SO[SILVER.ORDERS] --> STO[stg_orders]
    SR[SILVER.REFERENCE_EVENTS] --> STR[stg_restaurant_changes]
    SR --> STC[stg_customers]
    STR --> IRV[int_restaurant_versions<br/>event-sourced SCD2]
    IRV --> DR[dim_restaurant]
    IRV --> IRC[int_restaurant_current] --> SNAP[snap_restaurants<br/>dbt snapshot]
    STO --> ICF[int_customer_first_order]
    STC --> DC[dim_customer]
    ICF --> DC
    STO --> DD[dim_driver]
    SEED[seeds: zones, zone_cells] --> DZ[dim_zone]
    STO --> F[fct_orders<br/>incremental merge]
    DR -- point-in-time --> F
    F --> K[mart_daily_kpis]
    F --> CR[mart_cohort_retention]
    DC --> CR
    F --> RD[mart_restaurant_daily]
```

Schemas: `STAGING` (stg/int views+tables, snapshot; not visible to BI), `MARTS` (dims, fact, marts, seeds).

## Orchestration, data quality, alerts (P5)

```mermaid
flowchart LR
    subgraph Airflow["Airflow 3 (Astro) - fooddel_pipeline, hourly"]
        C[check_new_data<br/>SHOW TABLES, no warehouse] --> R[refresh_dynamic_tables]
        R --> D[dbt_build<br/>SVC_AIRFLOW]
        D --> Q[data_quality<br/>no retries]
        Q --> L[record_latency]
        L --> P[record_run<br/>all_done]
    end
    Q --> DQ[(OPS.DQ_RESULTS)]
    L --> LM[(OPS.LATENCY_METRICS)]
    P --> PR[(OPS.PIPELINE_RUNS)]
    DQ --> A1{{ALERT_DQ_FAILED}}
    SO[SILVER.ORDERS] --> A2{{ALERT_SLA_SPIKE}}
    VL[OPS.V_INGEST_LATENCY] --> A3{{ALERT_INGEST_LATENCY}}
    A1 & A2 & A3 --> RA[OPS.RAISE_ALERT] --> AL[(OPS.ALERT_LOG)]
    RA -. if email set .-> EM[email]
```

| Local UI | Port |
|---|---|
| Airflow | http://localhost:8082 |
| kafka-ui | http://localhost:8080 |

## ETA model (P6)

```mermaid
flowchart LR
    F[MARTS.FCT_ORDERS + DIM_RESTAURANT] --> FE[ML.ML_ETA_FEATURES<br/>dbt, known-at-placement only]
    FE -->|Snowpark pull| T[ml/train.py<br/>local sklearn+XGBoost]
    T -->|log_model + metrics| R[(Model Registry<br/>ML.ETA_XGB V1, V2...)]
    T --> MV[(ML.MODEL_VERSIONS)]
    R -->|default version| S[score_eta<br/>MODEL!PREDICT in warehouse]
    FE --> S
    S --> P[(ML.ETA_PREDICTIONS)]
    P --> A[ML.V_ETA_ACCURACY_DAILY]
    A --> D{{ALERT_ETA_DRIFT}}
```

## Governance (P7)

```mermaid
flowchart LR
    M[(MARTS tables)] --> SV[MARTS_SECURE secure views<br/>mask_* + zone_filter macros]
    ZA[(GOVERNANCE.ZONE_ACCESS)] --> SV
    SV --> BI[FOODDEL_BI<br/>all zones, PII masked]
    SV --> PR[FOODDEL_PII_READER<br/>all zones, clear PII]
    SV --> OW[FOODDEL_OPS_WEST<br/>6 zones, masked]
    SV --> OE[FOODDEL_OPS_EAST<br/>6 zones, masked]
    M -. no grants .-x BI
```

## Design rationale
- [ADR 0001: simulator](decisions/0001-simulator-design.md)
- [ADR 0002: Kafka, Schema Registry, Snowflake ingestion](decisions/0002-kafka-schema-registry-snowflake-ingestion.md)
- [ADR 0003: SILVER/GOLD Dynamic Tables](decisions/0003-dynamic-tables-silver-gold.md)
- [ADR 0004: dbt historical layer](decisions/0004-dbt-historical-layer.md)
- [ADR 0005: orchestration, data quality, alerts, latency](decisions/0005-orchestration-dq-alerts-latency.md)
- [ADR 0006: ETA model, Model Registry, MAE tracking](decisions/0006-eta-model-registry-mae.md)
- [ADR 0007: governance and cost](decisions/0007-governance-and-cost.md) · [cost report](cost_report.md)
- [ADR 0008: dashboards, Terraform, CI](decisions/0008-dashboards-terraform-ci.md)
