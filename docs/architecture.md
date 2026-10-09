# Architecture

```mermaid
flowchart LR
    SIM["Python simulator<br/>(simulator/)"] -->|JSON, keyed| K[("Kafka (KRaft)<br/>orders / driver_locations / reference")]
    SIM -. backfill JSONL .-> S3[(S3 / stage)]
    K -->|Snowflake Kafka Connector<br/>Snowpipe Streaming| B[BRONZE<br/>VARIANT]
    S3 -. COPY INTO .-> B
    B --> DT[SILVER / GOLD<br/>Dynamic Tables]
    B --> DBT[dbt star schema<br/>MARTS]
    DT --> ML[Snowpark ML<br/>ETA model]
    DBT --> ML
    DT --> ST[Streamlit in Snowflake]
    DBT --> ST
    AF[Airflow] -.orchestrates.-> DBT
    AF -.-> ML
```

| Phase | Status | Components |
|---|---|---|
| P1 | done | simulator, Kafka docker-compose, Makefile, tests |
| P2-P8 | planned | see CLAUDE.md |

## Event flow (P1)
- Event contract: [`simulator/schemas/`](../simulator/schemas)
- Design rationale: [ADR 0001](decisions/0001-simulator-design.md)
