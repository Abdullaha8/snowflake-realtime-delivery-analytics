# Resume bullets

Pick 3–5. Every number below was measured in this project; see the ADRs and docs/cost_report.md.

**Project title:** Real-Time Food Delivery Analytics Platform (Snowflake, Kafka, dbt, Airflow, Snowpark ML)

- Built an end-to-end streaming platform for a simulated Pune food-delivery marketplace:
  Python event simulator → Kafka (Avro, Schema Registry) → Snowpipe Streaming → Snowflake. It
  lands **1.35 M events with zero loss** (Kafka offsets contiguous) at **0.21 s p50** producer
  → Snowflake latency.
- Designed a near-real-time SILVER/GOLD layer on **Snowflake Dynamic Tables** (incremental
  refresh enforced) that deduplicates, rebuilds order lifecycles from late and out-of-order
  events, and computes zone demand/supply, a surge signal and SLA breaches. Reconciled **37/37
  metrics** against simulator ground truth.
- Modelled a **dbt** star schema with an incremental fact table, event-sourced **SCD2**
  dimensions with point-in-time joins, cohort retention and daily GMV/AOV marts. **81 dbt nodes
  green**, plus a 35-metric reconciliation against raw files (GMV matched to the paisa).
- Trained an **XGBoost ETA model** with leakage-safe features and a time-based split, served
  from the **Snowflake Model Registry** via in-warehouse SQL inference. Cut ETA error **33%**
  (MAE 4.29 vs 6.45 min), with champion/challenger promotion and daily MAE drift alerting.
- Orchestrated the batch layer with **Airflow 3**: a metadata-only new-data check skips idle
  runs at zero cost; persisted data-quality checks and **Snowflake Alerts** caught an injected
  data defect end to end.
- Implemented role-based governance on Snowflake Standard edition: dbt-built **secure views**
  mask PII and filter rows by zone per role, verified by an automated access matrix (**6/6**).
  Plus zero-copy clones, Time Travel recovery, Terraform modules and GitHub Actions CI.
- Kept the whole build to **≈1 Snowflake credit**: XSMALL warehouses with 60 s auto-suspend,
  resource monitors, serverless ingestion, query-tag cost attribution.

**Short version (one line):** Real-time food-delivery analytics on Snowflake: Kafka →
Snowpipe Streaming → Dynamic Tables + dbt + Snowpark ML ETA model (−33% error), orchestrated by
Airflow with data-quality alerts and role-based PII governance, built for ≈1 credit.
