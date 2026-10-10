# Cost report - FOODDEL_DEV
_Generated 2026-10-10 by `make cost-report` from SNOWFLAKE.ACCOUNT_USAGE (last 7 days; this view lags up to ~3 hours)._
## Resource monitors (credits used this month / quota)
| monitor | used | quota |
|---|---|---|
| RM_FOODDEL_ACCOUNT | 1.03 | 20.00 |
| RM_WH_BI | 0.07 | 5.00 |
| RM_WH_INGEST | 0.23 | 5.00 |
| RM_WH_TRANSFORM | 0.73 | 5.00 |

## Credits by service

| SERVICE_TYPE | CREDITS |
|---|---|
| WAREHOUSE_METERING | 0.8474 |
| SERVERLESS_ALERTS | 0.0018 |
| SNOWPIPE_STREAMING | 0.0003 |

## Credits by warehouse

| WAREHOUSE_NAME | CREDITS | ACTIVE_HOURS |
|---|---|---|
| WH_TRANSFORM | 0.6111 | 5 |
| WH_INGEST | 0.2310 | 5 |
| COMPUTE_WH | 0.0039 | 5 |
| WH_BI | 0.0022 | 4 |
| CLOUD_SERVICES_ONLY | 0.0012 | 5 |

## Compute credits by workload (query tag)

| WORKLOAD | QUERIES | CREDITS |
|---|---|---|
| dbt | 84 | 0.05 |
| (untagged) | 314 | 0.0453 |
| fooddel-scripts | 95 | 0.0447 |

## Dynamic Table refreshes (SKIPPED = no new data, no cost)

| DYNAMIC_TABLE | STATE | REFRESHES |
|---|---|---|
| GOLD.HEX_DEMAND_HOURLY | SUCCEEDED | 7 |
| GOLD.RESTAURANT_PERF_HOURLY | SUCCEEDED | 7 |
| GOLD.SLA_BREACHES | SUCCEEDED | 8 |
| GOLD.ZONE_DEMAND_SUPPLY_5M | SUCCEEDED | 8 |
| SILVER.DRIVER_PINGS | SUCCEEDED | 8 |
| SILVER.ORDERS | SUCCEEDED | 27 |
| SILVER.ORDER_EVENTS | SUCCEEDED | 27 |
| SILVER.REFERENCE_EVENTS | SUCCEEDED | 8 |

## Storage by schema (FOODDEL_DEV)

_no rows yet (ACCOUNT_USAGE lags up to ~3 h)_

## Recommendations

**Already applied (and why):**
- **Warehouses:** all XSMALL, `AUTO_SUSPEND = 60`, created suspended, with a resource monitor
  each (5 credits) plus 20 credits account-wide. Nothing in this workload needs more than an
  XSMALL: the largest job, `dbt build` over 230k orders, finishes in ~11 s.
- **Ingestion:** Snowpipe Streaming is serverless and billed per GB, so streaming needs no
  warehouse.
- **Dynamic Tables:**
  - `REFRESH_MODE = INCREMENTAL` is forced, so a FULL refresh can never happen silently.
  - SILVER is `DOWNSTREAM`, so it refreshes only when GOLD needs it.
  - GOLD lag is 10 minutes; refreshes with no new data are SKIPPED (see the table above).
- **Airflow:** the pipeline short-circuits on metadata (`SHOW TABLES`), so an hour with no new
  data costs **0 credits**.
- **Alerts, Dynamic Tables, connector:** Alerts are created suspended; `make stop` suspends
  Alerts and Dynamic Tables and deletes the connector.
- **Storage:**
  - dbt tables are TRANSIENT (dbt-snowflake default), so they have no Fail-safe.
  - BRONZE `DATA_RETENTION_TIME_IN_DAYS = 1` keeps Time Travel short on the largest tables.

**Found in this report:**
- `COMPUTE_WH`, the trial's default warehouse, still shows small usage (Snowsight worksheets).
  Suspend it or set `AUTO_SUSPEND = 60` on it too, so ad-hoc work runs on WH_BI with a monitor.
- `(untagged)` queries were Airflow's and ad-hoc ones. The Airflow connection now sets
  `QUERY_TAG = airflow` (scripts/gen_airflow_env.py), so pipeline compute is attributable.

**Next, if usage grows:**
1. **A Snowflake Budget** for serverless spend (Snowpipe Streaming, serverless alerts).
   Resource monitors only cap warehouses.
2. **Keep demo lag (1 minute) for demos only.** At a 1-minute GOLD lag WH_TRANSFORM stays warm
   (~1 credit/h) instead of ~0.2–0.3 credits/h at 10 minutes. `make dt-normal` restores it.
3. **Partition BRONZE by time for pruning** before it reaches hundreds of millions of rows: a
   clustering key on event date, or a `RECORD_METADATA` push-date column. Not worth it at 1.4 M
   rows.
4. **Move the hourly DAG to "on new data"** (Snowflake streams / Airflow asset triggers) if
   ingestion becomes bursty.
5. **Scale out (multi-cluster), not up**, for many concurrent dashboard users on WH_BI;
   queries are small.
