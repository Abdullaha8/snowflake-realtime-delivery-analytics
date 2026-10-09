# ADR 0003: SILVER/GOLD with Dynamic Tables

- **Status:** Accepted (Phase 3, 2026-10-10)
- **Scope:** `snowflake/05_silver.sql`, `06_gold.sql`, `91_verify_dt.sql`, `scripts/reconcile.py`

## Context

BRONZE holds raw events exactly as they arrived, with all their problems:
- ~2% duplicates
- late events (up to 6 h)
- out-of-order events

The business needs near-real-time answers:
- SLA breaches
- demand vs. driver supply per zone, and a surge signal
- restaurant prep time and cancellation monitoring
- H3 geo views

All of it has to stay within trial credits.

## Options considered
| Option | Why not (or why) |
|---|---|
| Streams + Tasks (hand-written MERGE) | Full control, but I'd own the incremental logic, ordering and retries myself. |
| Materialized views | No joins, no window functions; too limited for lifecycle and dedup. |
| An external stream processor (Flink/Spark) | Another major system to run, plus watermarks that drop late data. |
| **Dynamic Tables (chosen)** | Declarative SQL. Snowflake handles incremental refresh, dependency ordering and scheduling. |

## Decisions

### 1. Two layers, each with a different refresh contract
- **SILVER** (`TARGET_LAG = DOWNSTREAM`): `ORDER_EVENTS`, `DRIVER_PINGS`, `REFERENCE_EVENTS` (dedup + typing) and `ORDERS` (one row per order). It refreshes only when GOLD needs it, so it never costs more than GOLD's lag.
- **GOLD** (`TARGET_LAG = '10 minutes'`, the project default): `ZONE_DEMAND_SUPPLY_5M`, `SLA_BREACHES`, `RESTAURANT_PERF_HOURLY`, `HEX_DEMAND_HOURLY`, plus the view `V_ORDERS_AT_RISK`. `make dt-demo` switches to 1 minute for demos.

### 2. `REFRESH_MODE = INCREMENTAL` is explicit
With `AUTO`, an unsupported construct silently becomes a FULL refresh that recomputes everything every time. Setting `INCREMENTAL` makes `CREATE` fail instead, so the cost model is enforced by the database rather than by hoping. Constraints from the docs that shaped the SQL:
- no subqueries outside `FROM`
- no `CURRENT_TIMESTAMP` in `SELECT`
- outer joins only with `=` conditions
- `APPROX_PERCENTILE` only in `SELECT`

### 3. Dedup by `event_id`, first-arrived copy
`QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY emitted_ts, partition, offset) = 1`. Copies are identical, so the tie-break only has to be deterministic.

### 4. Order status from which events exist, not arrival order
`ORDERS` aggregates `MIN(IFF(event_type = ..., event_ts, NULL))` per lifecycle step. Status follows a fixed precedence: DELIVERED > CANCELLED > PICKED_UP > DRIVER_ASSIGNED > ACCEPTED > PLACED. So a `DELIVERED` that arrives before `PICKED_UP` still yields DELIVERED. Orders whose `ORDER_PLACED` hasn't arrived yet appear with `HAS_PLACED = FALSE` instead of being dropped.

### 5. Late data: recompute, don't watermark
Each refresh folds newly arrived BRONZE rows into the affected groups, so a 6-hour-late event still corrects its order and its GOLD buckets. That's the main difference from a windowed stream processor, which would discard the event after the watermark. The cost is that "final" numbers for a bucket can change later, and that's the correct behaviour for a report on event time.

### 6. "Now" lives in a view, and it's stream time
"Orders past their promise right now" depends on the current time, which can't be maintained incrementally. So it's a view over `SILVER.ORDERS`. "Now" is the newest event time seen (`MAX(event_ts)`), because simulated runs replay accelerated or past time. In production this would be `CURRENT_TIMESTAMP()`.

### 7. Surge signal: simple, explainable bands
`pressure = orders placed in a 5-minute bucket / idle drivers seen in that bucket`, per zone. Multipliers:

| Condition | Multiplier |
|---|---|
| no idle drivers and orders waiting | 2.0× |
| pressure ≥ 2 | 1.75× |
| pressure ≥ 1.5 | 1.5× |
| pressure ≥ 1 | 1.25× |
| otherwise | 1.0× |

This is a *signal* for pricing, not the price itself.

### 8. H3 in Snowflake
`H3_LATLNG_TO_CELL_STRING(lat, lng, 8)` recomputes the cell for pings and delivery points. `DRIVER_PINGS.H3_MATCH` checks it against the simulator's cell (0 mismatches). `HEX_DEMAND_HOURLY` aggregates by cell for the P8 map.

### 9. Proven against ground truth
`make reconcile` produces one seeded run to Kafka and keeps the same events in memory. Python computes the expected numbers independently. After the connector lands everything, the Dynamic Tables are refreshed and **37 metrics** are compared: BRONZE rows, unique events, duplicates removed (equal to duplicates injected), order statuses, cancel reasons, SLA breaches, GMV, per-zone counts, and whether every GOLD table adds back up to SILVER. **Result: 37/37 match.** `91_verify_dt.sql` adds data-quality checks, all 0:
- no duplicate `event_id`s
- no impossible lifecycles
- payload and timestamp delivery times agree
- H3 cells agree

## Gotchas found
- **A DOWNSTREAM table nobody reads never refreshes.** `REFERENCE_EVENTS` stayed empty, and manually refreshing GOLD skipped it too. It now has its own 10-minute lag until P4's dimensions read it. `make dt-refresh` refreshes every table that has its own lag.
- **`make up` used `--build`,** which re-checked Docker Hub on every start. A Hub outage (504) then blocked a local stack whose image was already built. `make build` is now separate.

## Cost
- **Warehouse:** everything runs on `WH_TRANSFORM` (XSMALL), and only when there is new data; refreshes without base-table changes are skipped.
- **Measured:** the initial build plus one reconciliation of ~47k events cost well under 0.1 credits.
- **Estimated while streaming:** ~0.2–0.3 credits/hour at the 10-minute lag, ~1 credit/hour in demo mode at 1 minute.
- **Shutdown:** `make stop` suspends every Dynamic Table, then the warehouses.

## Trade-offs
- `CREATE OR REPLACE` keeps the SQL idempotent but re-initializes the table, which is a full rebuild. That's acceptable at this volume; at scale I'd use `ALTER` for lag/warehouse changes and redeploy only on logic changes.
- The surge signal is jumpy at low volume: a single order with no idle driver gives 2.0×. A production version would smooth over several buckets or require a minimum order count.
- Buckets are in IST, the business timezone. Storage stays in UTC (`*_TS`), with `*_IST` columns derived.
