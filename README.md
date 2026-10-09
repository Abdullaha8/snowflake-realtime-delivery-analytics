# Real-Time Food Delivery Analytics Platform (Pune)

A Swiggy/Zomato-style delivery marketplace, simulated in Pune, feeding a Snowflake pipeline
for near-real-time SLA breach detection, surge signals, ETA prediction and historical KPIs.

**Pipeline:** Python simulator → Kafka (KRaft) → Snowflake Kafka Connector → BRONZE → Dynamic
Tables + dbt → Snowpark ML → Streamlit. See [docs/architecture.md](docs/architecture.md).

| Phase | | |
|---|---|---|
| P1 | ✅ | Repo scaffold, Makefile, event simulator + tests |
| P2 | 🟡 | (local stack verified; Snowflake side awaits first run) Kafka topics, Schema Registry (Avro), Kafka Connect + Snowflake sink v4 → BRONZE, backfill COPY |
| P3–P8 | ⏳ | Dynamic Tables, dbt, Airflow, ML, governance, dashboards |

## Quick start

Requirements: Python 3.11, GNU make (Windows: `winget install ezwinports.make`), Docker Desktop.

```bash
make install        # .venv + dependencies
make test           # ruff + black + sqlfluff + unit tests
make sim-sample     # 3 simulated hours of Friday dinner rush -> data/sample, then a report
make sim-backfill   # history since launch (2026-07-01) -> data/backfill/*.jsonl.gz
```

### Local streaming (no Snowflake needed)

```bash
cp .env.example .env
make up                 # Kafka, Schema Registry, Kafka Connect, kafka-ui (http://localhost:8080)
make schemas-register   # 11 Avro subjects, BACKWARD compatibility
make sim SINK=kafka SPEED=60 DURATION=60
make topics             # messages per topic-partition
make test-integration   # topics, contract gate, Avro round trip, connector plugin
make stop               # always stop when done
```

### Snowflake ingestion (one-time setup, then per session)

```bash
make sf-keygen          # key pairs in secrets/ (gitignored); prints the ALTER USER + .env lines
#  -> run the printed ALTER USER in Snowsight, fill .env (account, user, keys)
make sf-setup           # 01_setup, 02_rbac, 03_bronze: DB, schemas, warehouses, monitors, roles, BRONZE
make up                 # restart so Connect picks up the Snowflake settings from .env
make connector-up       # starts Snowpipe Streaming ingestion (billed per GB)
make sim SINK=kafka SPEED=60 DURATION=120
make connector-status   # RUNNING, 3 tasks
make sf-verify          # row counts, offset gaps, latency, sample row
make load-backfill      # optional: ~100 days of history into BRONZE via stage + COPY
make stop               # delete connector, stop containers, suspend warehouses
```

## The simulator

- **Entities:**
  - 12 Pune zones (H3 res-8 cells)
  - 300 restaurants (menus, prep times, ratings)
  - 20k customers (signups since launch, churn, heavy-tailed order frequency)
  - 400 drivers (shift templates)
- **Order lifecycle:** `PLACED → ACCEPTED → DRIVER_ASSIGNED / FOOD_READY → PICKED_UP → DELIVERED`.
  `CANCELLED` can happen any time before pickup: the restaurant rejects it, the customer
  cancels, or no driver is found within 10 minutes.
- **Patterns:**
  - lunch and dinner peaks; IT-park, nightlife and residential zone profiles; weekend effects
  - monsoon-aware rain: more demand, fewer drivers, slower roads
- **Chaos:** 2% duplicates, 3% late (1–30 min), 0.2% very late (2–6 h), 5% out-of-order.
  Every injected anomaly is logged to a ground-truth manifest.
- **Topics:** `fooddel.orders.v1`, `fooddel.driver_locations.v1`, `fooddel.reference.v1`.
  JSON Schemas are in [`simulator/schemas/`](simulator/schemas).

```text
python -m simulator run --help      # all options (mode, sink, speed, seed, --force-rain, --no-chaos...)
python -m simulator report <dir>    # volumes, KPIs, observed vs injected chaos
python -m simulator schemas         # regenerate JSON Schemas
```

Design rationale: [ADR 0001](docs/decisions/0001-simulator-design.md), [ADR 0002](docs/decisions/0002-kafka-schema-registry-snowflake-ingestion.md).

## Repo layout

```
simulator/      event generator (zones, demand, entities, engine, chaos, sinks, report)
infra/          docker-compose (Kafka, Schema Registry, Connect, kafka-ui), topic script, connector config
snowflake/      numbered, idempotent SQL: 01_setup, 02_rbac, 03_bronze, 04_backfill_copy, 90_verify_bronze
scripts/        sf.py (Snowflake runner, key-pair auth), connect.py (Kafka Connect REST)
dbt/ airflow/ ml/ streamlit/   later phases
docs/           architecture, ADRs, interview Q&A
tests/          pytest
```
