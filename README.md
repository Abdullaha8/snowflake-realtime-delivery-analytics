# Real-Time Food Delivery Analytics Platform (Pune)

A Swiggy/Zomato-style delivery marketplace, simulated in Pune, feeding a Snowflake pipeline
for near-real-time SLA breach detection, surge signals, ETA prediction and historical KPIs.

**Pipeline:** Python simulator → Kafka (KRaft) → Snowflake Kafka Connector → BRONZE → Dynamic
Tables + dbt → Snowpark ML → Streamlit. See [docs/architecture.md](docs/architecture.md).

| Phase | | |
|---|---|---|
| P1 | ✅ | Repo scaffold, Makefile, Kafka docker-compose, event simulator + tests |
| P2–P8 | ⏳ | Ingestion, Dynamic Tables, dbt, Airflow, ML, governance, dashboards |

## Quick start

Requirements: Python 3.11, GNU make (Windows: `winget install ezwinports.make`), and Docker
Desktop (only for Kafka).

```bash
make install        # .venv + dependencies
make test           # ruff + black --check + pytest
make sim-sample     # 3 simulated hours of Friday dinner rush -> data/sample, then a report
make sim SPEED=60 DURATION=60 ARGS="--start 2026-10-09T20:00"   # live to stdout, 1 sim-hour/min
make sim-backfill   # history since launch (2026-07-01) -> data/backfill/*.jsonl.gz

# with Kafka
cp .env.example .env
make up             # Kafka on localhost:9092, kafka-ui on http://localhost:8080
make sim SINK=kafka DURATION=300
make stop           # always stop when done
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

Design rationale: [ADR 0001](docs/decisions/0001-simulator-design.md).

## Repo layout

```
simulator/      event generator (zones, demand, entities, engine, chaos, sinks, report)
infra/          docker-compose (Kafka + kafka-ui); terraform/ later
snowflake/ dbt/ airflow/ ml/ streamlit/   later phases
docs/           architecture, ADRs, interview Q&A
tests/          pytest
```
