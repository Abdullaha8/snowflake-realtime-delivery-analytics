# ADR 0001: Event simulator design

- **Status:** Accepted (Phase 1, 2026-10-10)
- **Scope:** `simulator/`

## Context

No real data exists for a Pune food-delivery marketplace, so we generate it. The data must
exercise every later phase:

- P2 ingestion (keys, partitions, schemas)
- P3 real-time logic (dedup, late data, lifecycle, demand vs supply, SLA, surge)
- P4 history (months of orders for cohorts, slowly-changing restaurant attributes)
- P6 ETA model (realistic, learnable signal)
- P7 governance (PII to mask)

It also has to be cheap to run (trial credits) and every choice has to be explainable in an
interview.

## Decisions

### 1. Discrete-event simulation on a simulated clock
A priority queue of future actions (place order → restaurant decision → dispatch → food ready
→ pickup → deliver), all driven by one simulated clock.
- **Live mode** maps simulated time onto the wall clock, optionally accelerated (`--speed 60`
  = one simulated hour per minute), and stops itself after `--duration`.
- **Backfill mode** runs as fast as the CPU allows: ~100 days since launch in ~1.5 min.

*Rejected:* a fixed-tick `while True: sleep(1)` loop. It can't do backfill, its timing is
coarse, and it couples realism to wall time.

### 2. Two seeds: `world_seed` (fixed, 42) and run `seed` (random by default)
The world is restaurants, customers, drivers and their attributes, and it is identical across
runs, so dimension keys stay stable across live and backfill runs. Each run's traffic differs.
Any run is fully reproducible with `--seed`. Sets are never iterated (dicts only), so the
output is byte-identical even across `PYTHONHASHSEED` values; a test checks this.

### 3. One envelope for every event
`event_id, event_type, schema_version, event_ts, emitted_ts, source, data{...}`
- `event_id` (UUID) is the dedup key.
- `event_ts` vs `emitted_ts` separates event time from send time.
- `schema_version` leaves room for evolution.
- In Snowflake BRONZE the envelope becomes typed columns; `data` stays VARIANT.

Payloads are pydantic models (`extra="forbid"`), so the simulator can't emit an invalid event.
`python -m simulator schemas` exports JSON Schema, which P2 can register.

### 4. Three topics, keyed for ordering
| Topic | Key | Events |
|---|---|---|
| `fooddel.orders.v1` | `order_id` | PLACED, ACCEPTED, DRIVER_ASSIGNED, FOOD_READY, PICKED_UP, DELIVERED, CANCELLED |
| `fooddel.driver_locations.v1` | `driver_id` | DRIVER_LOCATION, every 30 s while online (~95% of volume) |
| `fooddel.reference.v1` | entity id | RESTAURANT_UPDATED (snapshot + changes → SCD2), CUSTOMER_REGISTERED (PII), WEATHER_UPDATED |

- Keying by `order_id` puts one order's lifecycle in one partition.
- Pings get their own topic so their volume and retention can be tuned separately.

*Rejected:* one topic per event type, which means 11 topics and makes lifecycle joins harder.
*Rejected:* a single topic, which mixes 30-second pings with rare reference data.

### 5. Zones = named neighbourhoods built from H3 resolution-8 cells
- There are 12 Pune neighbourhoods. Each zone is `grid_disk(centre, k=2)`, about 19 cells of
  ~0.5 km edge.
- Where two disks overlap, a cell goes to the nearest centre. This gives a deterministic
  `cell → zone` map.
- Events carry raw `lat/lng` plus `h3_r8`. P3 can recompute the cell in Snowflake
  (`H3_LATLNG_TO_CELL`) and check that the two agree.
- Drivers between zones have `zone_id = null`, which is realistic and tests null handling.
- Driver dispatch searches H3 rings outward from the restaurant (k = 0..8).

*Rejected:* hand-drawn polygons, which are hard to maintain and need point-in-polygon logic.
*Rejected:* raw res-7 parent cells, which have no business names for dashboards.

### 6. Demand and supply models: simple multiplicative curves
**Demand.** Orders per zone per minute follow a Poisson process with rate:

`λ = base × hour_curve(IST) × zone_profile(hour, weekend) × growth(active customers) × weather`

- The hour curve peaks at lunch (~13:00) and at dinner (~21:00, the bigger peak).
- Zone profiles: IT-park zones are lunch-heavy on weekdays; nightlife zones are dinner- and
  late-night-heavy, and busier on weekends.
- Growth comes from customer signups since launch (2026-07-01) minus churn. Customers have
  heavy-tailed order frequency, which gives P4 real cohort curves.

**Supply.** Drivers work shift templates. Supply follows demand, but more weakly. The gaps at
the peaks, and at 08:00–09:00, produce real SLA breaches and NO_DRIVER cancellations.

**Rain.** A CLEAR ↔ RAIN Markov chain, with a higher rain probability in Jun–Sep. Rain raises
demand, cuts supply and slows travel.

**Promised ETA.** It deliberately ignores weather and noise. This creates the error the P6
model must beat.

Measured on a 21-day backfill:

| | All | Clear | Rain |
|---|---|---|---|
| Delivered | 91% | — | — |
| SLA breach | 19% | 17% | 30% |
| NO_DRIVER cancel | 1% | 0.5% | 3.8% |

### 7. Chaos is applied *after* generation, with a ground-truth manifest
The engine emits a perfect, event-time-ordered stream. `ChaosStage` then decides each event's
**emit time**:

| Anomaly | Default | Applies to | Mechanism |
|---|---|---|---|
| Duplicate | 2% | all | Identical copy (same `event_id`) 0–5 s later (producer retry) |
| Late | 3% | `driver_app` | Emitted 1–30 min after `event_ts` (phone offline) |
| Very late | 0.2% | `driver_app` | Emitted 2–6 h later (tests late-data cutoffs) |
| Out-of-order | 5% | all | Held until the *next event with the same key* is sent, then sent after it, so a real swap is guaranteed (released after 10 min if nothing follows) |

- Every injected anomaly is written to `_manifest/<run_id>.jsonl` and is never flagged inside
  the event. Tests, and later P5 data-quality checks, can measure detection against ground
  truth.
- Kafka uses an idempotent producer with `acks=all`, so every duplicate downstream is one we
  injected on purpose.

*Rejected:* random emit jitter. With lifecycle steps minutes apart, jitter seldom swaps
events, so the out-of-order rate would be a fiction.

### 8. JSONL files partitioned by *emitted* date
`<out>/<topic>/dt=YYYY-MM-DD/part-<run_id>.jsonl[.gz]` is an ingestion-time partition layout,
so late events land in a later partition than their event time. Backfill can be loaded into
BRONZE with `COPY INTO` instead of streaming 1.3 M events through Kafka.

## Trade-offs and limitations
- **City-wide weather.** Real rain is local; per-zone weather is an easy extension.
- **Simplified driver movement.** Drivers travel in straight lines (road distance = 1.3 ×
  straight-line) and idle drivers don't reposition, so supply drifts toward where deliveries
  end.
- **No surge feedback.** The simulator's `surge_fee_inr` uses its own internal state but
  doesn't feed back into demand. The P3 surge signal is computed independently from events.
- **Live runs only see live signups.** They emit CUSTOMER_REGISTERED only for signups during
  the run; earlier customers come from the backfill.
- **Restaurant changes reset each run.** Changes start from the world baseline every run, so
  SCD2 history is only coherent within a single backfill.
- **Python throughput.** ~14 k events/s in backfill, and live mode is capped at 500 EPS. That
  is ample for a portfolio, not a load test.
