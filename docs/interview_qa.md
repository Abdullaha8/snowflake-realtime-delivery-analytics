# Interview Q&A

## P1: Simulator

**Why build a simulator instead of using a public dataset?**
Public delivery datasets are static CSVs. I needed a *stream* that contains the problems a
real pipeline faces: late, duplicated and out-of-order events. I also needed a ground-truth
record of exactly which events were tampered with, so I can prove the pipeline handles them.

**How do you generate realistic out-of-order events?**
Random jitter of a few seconds almost never reorders lifecycle events that are minutes apart.
Instead, I hold a chosen event until the next event with the same Kafka key is sent, then send
it right after. That guarantees a real inversion, e.g. `DELIVERED` arriving before
`PICKED_UP`.

**How do you know your dedup or late-data handling works?**
Every injected anomaly is logged to a manifest (`event_id`, type, delay) and never flagged in
the event itself. Tests compare the observed stream to the manifest: duplicate copies equal
manifest duplicates exactly, and late and out-of-order rates match the configuration within
tolerance. Downstream DQ checks can be scored the same way.

**Why two timestamps?**
`event_ts` is when it happened (business logic, windows, SLA). `emitted_ts` is when the
producer sent it. The gap between them is the lateness, and the file partitions are by
`emitted_ts`, like ingestion-time partitions.

**Why H3?**
It gives uniform hexagonal cells with cheap neighbour queries (`grid_ring`) for driver
search, and Snowflake has native H3 functions. Zones are named groups of res-8 cells, so
dashboards can say "Koregaon Park" rather than a cell ID.

**Why is the promised ETA deliberately naive?**
It models today's rule-based ETA. It knows traffic but ignores weather and kitchen load, which
gives a 30% breach rate in rain vs 17% in clear weather. That's the gap the P6 ML model has to
close.

**How is it reproducible?**
A fixed `world_seed` keeps restaurants, customers and drivers the same in every run. A run
`seed` controls traffic. No set iteration happens anywhere, so the same seed gives
byte-identical output even across Python hash seeds; a test checks this.
