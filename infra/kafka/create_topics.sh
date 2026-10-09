#!/bin/bash
# Creates the platform topics explicitly (broker auto-create is OFF). Idempotent.
# RF=1 because this is a single local broker; production would use RF=3, min.insync.replicas=2.
set -euo pipefail

BOOTSTRAP=${BOOTSTRAP:-kafka:19092}
DAY_MS=86400000

create() {  # name partitions retention_ms
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 \
    --config retention.ms="$3" --config cleanup.policy=delete
  # --if-not-exists skips existing topics, so re-apply config to fix drift
  kafka-configs --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics --entity-name "$1" \
    --add-config retention.ms="$3",cleanup.policy=delete
}

# Order lifecycle: 7 days of replay.
create fooddel.orders.v1 3 $((7 * DAY_MS))
# GPS pings: ~95% of volume and only useful while fresh.
create fooddel.driver_locations.v1 3 $((1 * DAY_MS))
# Restaurant/customer/weather changes. "delete", not "compact": compaction would keep only the
# latest version per key and erase the change history SCD2 snapshots need.
create fooddel.reference.v1 3 $((7 * DAY_MS))
# Records the Snowflake sink could not convert (dead-letter queue).
create fooddel.dlq.v1 1 $((7 * DAY_MS))

kafka-topics --bootstrap-server "$BOOTSTRAP" --list
