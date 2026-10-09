# Kafka Connect (Confluent 7.8.2 - a version Snowflake tests the connector against) with the
# Snowflake Connector for Kafka v4, Confluent package (needed for Schema Registry / Avro).
# The Avro converter ships with cp-kafka-connect.

ARG CONNECTOR_VERSION=4.1.0

FROM alpine:3.20 AS download
ARG CONNECTOR_VERSION
WORKDIR /tmp
RUN apk add --no-cache curl unzip \
 && base=https://sfc-repo.snowflakecomputing.com/kafka-connector/confluent/java/${CONNECTOR_VERSION} \
 && zip=snowflakeinc-snowflake-kafka-connector-${CONNECTOR_VERSION}.zip \
 && curl -fsSLO "$base/$zip" \
 && curl -fsSL "$base/$zip.sha256" | awk '{print $1}' > expected.sha256 \
 && echo "$(cat expected.sha256)  $zip" | sha256sum -c - \
 && unzip -q "$zip" -d /plugins

FROM confluentinc/cp-kafka-connect:7.8.2
COPY --from=download /plugins /usr/share/confluent-hub-components
