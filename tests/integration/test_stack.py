"""Against the local docker stack (`make up`): topics, Schema Registry contract, Avro produce/
consume, Connect plugin. Run with `make test-integration`. Skipped if the stack is down."""

from __future__ import annotations

import copy
import json
import os
import urllib.request
import uuid

import pytest

from simulator import registry
from simulator.avro import avro_schema, avro_schemas, subject
from simulator.sinks import KafkaSink, avro_serializer

from ..simulator.conftest import simulate

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CONNECT = os.environ.get("CONNECT_URL", "http://localhost:8083")


def _get(url: str) -> object:
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


@pytest.fixture(scope="module", autouse=True)
def stack_up():
    try:
        _get(f"{REGISTRY}/subjects")
    except OSError:
        pytest.skip("local stack not running (make up)")
    registry.register_all(REGISTRY)


def test_topics_exist_with_retention():
    from confluent_kafka.admin import AdminClient, ConfigResource

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topics = admin.list_topics(timeout=10).topics
    expected = {
        "fooddel.orders.v1": 7,
        "fooddel.driver_locations.v1": 1,
        "fooddel.reference.v1": 7,
        "fooddel.dlq.v1": 7,
    }
    for name, days in expected.items():
        assert name in topics
        resource = ConfigResource(ConfigResource.Type.TOPIC, name)
        config = admin.describe_configs([resource])[resource].result(timeout=10)
        assert int(config["retention.ms"].value) == days * 86_400_000
        assert config["cleanup.policy"].value == "delete"


def test_unknown_topic_is_rejected_not_created():
    from confluent_kafka import Producer

    errors = []
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "message.timeout.ms": 5000})
    producer.produce(
        f"fooddel.typo-{uuid.uuid4().hex[:6]}", b"x", on_delivery=lambda e, _: errors.append(e)
    )
    producer.flush(15)
    assert errors and errors[0] is not None


def test_all_subjects_registered_with_backward_compatibility():
    subjects = set(registry.list_subjects(REGISTRY))
    for event_type in avro_schemas():
        subj = subject(event_type)
        assert subj in subjects
        assert _get(f"{REGISTRY}/config/{subj}")["compatibilityLevel"] == "BACKWARD"


def _with_new_field(field: dict) -> dict:
    schema = copy.deepcopy(avro_schema("ORDER_PLACED"))
    data = next(f for f in schema["fields"] if f["name"] == "data")
    data["type"]["fields"].append(field)
    return schema


def test_backward_compatibility_gate():
    subj = subject("ORDER_PLACED")
    # New optional field with a default: new readers can still read old records -> allowed.
    ok = _with_new_field({"name": "coupon_code", "type": ["null", "string"], "default": None})
    assert registry.is_compatible(REGISTRY, subj, ok)
    # New required field: old records have no value for it -> rejected.
    bad = _with_new_field({"name": "coupon_code", "type": "string"})
    assert not registry.is_compatible(REGISTRY, subj, bad)


def test_producer_refuses_unregistered_schema():
    from confluent_kafka.schema_registry import (
        SchemaRegistryClient,
        topic_record_subject_name_strategy,
    )
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.schema_registry.error import SchemaRegistryError
    from confluent_kafka.serialization import MessageField, SerializationContext

    changed = _with_new_field({"name": "coupon_code", "type": ["null", "string"], "default": None})
    serializer = AvroSerializer(
        SchemaRegistryClient({"url": REGISTRY}),
        json.dumps(changed),
        conf={
            "auto.register.schemas": False,
            "use.latest.version": False,
            "subject.name.strategy": topic_record_subject_name_strategy,
        },
    )
    # The subject exists (registered above) but this version of the schema does not.
    with pytest.raises(SchemaRegistryError, match="not found"):
        serializer({}, SerializationContext("fooddel.orders.v1", MessageField.VALUE))


def test_avro_round_trip_through_kafka():
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    run = simulate(days=0.01, seed=21, start="2026-08-14T20:00")  # ~15 sim-minutes
    sent = run.values
    topics = sorted({e.topic for e in run.events})

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"itest-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    start = []
    for t in topics:
        for p in consumer.list_topics(t, timeout=10).topics[t].partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(t, p), timeout=10)
            start.append(TopicPartition(t, p, high))

    sink = KafkaSink(BOOTSTRAP, serializer=avro_serializer(REGISTRY))
    for ev in run.events:
        sink.write(ev)
    sink.close()
    assert sink.delivered == len(sent)

    consumer.assign(start)
    deserialize = AvroDeserializer(SchemaRegistryClient({"url": REGISTRY}))
    received = []
    while len(received) < len(sent):
        msg = consumer.poll(10)
        assert msg is not None, f"only {len(received)}/{len(sent)} messages came back"
        assert msg.error() is None
        ctx = SerializationContext(msg.topic(), MessageField.VALUE)
        received.append(deserialize(msg.value(), ctx))
    consumer.close()

    def canon(values):
        return sorted(json.dumps(v, sort_keys=True) for v in values)

    assert canon(received) == canon(sent)


def test_connect_has_snowflake_plugin():
    plugins = _get(f"{CONNECT}/connector-plugins")
    classes = {p["class"]: p["version"] for p in plugins}
    assert "com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector" in classes
