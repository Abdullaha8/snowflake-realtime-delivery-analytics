"""The Avro contract: generated from the pydantic models, committed, and able to carry every
event the simulator produces without changing it."""

import io
import json

import fastavro
import pytest

from simulator.avro import AVRO_DIR, avro_schemas, full_name, schema_json, subject
from simulator.events import EVENT_SPECS

from .conftest import simulate


def test_committed_avsc_files_match_the_models():
    """Changing an event model without `make schemas` fails CI (contract drift)."""
    for event_type, schema in avro_schemas().items():
        path = AVRO_DIR / f"{event_type.lower()}.avsc"
        assert path.exists(), f"{path.name} missing: run `make schemas`"
        assert path.read_text(encoding="utf-8") == schema_json(schema), f"{path.name} is stale"


def test_schemas_are_valid_avro():
    for schema in avro_schemas().values():
        fastavro.parse_schema(schema)


def test_subject_names_follow_topic_record_name_strategy():
    assert subject("ORDER_PLACED") == "fooddel.orders.v1-com.fooddel.events.OrderPlaced"
    assert full_name("DRIVER_LOCATION") == "com.fooddel.events.DriverLocation"
    assert len({subject(et) for et in EVENT_SPECS}) == len(EVENT_SPECS)


def test_nullable_fields_have_null_defaults():
    """Optional fields default to null, which keeps future additions BACKWARD compatible."""
    data = next(f for f in avro_schemas()["DRIVER_LOCATION"]["fields"] if f["name"] == "data")
    zone = next(f for f in data["type"]["fields"] if f["name"] == "zone_id")
    assert zone["type"] == ["null", "string"] and zone["default"] is None


@pytest.fixture(scope="module")
def one_of_each() -> dict[str, dict]:
    run = simulate(days=0.25, seed=8, start="2026-08-14T17:00")
    events: dict[str, dict] = {}
    for v in run.values:
        events.setdefault(v["event_type"], v)
    return events


def test_every_event_type_round_trips_unchanged(one_of_each):
    assert set(one_of_each) == set(EVENT_SPECS), "simulation should produce every event type"
    schemas = {et: fastavro.parse_schema(s) for et, s in avro_schemas().items()}
    for event_type, value in one_of_each.items():
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schemas[event_type], value)
        buf.seek(0)
        decoded = fastavro.schemaless_reader(buf, schemas[event_type])
        # Same JSON as the JSONL backfill -> identical RECORD_CONTENT in BRONZE
        assert json.dumps(decoded, sort_keys=True) == json.dumps(value, sort_keys=True)
