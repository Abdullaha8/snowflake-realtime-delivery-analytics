"""Avro schemas derived from the pydantic event models (single source of truth).

One Avro record per event type: the envelope fields plus a typed `data` record. Field names
and JSON shapes match the JSON events exactly, so a record decoded by the Kafka Connect
AvroConverter lands in Snowflake as the same RECORD_CONTENT as a COPY-loaded JSONL line.

Type mapping: str/Literal -> string (enums are painful to evolve), int -> long,
float -> double, bool -> boolean, X | None -> ["null", X] with default null,
list[T] -> array<T>, nested model -> record. Timestamps stay ISO-8601 strings.
"""

from __future__ import annotations

import json
import types
import typing
from pathlib import Path

from pydantic import BaseModel

from simulator.events import EVENT_SPECS

NAMESPACE = "com.fooddel.events"
AVRO_DIR = Path(__file__).parent / "schemas" / "avro"


def record_name(event_type: str) -> str:
    """ORDER_PLACED -> OrderPlaced"""
    return "".join(part.capitalize() for part in event_type.split("_"))


def full_name(event_type: str) -> str:
    return f"{NAMESPACE}.{record_name(event_type)}"


def _avro_type(annotation: object, defined: set[str]) -> object:
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin in (typing.Union, types.UnionType):
        non_null = [a for a in args if a is not type(None)]
        if len(non_null) != 1:
            raise TypeError(f"unsupported union {annotation}")
        return ["null", _avro_type(non_null[0], defined)]
    if origin is typing.Literal:
        return "string"
    if origin is list:
        return {"type": "array", "items": _avro_type(args[0], defined)}
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return _record(annotation, annotation.__name__, defined)
    simple = {str: "string", int: "long", float: "double", bool: "boolean"}
    if annotation in simple:
        return simple[annotation]
    raise TypeError(f"no Avro mapping for {annotation!r}")


def _record(model: type[BaseModel], name: str, defined: set[str]) -> object:
    if name in defined:  # Avro allows a named type only once per schema; reuse by name
        return name
    defined.add(name)
    fields = []
    for field_name, info in model.model_fields.items():
        avro_t = _avro_type(info.annotation, defined)
        field: dict = {"name": field_name, "type": avro_t}
        if isinstance(avro_t, list) and avro_t[0] == "null":
            field["default"] = None
        fields.append(field)
    return {"type": "record", "name": name, "fields": fields}


def avro_schema(event_type: str) -> dict:
    spec = EVENT_SPECS[event_type]
    defined: set[str] = set()
    data = _record(spec.model, f"{record_name(event_type)}Data", defined)
    return {
        "type": "record",
        "name": record_name(event_type),
        "namespace": NAMESPACE,
        "doc": f"{event_type} on topic {spec.topic} (key: {spec.key_field})",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "event_type", "type": "string"},
            {"name": "schema_version", "type": "long"},
            {"name": "event_ts", "type": "string", "doc": "when it happened (ISO-8601 UTC)"},
            {"name": "emitted_ts", "type": "string", "doc": "when it was sent (ISO-8601 UTC)"},
            {"name": "source", "type": "string"},
            {"name": "data", "type": data},
        ],
    }


def avro_schemas() -> dict[str, dict]:
    return {et: avro_schema(et) for et in EVENT_SPECS}


def schema_json(schema: dict) -> str:
    return json.dumps(schema, indent=2) + "\n"


def write_avro_schemas(out_dir: Path = AVRO_DIR) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for event_type, schema in avro_schemas().items():
        path = out_dir / f"{event_type.lower()}.avsc"
        path.write_text(schema_json(schema), encoding="utf-8")
        paths.append(path)
    return paths


def subject(event_type: str) -> str:
    """TopicRecordNameStrategy: several record types share one topic, each with its own
    subject (and therefore its own compatibility history)."""
    return f"{EVENT_SPECS[event_type].topic}-{full_name(event_type)}"
