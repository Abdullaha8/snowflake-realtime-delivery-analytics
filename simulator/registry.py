"""Confluent Schema Registry client (stdlib only): register the event contract, check changes.

`register`: set BACKWARD compatibility per subject, then register the current schemas.
`check`:    ask the registry whether the current schemas are compatible with the latest
            registered versions, without registering anything (the CI gate for schema changes).
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

from simulator.avro import avro_schemas, subject

DEFAULT_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
COMPATIBILITY = "BACKWARD"
_CT = "application/vnd.schemaregistry.v1+json"


class RegistryError(RuntimeError):
    pass


def _call(method: str, url: str, body: dict | None = None) -> dict | list:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": _CT})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        raise RegistryError(f"{method} {url} -> {e.code}: {e.read().decode()}") from e


def set_compatibility(url: str, subj: str, level: str = COMPATIBILITY) -> None:
    _call("PUT", f"{url}/config/{subj}", {"compatibility": level})


def register(url: str, subj: str, schema: dict) -> int:
    resp = _call("POST", f"{url}/subjects/{subj}/versions", {"schema": json.dumps(schema)})
    return int(resp["id"])


def is_compatible(url: str, subj: str, schema: dict) -> bool:
    try:
        resp = _call(
            "POST",
            f"{url}/compatibility/subjects/{subj}/versions/latest",
            {"schema": json.dumps(schema)},
        )
    except RegistryError as e:
        if "40401" in str(e):  # subject not registered yet: nothing to be incompatible with
            return True
        raise
    return bool(resp["is_compatible"])


def list_subjects(url: str) -> list[str]:
    return list(_call("GET", f"{url}/subjects"))


def register_all(url: str = DEFAULT_URL) -> dict[str, int]:
    """Idempotent: re-registering an identical schema returns the existing id."""
    ids = {}
    for event_type, schema in avro_schemas().items():
        subj = subject(event_type)
        set_compatibility(url, subj)
        ids[subj] = register(url, subj, schema)
    return ids


def check_all(url: str = DEFAULT_URL) -> dict[str, bool]:
    return {
        subject(et): is_compatible(url, subject(et), schema)
        for et, schema in avro_schemas().items()
    }
