"""Manage the Snowflake sink on the local Kafka Connect worker (stdlib only).

python scripts/connect.py deploy | status | pause | resume | delete
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

from _env import load_dotenv

CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
NAME = "fooddel_snowflake_sink"
CONFIG_PATH = Path(__file__).resolve().parents[1] / "infra" / "connect" / "snowflake-sink.json"
PLUGIN = "com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector"
DLQ_TOPIC = "fooddel.dlq.v1"
# Resolved inside the Connect container from ../.env; checked here so a missing value fails fast.
REQUIRED_ENV = [
    "SNOWFLAKE_ACCOUNT_URL",
    "SNOWFLAKE_CONNECTOR_USER",
    "SNOWFLAKE_CONNECTOR_PRIVATE_KEY",
    "SNOWFLAKE_DATABASE",
]


def call(method: str, path: str, body: dict | None = None) -> tuple[int, object]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{CONNECT_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, json.loads(raw) if raw else None


def deploy() -> int:
    missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        print(f"missing in .env: {', '.join(missing)} (then `make up` again so Connect sees them)")
        return 1
    _, plugins = call("GET", "/connector-plugins")
    if not any(p["class"] == PLUGIN for p in plugins or []):
        print(f"{PLUGIN} is not installed on the Connect worker")
        return 1
    config = json.loads(CONFIG_PATH.read_text())
    code, body = call("PUT", f"/connectors/{NAME}/config", config)  # create or update
    print(f"PUT config -> {code}")
    if code >= 300:
        print(json.dumps(body, indent=2))
        return 1
    return 0


def status() -> int:
    code, body = call("GET", f"/connectors/{NAME}/status")
    if code == 404:
        print(f"{NAME}: not deployed")
        return 1
    print(f"connector: {body['connector']['state']}")
    for task in body["tasks"]:
        print(f"  task {task['id']}: {task['state']}")
        if task.get("trace"):
            print("    " + task["trace"].splitlines()[0])
    healthy = body["connector"]["state"] == "RUNNING" and all(
        t["state"] == "RUNNING" for t in body["tasks"]
    )
    # errors.tolerance=all keeps the connector RUNNING while bad records go to the DLQ,
    # so RUNNING alone does not mean data is landing.
    depth, last_error = dlq_status()
    print(f"dead-letter queue {DLQ_TOPIC}: {depth} record(s)")
    if last_error:
        print(f"  latest error: {last_error[:200]}")
    return 0 if healthy else 1


def dlq_status() -> tuple[int, str | None]:
    """Record count in the DLQ and the error message of the newest record (if any)."""
    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError:
        return 0, None
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "group.id": "fooddel-dlq-inspector",
            "enable.auto.commit": False,
        }
    )
    try:
        tp = TopicPartition(DLQ_TOPIC, 0)
        low, high = consumer.get_watermark_offsets(tp, timeout=10)
        if high <= low:
            return 0, None
        consumer.assign([TopicPartition(DLQ_TOPIC, 0, high - 1)])
        msg = consumer.poll(10)
        headers = dict(msg.headers() or []) if msg is not None and not msg.error() else {}
        error = headers.get("__connect.errors.exception.message")
        return high - low, error.decode(errors="replace") if error else None
    finally:
        consumer.close()


def simple(method: str, suffix: str) -> int:
    code, body = call(method, f"/connectors/{NAME}{suffix}")
    print(f"{method} {NAME}{suffix} -> {code}")
    return 0 if code < 300 or code == 404 else 1


def main(argv: list[str]) -> int:
    load_dotenv()
    commands = {
        "deploy": deploy,
        "status": status,
        "pause": lambda: simple("PUT", "/pause"),
        "resume": lambda: simple("PUT", "/resume"),
        "delete": lambda: simple("DELETE", ""),
    }
    if len(argv) != 1 or argv[0] not in commands:
        print(__doc__)
        return 2
    try:
        return commands[argv[0]]()
    except urllib.error.URLError as e:
        print(f"Kafka Connect not reachable at {CONNECT_URL}: {e.reason}")
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
