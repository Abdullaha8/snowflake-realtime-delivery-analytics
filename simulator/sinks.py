"""Where events go: stdout, JSONL files, Kafka, or an in-memory list (tests)."""

from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path
from typing import IO, Any, Protocol

from simulator.events import OutEvent


def dumps(value: dict) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


class Sink(Protocol):
    def write(self, ev: OutEvent) -> None: ...
    def close(self) -> None: ...


class ListSink:
    def __init__(self) -> None:
        self.events: list[OutEvent] = []

    def write(self, ev: OutEvent) -> None:
        self.events.append(ev)

    def close(self) -> None:
        pass


class StdoutSink:
    def __init__(self, stream: IO[str] | None = None) -> None:
        self.stream = stream or sys.stdout

    def write(self, ev: OutEvent) -> None:
        self.stream.write(dumps(ev.value) + "\n")

    def close(self) -> None:
        self.stream.flush()


class JsonlSink:
    """<out>/<topic>/dt=<emitted UTC date>/part-<run_id>.jsonl[.gz]

    Partitioned by *emitted* date (like an ingestion-time partition), so late events land
    in a later partition than their event_ts - exactly what downstream has to cope with.
    """

    def __init__(self, out_dir: Path, run_id: str, compress: bool = False) -> None:
        self.out_dir = Path(out_dir)
        self.run_id = run_id
        self.compress = compress
        self._files: dict[tuple[str, str], IO[str]] = {}

    def _file(self, topic: str, dt: str) -> IO[str]:
        f = self._files.get((topic, dt))
        if f is None:
            path = self.out_dir / topic / f"dt={dt}"
            path.mkdir(parents=True, exist_ok=True)
            name = f"part-{self.run_id}.jsonl"
            if self.compress:
                f = gzip.open(path / f"{name}.gz", "at", encoding="utf-8")  # noqa: SIM115
            else:
                f = open(path / name, "a", encoding="utf-8")  # noqa: SIM115
            self._files[(topic, dt)] = f
        return f

    def write(self, ev: OutEvent) -> None:
        self._file(ev.topic, ev.value["emitted_ts"][:10]).write(dumps(ev.value) + "\n")

    def close(self) -> None:
        for f in self._files.values():
            f.close()
        self._files.clear()


class KafkaSink:
    """confluent-kafka producer. Idempotent producer + acks=all: the broker will not create
    duplicates on retry, so every duplicate downstream is one the simulator injected."""

    def __init__(self, bootstrap_servers: str, producer: Any | None = None) -> None:
        if producer is None:
            from confluent_kafka import Producer  # imported lazily: optional dependency

            producer = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "enable.idempotence": True,
                    "acks": "all",
                    "linger.ms": 50,
                    "compression.type": "lz4",
                    "client.id": "fooddel-simulator",
                }
            )
        self.producer = producer
        self.errors: list[str] = []
        self.delivered = 0

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self.errors.append(str(err))
        else:
            self.delivered += 1

    def write(self, ev: OutEvent) -> None:
        kwargs = {
            "topic": ev.topic,
            "key": ev.key.encode(),
            "value": dumps(ev.value).encode(),
            "on_delivery": self._on_delivery,
        }
        try:
            self.producer.produce(**kwargs)
        except BufferError:  # local queue full: let it drain, then retry once
            self.producer.poll(1.0)
            self.producer.produce(**kwargs)
        self.producer.poll(0)

    def close(self) -> None:
        remaining = self.producer.flush(30)
        if remaining or self.errors:
            raise RuntimeError(
                f"Kafka delivery failed: {remaining} undelivered, errors={self.errors[:5]}"
            )


class ManifestWriter:
    """Ground-truth log of injected anomalies (one JSON object per line)."""

    def __init__(self, path: Path | None) -> None:
        self.records: list[dict] = []
        self._f: IO[str] | None = None
        if path is not None:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._f = open(path, "w", encoding="utf-8")  # noqa: SIM115

    def __call__(self, record: dict) -> None:
        if self._f is not None:
            self._f.write(dumps(record) + "\n")
        else:
            self.records.append(record)

    def close(self) -> None:
        if self._f is not None:
            self._f.close()
