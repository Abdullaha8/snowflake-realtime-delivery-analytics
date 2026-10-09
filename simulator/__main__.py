"""CLI: python -m simulator {run,report,schemas} ..."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
from dataclasses import replace
from pathlib import Path

from simulator.chaos import ChaosConfig
from simulator.config import SimConfig
from simulator.engine import Simulator, make_run_id
from simulator.report import report_dir
from simulator.sinks import JsonlSink, KafkaSink, ManifestWriter, StdoutSink


def _run(args: argparse.Namespace) -> int:
    cfg = SimConfig(
        mode=args.mode,
        start=args.start,
        duration_s=args.duration,
        speed=args.speed,
        days=args.days,
        max_eps=args.max_eps or None,
        seed=args.seed if args.seed is not None else random.SystemRandom().randrange(2**32),
        world_seed=args.world_seed,
        emit_pings=args.pings if args.pings is not None else args.mode == "live",
        force_rain=args.force_rain,
        chaos=ChaosConfig.disabled() if args.no_chaos else ChaosConfig(),
    )
    if args.n_drivers:
        cfg = replace(cfg, n_drivers=args.n_drivers)

    run_id = make_run_id(cfg.mode, cfg.seed)
    out = Path(args.out)
    if args.sink == "jsonl":
        sink = JsonlSink(out, run_id, compress=args.gzip)
        manifest_path = out / "_manifest" / f"{run_id}.jsonl"
    else:
        sink = (
            StdoutSink()
            if args.sink == "stdout"
            else KafkaSink(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        )
        manifest_path = Path("data/manifests") / f"{run_id}.jsonl"
    sim = Simulator(cfg, sink, run_id=run_id, manifest=ManifestWriter(manifest_path))

    print(f"[simulator] run_id={sim.run_id} seed={sim.seed} sink={args.sink}", file=sys.stderr)
    try:
        stats = sim.run()
    except KeyboardInterrupt:
        print("[simulator] interrupted", file=sys.stderr)
        return 130
    summary = {
        "run_id": stats.run_id,
        "emitted": stats.emitted,
        "orders_placed": stats.orders_placed,
        "events_by_type": dict(stats.events_by_type),
        "anomalies": dict(stats.anomalies),
        "wall_seconds": round(stats.wall_seconds, 1),
        "manifest": str(manifest_path),
    }
    print(f"[simulator] done {json.dumps(summary)}", file=sys.stderr)
    return 0


def _report(args: argparse.Namespace) -> int:
    print(report_dir(Path(args.path)))
    return 0


def _schemas(args: argparse.Namespace) -> int:
    from simulator.events import json_schemas

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for event_type, schema in json_schemas().items():
        (out / f"{event_type.lower()}.schema.json").write_text(json.dumps(schema, indent=2) + "\n")
    print(f"wrote {len(json_schemas())} schemas to {out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="simulator", description="Pune food-delivery event simulator")
    sub = p.add_subparsers(dest="command", required=True)

    r = sub.add_parser("run", help="generate events")
    r.add_argument("--mode", choices=["live", "backfill"], default="live")
    r.add_argument("--sink", choices=["stdout", "jsonl", "kafka"], default="stdout")
    r.add_argument("--out", default="data/sim", help="jsonl output directory")
    r.add_argument("--gzip", action="store_true", help="gzip jsonl files")
    r.add_argument("--start", help="simulated start, ISO-8601 (naive = IST)")
    r.add_argument("--duration", type=float, default=600, help="live: wall seconds (auto-stop)")
    r.add_argument("--speed", type=float, default=1.0, help="live: sim seconds per wall second")
    r.add_argument("--days", type=float, help="backfill: simulated days")
    r.add_argument("--max-eps", type=float, default=500, help="live: events/sec cap (0 = none)")
    r.add_argument("--seed", type=int, help="run seed (default random)")
    r.add_argument("--world-seed", type=int, default=42)
    r.add_argument("--n-drivers", type=int)
    r.add_argument(
        "--pings",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="emit DRIVER_LOCATION (default: on for live, off for backfill)",
    )
    r.add_argument("--force-rain", action="store_true")
    r.add_argument("--no-chaos", action="store_true", help="disable late/dup/out-of-order")
    r.set_defaults(func=_run)

    rep = sub.add_parser("report", help="summarise a jsonl run directory")
    rep.add_argument("path")
    rep.set_defaults(func=_report)

    sch = sub.add_parser("schemas", help="export JSON Schemas")
    sch.add_argument("--out", default="simulator/schemas")
    sch.set_defaults(func=_schemas)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
