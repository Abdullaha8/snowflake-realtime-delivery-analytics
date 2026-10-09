import hashlib
import os
import subprocess
import sys

from .conftest import simulate


def test_same_seed_same_stream():
    a = simulate(days=0.05, seed=5)
    b = simulate(days=0.05, seed=5)
    assert a.values == b.values
    assert a.manifest == b.manifest


def test_different_run_seed_changes_traffic_not_world():
    a = simulate(days=0.05, seed=5)
    b = simulate(days=0.05, seed=6)
    assert a.values != b.values

    def snapshot(run):
        rows = [
            v["data"]
            for v in run.values
            if v["event_type"] == "RESTAURANT_UPDATED" and v["data"]["change_type"] == "SNAPSHOT"
        ]
        return {d["restaurant_id"]: d for d in rows}  # dedupe injected duplicate copies

    assert snapshot(a) == snapshot(b)


def _cli_hash(hash_seed: str) -> str:
    cmd = [
        sys.executable, "-m", "simulator", "run", "--mode", "backfill", "--start",
        "2026-08-14T19:00", "--days", "0.02", "--pings", "--seed", "9", "--sink", "stdout",
    ]  # fmt: skip
    env = {**os.environ, "PYTHONHASHSEED": hash_seed}
    out = subprocess.run(cmd, capture_output=True, check=True, env=env).stdout
    assert out.count(b"\n") > 1000
    return hashlib.sha256(out).hexdigest()


def test_output_independent_of_python_hash_seed():
    assert _cli_hash("1") == _cli_hash("2")
