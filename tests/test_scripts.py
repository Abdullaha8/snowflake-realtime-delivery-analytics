"""scripts/: .env loading, SQL placeholder rendering, connector config sanity."""

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import _env  # noqa: E402
import connect  # noqa: E402
import sf  # noqa: E402


def test_load_dotenv_does_not_override_existing(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("# comment\nFOO_A=1\nFOO_B = two\nFOO_C='quoted'\nnot a pair\n")
    monkeypatch.setenv("FOO_A", "already-set")
    monkeypatch.delenv("FOO_B", raising=False)
    monkeypatch.delenv("FOO_C", raising=False)
    _env.load_dotenv(env)
    import os

    assert os.environ["FOO_A"] == "already-set"
    assert os.environ["FOO_B"] == "two"
    assert os.environ["FOO_C"] == "quoted"


def test_render_fills_snowflake_placeholders(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "FOODDEL_DEV")
    assert (
        sf.render("USE ${SNOWFLAKE_DATABASE}.BRONZE; SELECT $1")
        == "USE FOODDEL_DEV.BRONZE; SELECT $1"
    )


def test_render_fails_on_missing_placeholder(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_NOPE", raising=False)
    with pytest.raises(SystemExit, match="SNOWFLAKE_NOPE"):
        sf.render("${SNOWFLAKE_NOPE}")


def test_every_sql_placeholder_is_documented_in_env_example():
    documented = {
        line.split("=", 1)[0]
        for line in (ROOT / ".env.example").read_text().splitlines()
        if "=" in line and not line.startswith("#")
    }
    for path in (ROOT / "snowflake").glob("*.sql"):
        for name in sf.PLACEHOLDER.findall(path.read_text()):
            assert name in documented, f"{path.name} uses ${{{name}}}, missing from .env.example"


def test_connector_config_secrets_come_from_env():
    config = json.loads(connect.CONFIG_PATH.read_text())
    assert config["snowflake.private.key"] == "${env:SNOWFLAKE_CONNECTOR_PRIVATE_KEY}"
    assert config["snowflake.enable.schematization"] == "false"
    # 4.x defaults to a v3-migration safety check that rejects a fresh v4 config at startup
    assert config["snowflake.streaming.validate.compatibility.with.classic"] == "false"
    # must match the producer's subject naming (simulator.avro.subject)
    assert config["value.converter.value.subject.name.strategy"].endswith("TopicRecordNameStrategy")
    topics = config["topics"].split(",")
    mapped = dict(pair.split(":") for pair in config["snowflake.topic2table.map"].split(","))
    assert set(topics) == set(mapped)
    for var in connect.REQUIRED_ENV:
        assert any(f"${{env:{var}}}" == v for v in config.values()), var


def test_deploy_fails_fast_without_snowflake_env(monkeypatch, capsys):
    for var in connect.REQUIRED_ENV:
        monkeypatch.delenv(var, raising=False)
    assert connect.deploy() == 1
    assert "missing in .env" in capsys.readouterr().out


def test_keygen_writes_keys_and_prints_single_line_bodies(tmp_path, monkeypatch, capsys):
    from cryptography.hazmat.primitives import serialization

    monkeypatch.setattr(sf, "SECRETS", tmp_path)
    monkeypatch.setattr(sf, "ROOT", tmp_path)
    assert sf.keygen() == 0
    out = capsys.readouterr().out
    for name in ("admin", "connector"):
        key = serialization.load_pem_private_key(
            (tmp_path / f"{name}_rsa_key.p8").read_bytes(), None
        )
        assert key.key_size == 2048
    line = next(x for x in out.splitlines() if "SNOWFLAKE_CONNECTOR_PRIVATE_KEY=" in x)
    assert "-----" not in line and " " not in line.split("=", 1)[1]
    # second run keeps existing keys
    before = (tmp_path / "connector_rsa_key.p8").read_bytes()
    sf.keygen()
    assert (tmp_path / "connector_rsa_key.p8").read_bytes() == before
