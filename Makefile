# Real-Time Food Delivery Analytics Platform
# Recipes are single commands so they work from PowerShell/cmd and from bash (CI).

ifeq ($(OS),Windows_NT)
  PY := ".venv/Scripts/python.exe"  # quoted: cmd.exe needs it for forward slashes
  BOOTSTRAP_PY := py -3.11
else
  PY := .venv/bin/python
  BOOTSTRAP_PY := python3.11
endif

-include .env
export

COMPOSE := docker compose -f infra/docker-compose.yml

# make sim SINK=kafka DURATION=300 SPEED=60 ARGS="--force-rain"
SINK ?= stdout
DURATION ?= 300
SPEED ?= 1
ARGS ?=
OUT ?= data/sim

.PHONY: help install up down stop sim sim-sample sim-backfill sim-report schemas lint format test clean

help:  ## list targets
	@$(PY) -c "import re;[print(f'  {m[0]:<14}{m[1]}') for m in re.findall(r'^([a-z-]+):.*?## (.*)$$', open('Makefile').read(), re.M)]"

install:  ## create .venv (Python 3.11) and install deps
	$(BOOTSTRAP_PY) -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[kafka,dev]"

up:  ## start Kafka (KRaft) + kafka-ui on localhost:9092 / :8080
	$(COMPOSE) up -d

down:  ## remove containers (keeps the Kafka volume)
	$(COMPOSE) down

stop:  ## stop containers (Snowflake warehouse suspend is added in P2)
	-$(COMPOSE) stop

sim:  ## live simulator, auto-stops after DURATION wall seconds
	$(PY) -m simulator run --mode live --sink $(SINK) --duration $(DURATION) --speed $(SPEED) --out $(OUT) $(ARGS)

sim-sample:  ## 3 simulated hours of Friday dinner rush -> data/sample (jsonl), then report
	$(PY) -m simulator run --mode backfill --start 2026-10-09T19:00 --days 0.125 --pings --seed 42 --sink jsonl --out data/sample $(ARGS)
	$(PY) -m simulator report data/sample

sim-backfill:  ## history since launch (2026-07-01) -> data/backfill (gzipped jsonl, no pings)
	$(PY) -m simulator run --mode backfill --sink jsonl --gzip --out data/backfill $(ARGS)

sim-report:  ## summarise a jsonl run: make sim-report OUT=data/sample
	$(PY) -m simulator report $(OUT)

schemas:  ## export JSON Schemas per event type -> simulator/schemas
	$(PY) -m simulator schemas --out simulator/schemas

lint:  ## ruff + black --check
	$(PY) -m ruff check simulator tests
	$(PY) -m black --check simulator tests

format:  ## ruff --fix + black
	$(PY) -m ruff check --fix simulator tests
	$(PY) -m black simulator tests

# dbt test and sqlfluff join this target in P3/P4, once there is SQL to check.
test: lint  ## lint + pytest (+ dbt test + sqlfluff from P4)
	$(PY) -m pytest

clean:  ## delete generated data and caches
	$(PY) -c "import shutil;[shutil.rmtree(p, ignore_errors=True) for p in ['data', '.pytest_cache', '.ruff_cache']]"
