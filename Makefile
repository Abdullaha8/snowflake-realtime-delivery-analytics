# Real-Time Food Delivery Analytics Platform
# Recipes are single commands so they work from PowerShell/cmd and from bash (CI).

ifeq ($(OS),Windows_NT)
  PY := ".venv/Scripts/python.exe"
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

.PHONY: help install up build down stop topics schemas schemas-register schemas-check \
        connector-up connector-status connector-down sf-keygen sf-setup sf-verify sf-suspend \
        load-backfill sf-reset-bronze sf-dt dt-status dt-refresh dt-demo dt-normal dt-suspend         dt-resume sf-verify-dt reconcile sim sim-sample sim-backfill sim-report lint format test test-integration clean

help:  ## list targets
	@$(PY) -c "import re;[print(f'  {m[0]:<18}{m[1]}') for m in re.findall(r'^([a-z-]+):.*?## (.*)$$', open('Makefile').read(), re.M)]"

install:  ## create .venv (Python 3.11) and install deps
	$(BOOTSTRAP_PY) -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[kafka,snowflake,dev]"

# --- local streaming stack ---------------------------------------------------------------
# No --build here: rebuilding on every start re-checks Docker Hub, so a Hub hiccup would stop
# a local stack whose image is already built. Compose still builds the image if it is missing.
up:  ## start Kafka, Schema Registry, Connect, kafka-ui (creates topics); connector NOT started
	$(COMPOSE) up -d --wait

build:  ## rebuild the Kafka Connect image (after changing infra/connect/Dockerfile)
	$(COMPOSE) build connect

down:  ## remove containers (keeps the Kafka volume)
	$(COMPOSE) down

stop:  ## delete connector, stop containers, suspend Dynamic Tables + warehouses
	-$(PY) scripts/connect.py delete
	-$(COMPOSE) stop
	-$(PY) scripts/sf.py dt suspend
	-$(PY) scripts/sf.py suspend

topics:  ## end offsets per topic-partition (= messages produced)
	docker exec fooddel-kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic-partitions "fooddel.*"

# --- schemas -----------------------------------------------------------------------------
schemas:  ## regenerate JSON Schemas + Avro .avsc from the pydantic models
	$(PY) -m simulator schemas --out simulator/schemas

schemas-register:  ## register Avro schemas in Schema Registry (BACKWARD per subject)
	$(PY) -m simulator registry register

schemas-check:  ## are the current schemas compatible with what is registered? (CI gate)
	$(PY) -m simulator registry check

# --- Snowflake (needs .env; costs credits only where noted) ------------------------------
sf-keygen:  ## generate admin + connector RSA key pairs in secrets/ and print next steps
	$(PY) scripts/sf.py keygen

sf-setup:  ## run 01_setup, 02_rbac, 03_bronze (idempotent; warehouses are created suspended)
	$(PY) scripts/sf.py run snowflake/01_setup.sql snowflake/02_rbac.sql snowflake/03_bronze.sql

connector-up:  ## deploy/update the Snowflake sink on Kafka Connect (starts ingestion)
	$(PY) scripts/connect.py deploy

connector-status:  ## connector + task state
	$(PY) scripts/connect.py status

connector-down:  ## delete the connector (offsets are kept; redeploy resumes)
	$(PY) scripts/connect.py delete

sf-verify:  ## BRONZE row counts, offset gaps, latency (uses WH_INGEST briefly)
	$(PY) scripts/sf.py verify

load-backfill:  ## PUT data/backfill to the stage and COPY into BRONZE (uses WH_INGEST)
	$(PY) scripts/sf.py load-backfill

sf-suspend:  ## suspend all project warehouses
	$(PY) scripts/sf.py suspend

sf-reset-bronze:  ## DEV only: truncate the BRONZE tables (before a reconciliation run)
	$(PY) scripts/sf.py reset-bronze

# --- Dynamic Tables (P3) -----------------------------------------------------------------
sf-dt:  ## (re)create SILVER + GOLD Dynamic Tables (initial build uses WH_TRANSFORM)
	$(PY) scripts/sf.py run snowflake/05_silver.sql snowflake/06_gold.sql

dt-status:  ## state, refresh mode (must be INCREMENTAL) and lag of every Dynamic Table
	$(PY) scripts/sf.py dt status

dt-refresh:  ## refresh GOLD now (and the SILVER tables it reads)
	$(PY) scripts/sf.py dt refresh

dt-demo:  ## GOLD lag -> 1 minute (demos only: ~1 credit/hour while data flows)
	$(PY) scripts/sf.py dt lag "1 minute"

dt-normal:  ## GOLD lag -> 10 minutes (project default)
	$(PY) scripts/sf.py dt lag "10 minutes"

dt-suspend:  ## suspend all Dynamic Tables (no refreshes, no cost)
	$(PY) scripts/sf.py dt suspend

dt-resume:  ## resume all Dynamic Tables
	$(PY) scripts/sf.py dt resume

sf-verify-dt:  ## data-quality checks + headline numbers on SILVER/GOLD
	$(PY) scripts/sf.py verify-dt

reconcile:  ## produce a seeded run and compare SILVER/GOLD with simulator ground truth
	$(PY) scripts/reconcile.py

# --- simulator ---------------------------------------------------------------------------
sim:  ## live simulator, auto-stops after DURATION wall seconds (SINK=stdout|jsonl|kafka)
	$(PY) -m simulator run --mode live --sink $(SINK) --duration $(DURATION) --speed $(SPEED) --out $(OUT) $(ARGS)

sim-sample:  ## 3 simulated hours of Friday dinner rush -> data/sample (jsonl), then report
	$(PY) -m simulator run --mode backfill --start 2026-10-09T19:00 --days 0.125 --pings --seed 42 --sink jsonl --out data/sample --overwrite $(ARGS)
	$(PY) -m simulator report data/sample

sim-backfill:  ## history since launch (2026-07-01) -> data/backfill (gzipped jsonl, no pings)
	$(PY) -m simulator run --mode backfill --sink jsonl --gzip --out data/backfill --overwrite $(ARGS)

sim-report:  ## summarise a jsonl run: make sim-report OUT=data/sample
	$(PY) -m simulator report $(OUT)

# --- quality -----------------------------------------------------------------------------
lint:  ## ruff + black --check + sqlfluff
	$(PY) -m ruff check simulator scripts tests
	$(PY) -m black --check simulator scripts tests
	$(PY) -m sqlfluff lint snowflake

format:  ## ruff --fix + black + sqlfluff fix
	$(PY) -m ruff check --fix simulator scripts tests
	$(PY) -m black simulator scripts tests
	$(PY) -m sqlfluff fix snowflake -f

# dbt test joins this target in P4, once there is a dbt project.
test: lint  ## lint + unit tests (+ dbt test from P4)
	$(PY) -m pytest

test-integration:  ## tests against the local docker stack (run `make up` first)
	$(PY) -m pytest -m integration

clean:  ## delete generated data and caches
	$(PY) -c "import shutil;[shutil.rmtree(p, ignore_errors=True) for p in ['data', '.pytest_cache', '.ruff_cache']]"
