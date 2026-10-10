# ADR 0008: Dashboards, Terraform and CI

- **Status:** Accepted (Phase 8, 2026-10-10)
- **Scope:** `streamlit/`, `infra/terraform/`, `.github/workflows/ci.yml`

## Decisions

### 1. One Streamlit in Snowflake app, owned by FOODDEL_BI
- **Tabs:**
  - **Live ops:** surge by zone, SLA breaches, orders at risk (GOLD)
  - **Business KPIs:** GMV, AOV, cohort retention, top restaurants (MARTS_SECURE)
  - **ETA model:** daily MAE vs the promised ETA, model versions (ML)
  - **Pipeline health:** runs, DQ results, latency, alerts (OPS)
- **Owned by the BI role**, so the app runs with BI's rights and can only ever read governed
  data. PII is masked inside the app too.
- **Queries live in `streamlit/queries.py`.** `make streamlit-smoke` runs all 12 as
  FOODDEL_BI (with secondary roles off), which tests the app's data access without a browser.
- **Query results are cached for 10 minutes**, the same as the GOLD target lag, so browsing
  tabs doesn't keep WH_BI busy.
- **Deployment:** `make streamlit-deploy` PUTs the files to an internal stage, then runs
  `CREATE STREAMLIT … FROM '@stage'` (the current syntax; `ROOT_LOCATION` is legacy) and adds
  a live version.
- **Runtime pinned to `SYSTEM$WAREHOUSE_RUNTIME`.** New accounts default `CREATE STREAMLIT` to
  the container runtime. That runtime needs a `pyproject.toml` and bills a compute pool
  (`SYSTEM_COMPUTE_POOL_CPU`) while the app runs, and the first deploy failed with "pyproject.toml
  does not exist". The warehouse runtime uses `environment.yml` and runs on WH_BI (XSMALL, 60 s
  auto-suspend), which is simpler and cheaper for a single-user demo.

*Rejected:* a separate dashboard tool. Streamlit in Snowflake needs no hosting, no extra
credentials, and inherits Snowflake RBAC.

### 2. Terraform: a reusable environment module, validate-only
- **Pins:** `infra/terraform/modules/environment` uses provider `snowflakedb/snowflake` 2.21.0
  and Terraform 1.16.3, both at least two weeks old.
- **One module call per environment** creates the database, schemas, XSMALL warehouses
  (60 s auto-suspend, initially suspended), a resource monitor per warehouse, the functional
  roles rolled up to SYSADMIN, and their database and warehouse grants.
- **`main.tf` instantiates PROD as the example.** CI runs `fmt -check`, `init -backend=false`
  and `validate`; nothing is applied from this repo.
- **DEV was built with the idempotent SQL scripts** (`snowflake/01-10`), and those remain its
  source of truth.
  - Importing DEV into Terraform state would cost hours for no portfolio value.
  - Terraform is the promotion path for new environments.
  - The SQL scripts also cover what the provider handles less well here: Dynamic Tables,
    Alerts, secure views and the Model Registry.

### 3. GitHub Actions CI, without cloud credentials
- **Job `test`:** `make install` + `make test` (ruff, black, sqlfluff, pytest, offline
  `dbt parse`).
- **Job `terraform`:** `fmt -check`, `init -backend=false`, `validate`.
- **Kept out of CI:**
  - Snowflake-dependent checks (dbt data tests, reconciliations, the governance matrix,
    backtests) run from the Makefile against a real account. In a team, they'd run in a
    deploy job with a service user's key stored as a CI secret.
  - The Astro DAG integrity tests stay local (`make airflow-test`), because they need the
    Astro image.

## Trade-offs
- **No real PROD environment**, so the PROD Terraform has never been applied. It's validated
  only, by design.
- **CI hasn't run yet.** The repo has no remote; the workflow runs on the first push.
- **I didn't view the Streamlit UI from here.** Its data access is proven by the smoke test,
  and the app was deployed and set live.
