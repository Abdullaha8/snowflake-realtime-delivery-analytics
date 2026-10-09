# Astro Runtime 3.3-8 = Airflow 3.3.2 (pinned; released 2026-09-23).
FROM astrocrpublic.azurecr.io/runtime:3.3-8

# dbt lives in its own virtualenv so its dependencies can never clash with Airflow's.
# The DAG calls /usr/local/airflow/dbt_venv/bin/dbt; the dbt project itself is mounted from
# ../dbt (docker-compose.override.yml), so dbt code changes need no image rebuild.
RUN python -m venv /usr/local/airflow/dbt_venv \
 && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.5 dbt-snowflake==1.12.1
