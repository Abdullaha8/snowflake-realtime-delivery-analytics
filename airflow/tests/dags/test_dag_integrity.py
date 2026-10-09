"""DAG integrity: every DAG imports, and follows the project's conventions.
Run inside the Astro image: `make airflow-test` (astro dev pytest)."""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_pipeline_shape(dagbag):
    # dagbag.dags, not get_dag(): in Airflow 3 get_dag() also queries the metadata DB.
    dag = dagbag.dags.get("fooddel_pipeline")
    assert dag is not None
    assert dag.catchup is False and dag.max_active_runs == 1
    order = [
        "check_new_data",
        "refresh_dynamic_tables",
        "dbt_build",
        "data_quality",
        "record_latency",
        "record_run",
    ]
    assert set(dag.task_ids) == set(order)
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert downstream in dag.get_task(upstream).downstream_task_ids, (upstream, downstream)


def test_conventions(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        for t in dag.tasks:
            assert t.owner == "data-eng", f"{dag_id}.{t.task_id} owner"
    pipeline = dagbag.dags["fooddel_pipeline"]
    # data_quality must not retry: the same data gives the same answer
    assert pipeline.get_task("data_quality").retries == 0
    assert pipeline.get_task("refresh_dynamic_tables").retries == 2
    assert pipeline.get_task("record_run").trigger_rule == "all_done"
