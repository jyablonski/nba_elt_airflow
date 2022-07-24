from datetime import datetime, timedelta
from airflow import DAG

import pytest
from dags.dev.nba_elt_pipeline_dag_dev import (
    jacobs_ecs_task,
    jacobs_default_args,
    create_dag,
    jacobs_tags,
)

from dags.utils import jacobs_slack_alert


def test_nba_elt_pipeline_dag(nba_elt_pipeline_dag):
    jacobs_ecs_task(nba_elt_pipeline_dag)
    assert len(nba_elt_pipeline_dag.tasks) == 1
    assert nba_elt_pipeline_dag.catchup is False
    assert nba_elt_pipeline_dag.tasks[0].task_id == "jacobs_airflow_ecs_task_dev"
    assert nba_elt_pipeline_dag.schedule_interval == None
    assert nba_elt_pipeline_dag.tags == jacobs_tags
    # assert nba_elt_pipeline_dag.default_args['on_failure_callback'] == jacobs_slack_alert
