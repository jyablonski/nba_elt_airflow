from datetime import datetime, timedelta

import pytest
from airflow import DAG
# from unittest.mock import patch, call
# from freezegun import freeze_time
from dags.nba_elt_pipeline_dag_dev import (
    jacobs_ecs_task,
    JACOBS_DEFAULT_ARGS,
    create_dag,
    jacobs_tags
)

from dags.utils import jacobs_slack_alert


# class test_nba_elt_pipeline_dag:
#     @freeze_time("2021-11-01")
#     @patch("dags.nba_elt_pipeline_dag_dev.jacobs_ecs_task")
#     def test_dag_argument(self, mock_jacobs_ecs_task):
#         dag = create_dag()
#         assert dag.schedule_interval == "0 11 * * *"
#         assert dag.catchup is False
#         assert dag.default_args == JACOBS_DEFAULT_ARGS
#         assert mock_jacobs_ecs_task.mock_calls[0] == call(dag, "2021-11-01")


@pytest.fixture(scope="session")
def nba_elt_pipeline_dag():
    test_dag = DAG(
        "jacobs_test_dag",
        default_args=JACOBS_DEFAULT_ARGS,
        start_date=datetime(2021, 11, 1),
        catchup=False,
        schedule_interval=None,
        tags=jacobs_tags,
    )

    return test_dag
