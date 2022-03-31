from datetime import datetime, timedelta
from airflow import DAG
from unittest.mock import patch, call
from freezegun import freeze_time
from dags.nba_elt_pipeline_dag_dev import (
    jacobs_ecs_task,
    jacobs_dummy_task,
    JACOBS_DEFAULT_ARGS,
    create_dag,
)


class test_nba_elt_pipeline_dag:
    @freeze_time("2021-11-01")
    @patch("dags.nba_elt_pipeline_dag_dev.jacobs_ecs_task")
    def test_dag_argument(self, mock_jacobs_ecs_task):
        dag = create_dag()
        assert dag.schedule_interval == "0 11 * * *"
        assert dag.catchup is False
        assert dag.default_args == JACOBS_DEFAULT_ARGS
        assert mock_jacobs_ecs_task.mock_calls[0] == call(dag, "2021-11-01")


def test_nba_elt_pipeline_dag_dev_dag():
    test_dag = DAG("jacobs_test_dag", default_args={}, start_date=datetime(2021, 11, 1))
    jacobs_ecs_task(test_dag)
    assert len(test_dag.tasks) == 1
    assert test_dag.tasks[0].task_id == "jacobs_airflow_ecs_task_dev"
