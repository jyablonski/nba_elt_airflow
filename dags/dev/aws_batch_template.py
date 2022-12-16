from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from utils import get_ssm_parameter, jacobs_slack_alert

# dbt test failure WILL fail the task, and fail the dag.

jacobs_default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
    # "on_failure_callback": jacobs_slack_alert,
}

def jacobs_ecs_task(dag: DAG) -> BatchOperator:

    return BatchOperator(
        task_id='submit_batch_job',
        dag=dag,
        job_name="jacobs-job2",              # can be named anything
        job_queue="jacobs-batch-queue",           # has to be setup in aws batch
        job_definition="jacobs-job-definition", # has to be setup in aws batch
        overrides={
        'environment': [
            {
                'name': 'string',
                'value': 'string'
            },
        ]
    },
    )


def create_dag() -> DAG:
    """
    xxx
    """
    schedule_interval = "0 11 * * *"

    dag = DAG(
        "aws_batch_template",
        catchup=False,
        default_args=jacobs_default_args,
        schedule_interval=None,  # change to none when testing / schedule_interval | None
        start_date=datetime(2021, 11, 20),
        max_active_runs=1,
        tags=["dev", "ecs", "template"],
    )
    t1 = jacobs_ecs_task(dag)

    t1

    return dag


dag = create_dag()
