from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from utils import get_ssm_parameter, jacobs_slack_alert

# dbt test failure WILL fail the task, and fail the dag.

JACOBS_DEFAULT_ARGS = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": jacobs_slack_alert,
}

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

def jacobs_dummy_task(dag: DAG, task_id) -> DummyOperator:
    task_id = "dummy_task_qa" + str(task_id)
    return DummyOperator(task_id=task_id, dag=dag)

# having problems installing meltano on airflow, gunicorn dependency issue as of 2022-03-21
def jacobs_meltano_task(dag: DAG) -> BashOperator:
    task_id = "meltano_task_qa"

    return BashOperator(
        task_id=task_id,
        dag=dag,
        bash_command=f"meltano elt tap-gitlab target-postgres --job_id=gitlab-to-postgres",
    )

### jacobs_dbt_task - do some dbt stuff here after meltano extracts the data and loads it into postgres

def jacobs_email_task(dag: DAG) -> EmailOperator:
    task_id = "send_email_notification_qa"

    return EmailOperator(
        task_id=task_id,
        dag=dag,
        to="jyablonski9@gmail.com",
        subject="Airflow NBA ELT Pipeline DAG Run",
        html_content="""<h3>Process Completed</h3> <br>
        XCOM VAlue in ECS Task: {{ ti.xcom_pull(key="return_value", task_ids='jacobs_airflow_ecs_task_qa') }}

        """,
    )



def create_dag() -> DAG:
    """
    xxx
    """
    schedule_interval = "0 11 * * *"

    dag = DAG(
        "meltano_pipeline_qa",
        catchup=False,
        default_args=JACOBS_DEFAULT_ARGS,
        schedule_interval=None,  # change to none when testing / schedule_interval | None
        start_date=datetime(2022, 3, 21),
        max_active_runs=1,
        tags=["qa", "meltano", "practice", "airflow_test"],
    )
    t1 = jacobs_dummy_task(dag, 1)
    t2 = jacobs_meltano_task(dag)
    t3 = jacobs_email_task(dag)
    t4 = jacobs_dummy_task(dag, 5)

    t1 >> t2 >> t3 >> t4

    return dag


dag = create_dag()
