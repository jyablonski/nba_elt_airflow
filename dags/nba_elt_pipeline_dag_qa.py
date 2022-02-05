from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from utils import get_ssm_parameter

jacobs_network_config = {
    "awsvpcConfiguration": {
        "securityGroups": [get_ssm_parameter("jacobs_ssm_sg_task")],
        "subnets": [
            get_ssm_parameter("jacobs_ssm_subnet1"),
            get_ssm_parameter("jacobs_ssm_subnet2"),
        ],
        "assignPublicIp": "ENABLED",
    }
}

JACOBS_DEFAULT_ARGS = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
DBT_PROFILE_DIR = "~/.dbt/"
DBT_PROJECT_DIR = "~/airflow/dags/dbt/"


def jacobs_dummy_task(dag: DAG, task_id) -> DummyOperator:
    task_id = "dummy_task_qa" + str(task_id)
    return DummyOperator(task_id=task_id, dag=dag)


def jacobs_ecs_task(dag: DAG) -> ECSOperator:
    return ECSOperator(
        task_id="jacobs_airflow_ecs_task_qa",
        dag=dag,
        aws_conn_id="aws_ecs",
        cluster="jacobs_fargate_cluster",
        task_definition="jacobs_task_airflow",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": "jacobs_container_airflow",
                    "environment": [
                        {
                            "name": "dag_run_ts",
                            "value": "{{ ts }}",
                        },  # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
                        {
                            "name": "dag_run_date",
                            "value": " {{ ds }}",
                        },  # USE THESE TO CREATE IDEMPOTENT TASKS / DAGS
                    ],
                }
            ]
        },
        network_configuration=jacobs_network_config,
        awslogs_group="aws_ecs_logs",
    )


def jacobs_dbt_task1(dag: DAG) -> BashOperator:
    task_id = "dbt_deps_qa"

    return BashOperator(
        task_id=task_id,
        dag=dag,
        bash_command=f"dbt deps --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}",
    )


def jacobs_dbt_task2(dag: DAG) -> BashOperator:
    task_id = "dbt_seed_qa"

    return BashOperator(
        task_id=task_id,
        dag=dag,
        bash_command=f"dbt seed --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}",
    )


def jacobs_dbt_task3(dag: DAG) -> BashOperator:
    task_id = "dbt_run_qa"

    return BashOperator(
        task_id=task_id,
        dag=dag,
        bash_command=f"dbt run --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}",
    )


def jacobs_dbt_task4(dag: DAG) -> BashOperator:
    task_id = "dbt_test_qa"

    return BashOperator(
        task_id=task_id,
        dag=dag,
        bash_command=f"dbt test --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}",
    )


def jacobs_email_task(dag: DAG) -> EmailOperator:
    task_id = "send_email_notification_qa"

    return EmailOperator(
        task_id=task_id,
        dag=dag,
        to="jyablonski9@gmail.com",
        subject="Airflow NBA ELT Pipeline DAG Run",
        html_content="<h3>Process Completed</h3>",
    )


def create_dag() -> DAG:
    """
    xxx
    """
    schedule_interval = "0 11 * * *"

    dag = DAG(
        "nba_elt_pipeline_dag_qa",
        catchup=False,
        default_args=JACOBS_DEFAULT_ARGS,
        schedule_interval=None,  # change to none when testing / schedule_interval | None
        start_date=datetime(2021, 11, 20),
        max_active_runs=1,
        tags=["nba_elt_pipeline", "qa"],
    )
    t1 = jacobs_dummy_task(dag, 1)
    t2 = jacobs_ecs_task(dag)
    t3 = jacobs_dummy_task(dag, 2)
    t4 = jacobs_dummy_task(dag, 3)
    t5 = jacobs_dummy_task(dag, 4)
    t6 = jacobs_dbt_task1(dag)
    t7 = jacobs_dbt_task2(dag)
    t8 = jacobs_dbt_task3(dag)
    t9 = jacobs_dbt_task4(dag)
    t10 = jacobs_email_task(dag)
    t11 = jacobs_dummy_task(dag, 5)

    t1 >> t2 >> [t3, t4, t5] >> t6 >> t7 >> t8 >> t9 >> [t10, t11]

    return dag


dag = create_dag()

#     # jacobs_ge_task = GreatExpectationsOperator(
#     task_id='jacobs_ge_task',
#     expectation_suite_name='my_suite',
#     batch_kwargs={
#         'table': 'my_table',
#         'datasource': 'my_datasource'
#     }
# )
