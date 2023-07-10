from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from include.utils import get_schedule_interval, jacobs_slack_alert

# dbt test failure WILL fail the task, and fail the dag.

jacobs_default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "on_failure_callback": jacobs_slack_alert,
}

jacobs_tags = ["nba_elt_project"]

DBT_PROFILE_DIR = "~/.dbt/"
DBT_PROJECT_DIR = "~/airflow/dags/dbt/"


def jacobs_ecs_task(dag: DAG, network_config: dict) -> EcsRunTaskOperator:

    return EcsRunTaskOperator(
        task_id="jacobs_airflow_ecs_task",
        dag=dag,
        aws_conn_id="aws_ecs",
        cluster="jacobs_fargate_cluster",
        task_definition="jacobs_webscrape_task",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": "jacobs_container",
                    "environment": [
                        {"name": "dag_run_ts", "value": "{{ ts }}",},
                        {"name": "dag_run_date", "value": "{{ ds }}",},
                        {"name": "run_type", "value": "dev",},
                        {"name": "S3_BUCKET", "value": "jacobsbucket97",},
                        {"name": "RDS_SCHEMA", "value": "nba_source",},
                    ],
                }
            ]
        },
        network_configuration=network_config,
        awslogs_group="jacobs_ecs_logs_airflow",
        awslogs_stream_prefix="ecs/jacobs_container_airflow",
        do_xcom_push=True,
    )


def jacobs_ecs_task_dbt(
    dag: DAG, dbt_config: dict, network_config: dict
) -> EcsRunTaskOperator:

    return EcsRunTaskOperator(
        task_id="jacobs_airflow_dbt_task",
        dag=dag,
        aws_conn_id="aws_ecs",
        cluster="jacobs_fargate_cluster",
        task_definition="jacobs_dbt_task",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": "jacobs_container_dbt",
                    "environment": [
                        {"name": "dag_run_ts", "value": "{{ ts }}",},
                        {"name": "dag_run_date", "value": " {{ ds }}",},
                        {"name": "run_type", "value": "dev",},
                        {"name": "DBT_DBNAME", "value": dbt_config["DBT_DBNAME"],},
                        {"name": "DBT_HOST", "value": dbt_config["DBT_HOST"],},
                        {"name": "DBT_USER", "value": dbt_config["DBT_USER"],},
                        {"name": "DBT_PASS", "value": dbt_config["DBT_PASS"],},
                        {"name": "DBT_SCHEMA", "value": dbt_config["DBT_SCHEMA"],},
                        {"name": "DBT_PRAC_KEY", "value": dbt_config["DBT_PRAC_KEY"],},
                    ],
                }
            ]
        },
        network_configuration=network_config,
        awslogs_group="jacobs_ecs_logs_dbt",
        awslogs_stream_prefix="ecs/jacobs_container_dbt",
        do_xcom_push=True,
    )


def jacobs_ecs_task_ml(dag: DAG, network_config: dict) -> EcsRunTaskOperator:

    return EcsRunTaskOperator(
        task_id="jacobs_airflow_ecs_task_ml",
        dag=dag,
        aws_conn_id="aws_ecs",
        cluster="jacobs_fargate_cluster",
        task_definition="jacobs_ml_task",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": "jacobs_container_ml",
                    "environment": [
                        {"name": "dag_run_ts", "value": "{{ ts }}",},
                        {"name": "dag_run_date", "value": " {{ ds }}",},
                        {"name": "run_type", "value": "dev",},
                        {"name": "RDS_SCHEMA", "value": "ml_models",},
                    ],
                }
            ]
        },
        network_configuration=network_config,
        awslogs_group="jacobs_ecs_logs_ml",
        awslogs_stream_prefix="ecs/jacobs_container_ml",
        do_xcom_push=True,
    )


def jacobs_email_task(dag: DAG) -> EmailOperator:
    task_id = "send_email_notification"

    return EmailOperator(
        task_id=task_id,
        dag=dag,
        to="jyablonski9@gmail.com",
        subject="Airflow NBA ELT Pipeline DAG Run",
        html_content="""<h3>Process Completed</h3> <br>
        XCOM VAlue in ECS Task: {{ ti.xcom_pull(key="return_value", task_ids='jacobs_airflow_ecs_task') }}

        """,
    )


def create_dag() -> DAG:
    """
    xxx
    """
    jacobs_network_config = {
        "awsvpcConfiguration": {
            "securityGroups": ["1"],
            "subnets": ["2", "3",],
            "assignPublicIp": "ENABLED",
        }
    }

    jacobs_dbt_vars = {
        "DBT_DBNAME": 1,
        "DBT_HOST": 2,
        "DBT_USER": 3,
        "DBT_PASS": 4,
        "DBT_SCHEMA": 5,
        "DBT_PRAC_KEY": 6,
    }

    schedule_interval = "0 11 * * *"

    dag = DAG(
        "nba_elt_pipeline_dag",
        catchup=False,
        default_args=jacobs_default_args,
        schedule_interval=get_schedule_interval(
            None
        ),  # change to none when testing / schedule_interval | None
        start_date=datetime(2021, 11, 20),
        max_active_runs=1,
        tags=jacobs_tags,
    )
    t1 = jacobs_ecs_task(dag, jacobs_network_config)
    t2 = jacobs_ecs_task_dbt(dag, jacobs_dbt_vars, jacobs_network_config)
    t3 = jacobs_ecs_task_ml(dag, jacobs_network_config)
    t4 = jacobs_email_task(dag)

    t1 >> t2 >> t3 >> t4

    return dag


dag = create_dag()
