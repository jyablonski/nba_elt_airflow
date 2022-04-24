from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from utils import get_ssm_parameter, jacobs_slack_alert

# dbt test failure WILL fail the task, and fail the dag.

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
    "on_failure_callback": jacobs_slack_alert,
}

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
DBT_PROFILE_DIR = "~/.dbt/"
DBT_PROJECT_DIR = "~/airflow/dags/dbt/"

def jacobs_ecs_task(dag: DAG) -> ECSOperator:
    return ECSOperator(
        task_id="jacobs_airflow_ecs_task_dev",
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
                        },  # USE THESE TEMPLATE VARIABLES TO CREATE IDEMPOTENT TASKS / DAGS
                        {
                            "name": "run_type",
                            "value": "dev",                # you can do like if run_type == 'dev': S3_BUCKET=xxx_dev, RDS_SCHEMA=xxx_dev
                        },
                        {
                            "name": "S3_BUCKET",
                            "value": "jacobsbucket97_dev", # you can dynamically change this for dev/prod
                        },
                        {
                            "name": "RDS_SCHEMA",
                            "value": "nba_source_dev",     # you can dynamically change this for dev/prod
                        },
                    ],
                }
            ]
        },
        network_configuration=jacobs_network_config,
        awslogs_group="jacobs_ecs_logs_airflow",
        awslogs_stream_prefix="ecs/jacobs_container_airflow",
        do_xcom_push=True,
    )

# 3 ways of doing dbt as far as i know:
# 1) you put the entire dbt project locally in the airflow server somewhere and run it like below, and keep it updated on your own
# 2) run it as an ecs operator and keep a docker image in ecr to run the dbt build job
# 3) you have a paid dbt cloud plan and use dbt airflow provider to call the api & run the job from there, which just pulls from the git repo automatically

def jacobs_dbt_task1(dag: DAG) -> BashOperator:
    task_id = "dbt_deps_dev"

    return BashOperator(
        task_id=task_id,
        dag=dag,
        bash_command=f"dbt deps --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}",
    )


def jacobs_dbt_task2(dag: DAG) -> BashOperator:
    task_id = "dbt_seed_dev"

    return BashOperator(
        task_id=task_id,
        dag=dag,
        bash_command=f"dbt seed --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}",
    )


def jacobs_dbt_task3(dag: DAG) -> BashOperator:
    task_id = "dbt_run_dev"

    return BashOperator(
        task_id=task_id,
        dag=dag,
        bash_command=f"dbt run --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}",
    )


def jacobs_dbt_task4(dag: DAG) -> BashOperator:
    task_id = "dbt_test_dev"

    return BashOperator(
        task_id=task_id,
        dag=dag,
        bash_command=f"dbt test --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

# adding in framework for adding the ml pipeline in after dbt runs
# def jacobs_ecs_task_ml(dag: DAG) -> ECSOperator:
#     return ECSOperator(
#         task_id="jacobs_airflow_ecs_task_ml_dev",
#         dag=dag,
#         aws_conn_id="aws_ecs",
#         cluster="jacobs_fargate_cluster",
#         task_definition="jacobs_task_ml",
#         launch_type="FARGATE",
#         overrides={
#             "containerOverrides": [
#                 {
#                     "name": "jacobs_container_airflow",
#                     "environment": [
#                         {
#                             "name": "dag_run_ts",
#                             "value": "{{ ts }}",
#                         },  # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
#                         {
#                             "name": "dag_run_date",
#                             "value": " {{ ds }}",
#                         },  # USE THESE TO CREATE IDEMPOTENT TASKS / DAGS
#                         {
#                             "name": "run_type",
#                             "value": "dev",
#                         },
#                         {
#                             "name": "RDS_SCHEMA",
#                             "value": "ml_models_airflow",
#                         },
#                     ],
#                 }
#             ]
#         },
#         network_configuration=jacobs_network_config,
#         awslogs_group="jacobs_ecs_logs_airflow_ml",
#         awslogs_stream_prefix="ecs/jacobs_container_ml",
#         do_xcom_push=True,
#     )

def jacobs_email_task(dag: DAG) -> EmailOperator:
    task_id = "send_email_notification_dev"

    return EmailOperator(
        task_id=task_id,
        dag=dag,
        to="jyablonski9@gmail.com",
        subject="Airflow NBA ELT Pipeline DAG Run",
        html_content="""<h3>Process Completed</h3> <br>
        XCOM VAlue in ECS Task: {{ ti.xcom_pull(key="return_value", task_ids='jacobs_airflow_ecs_task_dev') }}

        """,
    )



def create_dag() -> DAG:
    """
    xxx
    """
    schedule_interval = "0 11 * * *"

    dag = DAG(
        "nba_elt_pipeline_dag_dev",
        catchup=False,
        default_args=JACOBS_DEFAULT_ARGS,
        schedule_interval=None,  # change to none when testing / schedule_interval | None
        start_date=datetime(2021, 11, 20),
        max_active_runs=1,
        tags=["nba_elt_pipeline", "dev", "ml"],
    )
    t1 = jacobs_ecs_task(dag)
    t2 = jacobs_dbt_task1(dag)
    t3 = jacobs_dbt_task2(dag)
    t4 = jacobs_dbt_task3(dag)
    t5 = jacobs_dbt_task4(dag)
    # t6 = jacobs_ecs_task_ml(dag)
    t7 = jacobs_email_task(dag)

    t1 >> t2 >> t3 >> t4 >> t5 >> t7

    return dag


dag = create_dag()
