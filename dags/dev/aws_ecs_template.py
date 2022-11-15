from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
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

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

def jacobs_ecs_task(dag: DAG) -> EcsRunTaskOperator:
    return EcsRunTaskOperator(
        task_id="jacobs_airflow_ecs_task_dev",
        dag=dag,
        aws_conn_id="aws_ecs",
        cluster="jacobs_fargate_cluster",
        task_definition="hello-world-test",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": "jacobs_hello_world_container",  # change this to any of the task_definitons created in ecs
                    "environment": [
                        {
                            "name": "dag_run_ts",
                            "value": "{{ ts }}",
                        },  # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
                        {
                            "name": "dag_run_date",
                            "value": " {{ ds }}",
                        }
                    ],
                }
            ]
        },
        network_configuration=jacobs_network_config,
        awslogs_group="/ecs/hello-world-test",
        awslogs_stream_prefix="ecs/jacobs_hello_world_container",
        # this ^ stream prefix shit is prefixed with ecs/ followed by the container name which comes from line 48.
        do_xcom_push=True,
    )


def create_dag() -> DAG:
    """
    xxx
    """
    schedule_interval = "0 11 * * *"

    dag = DAG(
        "aws_ecs_template",
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
