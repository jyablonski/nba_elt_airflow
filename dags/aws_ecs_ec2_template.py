from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from include.utils import get_ssm_parameter, jacobs_slack_alert

jacobs_default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    # "on_failure_callback": jacobs_slack_alert,
}


def jacobs_ecs_ec2_task(dag: DAG, network_config: dict) -> EcsRunTaskOperator:

    return EcsRunTaskOperator(
        task_id="jacobs_airflow_ecs_ec2_task_dev",
        dag=dag,
        aws_conn_id="aws_ecs_ec2",
        cluster="jacobs-ecs-ec2-cluster",
        task_definition="hello-world-ec2",
        launch_type="EC2",
        # launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {  # change this to any of the task_definitons created in ecs
                    "name": "hello-world-ec2",
                    "environment": [
                        {
                            "name": "dag_run_ts",
                            "value": "{{ ts }}",
                        },  # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
                        {"name": "dag_run_date", "value": " {{ ds }}",},
                    ],
                }
            ],
            "executionRoleArn": "arn:aws:iam::288364792694:role/jacobs_ecs_role",
            "taskRoleArn": "arn:aws:iam::288364792694:role/jacobs_ecs_role",
        },
        network_configuration=network_config,
        awslogs_group="/ecs/hello-world-ec2",
        awslogs_stream_prefix="ecs/hello-world-ec2",
        # this ^ stream prefix shit is prefixed with ecs/ followed by the container name which comes from line 48.
        do_xcom_push=True,
    )


def create_dag() -> DAG:
    """
    xxx
    """
    # jacobs_network_config = {
    #     "awsvpcConfiguration": {
    #         "securityGroups": [get_ssm_parameter("jacobs_ssm_sg_task")],
    #         "subnets": [
    #             get_ssm_parameter("jacobs_ssm_subnet1"),
    #             get_ssm_parameter("jacobs_ssm_subnet2"),
    #         ],
    #         "assignPublicIp": "ENABLED",
    #     } # has to be enabled otherwise it cant pull image from ecr??
    # }
    schedule_interval = "0 11 * * *"
    jacobs_network_config = {
        "awsvpcConfiguration": {
            "securityGroups": ["1"],
            "subnets": ["2", "3",],
            "assignPublicIp": "ENABLED",
        }  # has to be enabled otherwise it cant pull image from ecr??
    }

    dag = DAG(
        "aws_ecs_ec2_template",
        catchup=False,
        default_args=jacobs_default_args,
        schedule_interval=None,  # change to none when testing / schedule_interval | None
        start_date=datetime(2021, 11, 20),
        max_active_runs=1,
        tags=["dev", "ecs", "template"],
    )

    jacobs_ecs_ec2_task(dag, jacobs_network_config)

    return dag


dag = create_dag()
