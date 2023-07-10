from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from include.aws_utils import get_ssm_parameter
from include.utils import get_schedule_interval, jacobs_slack_alert

# ECR is a service that exists outside your VPC, so (when using fargate) you need one of the following for the network connection to ECR to be established:
# Public IP.
# NAT Gateway, with a route to the NAT Gateway in the subnet.
# ECR Interface VPC Endpoint, with a route to the endpoint in the subnet.

jacobs_default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "on_failure_callback": jacobs_slack_alert,
}


def jacobs_ecs_task(
    dag: DAG, network_config: dict, park_id="{{ params['park'] }}"
) -> EcsRunTaskOperator:

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
                    "name": "jacobs_hello_world_container",
                    "environment": [
                        {"name": "dag_run_ts", "value": "{{ ts }}"},
                        {"name": "dag_run_date", "value": " {{ ds }}"},
                        {
                            "name": "park_id",
                            "value": f"'{park_id}'",
                        },  # cant send an integer; has to be a string.
                    ],  # although, it will appear as an int for the container
                }
            ]
        },
        network_configuration=network_config,
        awslogs_group="/ecs/hello-world-test",
        awslogs_stream_prefix="ecs/jacobs_hello_world_container",
        do_xcom_push=True,
    )


def create_dag() -> DAG:
    """
    xxx
    """
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
    schedule_interval = "0 11 * * *"

    dag = DAG(
        "aws_ecs_template",
        catchup=False,
        default_args=jacobs_default_args,
        schedule_interval=get_schedule_interval(None),
        start_date=datetime(2021, 11, 20),
        max_active_runs=1,
        tags=["example", "template"],
        params={"park": 0},
        render_template_as_native_obj=True,
    )
    t1 = jacobs_ecs_task(dag, jacobs_network_config)

    t1

    return dag


dag = create_dag()
