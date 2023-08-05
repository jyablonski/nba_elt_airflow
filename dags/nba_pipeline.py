from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from include.aws_utils import get_ssm_parameter
from include.utils import get_schedule_interval, get_instance_type, jacobs_slack_alert

default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "on_failure_callback": jacobs_slack_alert,
}

# set these on each dev / stg / prod airflow instance
# doing this with ssm or secrets manager for ecs + batch tasks is a fucking bitch
# and it constantly grabs the secrets everytime airflow refreshes
instance_type_env = get_instance_type()
ecs_cluster_env = f"ecs_cluster_{get_instance_type()}"
network_config_env = Variable.get(
    "network_config", deserialize_json=True, default_var={}
)
dbt_config_env = Variable.get(
    "dbt_config",
    deserialize_json=True,
    default_var={
        "DBT_DBNAME": "test",
        "DBT_HOST": "test",
        "DBT_USER": "test",
        "DBT_PASS": "test",
        "DBT_SCHEMA": "test",
        "DBT_PRAC_KEY": "test",
    },
)


@dag(
    "nba_pipeline",
    schedule=get_schedule_interval(None),
    start_date=datetime(2023, 7, 1),
    catchup=False,
    default_args=default_args,
    tags=["nba_elt_project"],
)
def pipeline():
    def ingestion_pipeline(
        instance_type: str, ecs_cluster: str, network_config: dict, **context
    ):
        return EcsRunTaskOperator(
            task_id="ingestion_pipeline",
            aws_conn_id="aws_ecs",
            cluster=ecs_cluster,
            task_definition=f"jacobs_webscrape_task_",
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "jacobs_container",
                        "environment": [
                            {
                                "name": "DAG_RUN_TS",
                                "value": "{{ ts }}",
                            },
                            {
                                "name": "DAG_RUN_DATE",
                                "value": "{{ ds }}",
                            },
                            {
                                "name": "RUN_TYPE",
                                "value": instance_type,
                            },
                            {
                                "name": "S3_BUCKET",
                                "value": f"jacobsbucket97-{instance_type}",
                            },
                            {
                                "name": "RDS_SCHEMA",
                                "value": "nba_source",
                            },
                        ],
                    }
                ]
            },
            network_configuration=network_config,
            awslogs_group="jacobs_ecs_logs_airflow",
            awslogs_stream_prefix="ecs/jacobs_container_airflow",
            do_xcom_push=True,
        )

    def dbt_pipeline(
        instance_type: str,
        ecs_cluster: str,
        network_config: dict,
        dbt_config: dict,
        **context,
    ):
        return EcsRunTaskOperator(
            task_id="dbt_pipeline",
            aws_conn_id="aws_ecs",
            cluster=ecs_cluster,
            task_definition="jacobs_dbt_task",
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "jacobs_container_dbt",
                        "environment": [
                            {
                                "name": "DAG_RUN_TS",
                                "value": "{{ ts }}",
                            },
                            {
                                "name": "DAG_RUN_DATE",
                                "value": " {{ ds }}",
                            },
                            {
                                "name": "RUN_TYPE",
                                "value": instance_type,
                            },
                            {
                                "name": "DBT_DBNAME",
                                "value": dbt_config["DBT_DBNAME"],
                            },
                            {
                                "name": "DBT_HOST",
                                "value": dbt_config["DBT_HOST"],
                            },
                            {
                                "name": "DBT_USER",
                                "value": dbt_config["DBT_USER"],
                            },
                            {
                                "name": "DBT_PASS",
                                "value": dbt_config["DBT_PASS"],
                            },
                            {
                                "name": "DBT_SCHEMA",
                                "value": dbt_config["DBT_SCHEMA"],
                            },
                            {
                                "name": "DBT_PRAC_KEY",
                                "value": dbt_config["DBT_PRAC_KEY"],
                            },
                        ],
                    }
                ]
            },
            network_configuration=network_config,
            awslogs_group="jacobs_ecs_logs_dbt",
            awslogs_stream_prefix="ecs/jacobs_container_dbt",
            do_xcom_push=True,
        )

    def ml_pipeline(
        instance_type: str, ecs_cluster: str, network_config: dict, **context
    ):
        return EcsRunTaskOperator(
            task_id="ml_pipeline",
            aws_conn_id="aws_ecs",
            cluster=ecs_cluster,
            task_definition="jacobs_ml_task",
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": "jacobs_container_ml",
                        "environment": [
                            {
                                "name": "DAG_RUN_TS",
                                "value": "{{ ts }}",
                            },
                            {
                                "name": "DAG_RUN_DATE",
                                "value": " {{ ds }}",
                            },
                            {
                                "name": "RUN_TYPE",
                                "value": instance_type,
                            },
                            {
                                "name": "RDS_SCHEMA",
                                "value": "ml_models",
                            },
                        ],
                    }
                ]
            },
            network_configuration=network_config,
            awslogs_group="jacobs_ecs_logs_ml",
            awslogs_stream_prefix="ecs/jacobs_container_ml",
            do_xcom_push=True,
        )

    (
        ingestion_pipeline(
            instance_type=instance_type_env,
            ecs_cluster=ecs_cluster_env,
            network_config=network_config_env,
        )
        >> dbt_pipeline(
            instance_type=instance_type_env,
            ecs_cluster=ecs_cluster_env,
            network_config=network_config_env,
            dbt_config=dbt_config_env,
        )
        >> ml_pipeline(
            instance_type=instance_type_env,
            ecs_cluster=ecs_cluster_env,
            network_config=network_config_env,
        )
    )


dag = pipeline()
