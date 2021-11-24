from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
# from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
# from airflow.utils.dates import days_ago

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
DBT_PROFILE_DIR = '~/.dbt/'
DBT_PROJECT_DIR = '~/airflow/dags/dbt/'

with DAG(
    "nba_elt_pipeline",
    schedule_interval='0 11 * * *',
    start_date=datetime(2021, 10, 16),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    tags=["nba_elt_pipeline"]
) as dag:

    dummy_task = DummyOperator(task_id="dummy_task")

    jacobs_airflow_ecs_task = ECSOperator(
      task_id="jacobs_airflow_ecs_task",
      aws_conn_id="aws_ecs",
      cluster="jacobs_fargate_cluster",
      task_definition="jacobs_task_airflow",
      launch_type="FARGATE",
      overrides={},
      network_configuration={
          "awsvpcConfiguration": {
              "securityGroups": ["sg-0e3e9289166404b84"],
              "subnets": ["subnet-0652b6b91d94ebcd0", "subnet-0047afa4a7e93ec89"],
              "assignPublicIp": "ENABLED"
          }
      },
      awslogs_group="aws_ecs_logs"
    ) 

    dbt_seed = BashOperator(
      task_id="dbt_seed",
      bash_command=f"dbt seed --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_run = BashOperator(
      task_id="dbt_run",
      bash_command=f"dbt run --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_test = BashOperator(
      task_id="dbt_test",
      bash_command=f"dbt test --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    # jacobs_ge_task = GreatExpectationsOperator(
    #     task_id='jacobs_ge_task',
    #     expectation_suite_name='my_suite',
    #     batch_kwargs={
    #         'table': 'my_table',
    #         'datasource': 'my_datasource'
    #     }
    # )

    send_email_notification = EmailOperator(
      task_id="send_email_notification",
      to="jyablonski9@gmail.com",
      subject="Airflow NBA ELT Pipeline DAG Run",
      html_content="<h3>Process Completed</h3>"
    ) 

    jacobs_airflow_ecs_task >> [dbt_seed, dummy_task] >> dbt_run >> dbt_test >> send_email_notification

    # dummy_task >> jacobs_airflow_ecs_task >> dbt_seed >> dbt_run >> dbt_test >> send_email_notification