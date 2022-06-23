from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

from utils import get_ssm_parameter, jacobs_discord_alert

JACOBS_DEFAULT_ARGS = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": jacobs_discord_alert
}

with DAG(dag_id='trigger_airbyte_job_example',
         default_args=JACOBS_DEFAULT_ARGS,
         schedule_interval='@daily',
         start_date=datetime(2022, 6, 6),
    ) as dag:

    money_to_json = AirbyteTriggerSyncOperator(
        task_id='airbyte_money_json_example',
        airbyte_conn_id='airbyte_conn_example',
        connection_id='1e3b5a72-7bfd-4808-a13c-204505490110',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )