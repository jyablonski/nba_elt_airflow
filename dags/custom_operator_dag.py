""" Example Airflow DAG with custom plugin"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email import EmailOperator

DBT_PROFILE_DIR = '~/.dbt/'
DBT_PROJECT_DIR = '~/airflow/dags/dbt/'

with DAG(
    "custom_operator_dag",
    schedule_interval='0 11 * * *',
    start_date=datetime(2021, 10, 20),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False
) as dag:

    dummy_task = DummyOperator(task_id="dummy_task")

    dbt_deps = BashOperator(
      task_id="dbt_deps",
      bash_command=f"dbt deps --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dummy_task >> dbt_deps
