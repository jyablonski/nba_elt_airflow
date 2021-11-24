""" Example Airflow DAG with custom plugin"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from utils import my_function, get_ssm_parameter

my_function()

DBT_PROFILE_DIR = '~/.dbt/'
DBT_PROJECT_DIR = '~/airflow/dags/dbt/'

with DAG(
    "jacobs_test_dag",
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

    python_dummy_task = PythonOperator(
      task_id="python_dummy_task",
      python_callable=my_function
    )

    send_email_notification = EmailOperator(
      task_id="send_email_notification",
      to="jyablonski9@gmail.com",
      subject="Test Dag run",
      html_content="<h3>Process Completed</h3>"
    ) 

    dummy_task >> [python_dummy_task, dbt_deps] >> send_email_notification
