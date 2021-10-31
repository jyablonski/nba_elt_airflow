""" Example Airflow DAG with custom plugin"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator

with DAG(
    "custom_operator_dag",
    schedule_interval='0 11 * * *',
    start_date=datetime(2021, 10, 20),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False
) as dag:

    dummy_task = DummyOperator(task_id="dummy_task")

    send_email_notification = EmailOperator(
    task_id="send_email_notification",
    to="jyablonski9@gmail.com",
    subject="Airflow NBA ELT Pipeline DAG Run",
    html_content="<h3>Process Completed</h3>"
    ) 

    dummy_task >> send_email_notification
