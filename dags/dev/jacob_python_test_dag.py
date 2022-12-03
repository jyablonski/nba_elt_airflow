""" Python Test DAG"""
from datetime import datetime, timedelta
import os
import time

from airflow import DAG
from airflow.settings import Session
from airflow.models.connection import Connection
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from utils import (
    jacobs_slack_alert,
    check_connections
)

# send both an email alert + a slack alert to specified channel on any task failure
JACOBS_DEFAULT_ARGS = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "on_failure_callback": jacobs_slack_alert
}

@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=JACOBS_DEFAULT_ARGS,
    tags=["example"],
)
def my_practice_dag():
    @task
    def practice():
        print(f"Hello world at {datetime.now()}")
        # time.sleep(10)
        return 1

    @task
    def check_for_connection():
        check_connections("warehaus")
        return 1

    practice()
    check_for_connection()

my_practice_dag()