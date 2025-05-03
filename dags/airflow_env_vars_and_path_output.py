"""Python Test DAG"""

from datetime import datetime
import os
import sys

from airflow.decorators import dag, task

from include.common import DEFAULT_ARGS
from include.utils import (
    get_schedule_interval,
)


@dag(
    "airflow_env_vars_and_path_output",
    schedule=get_schedule_interval(None),
    start_date=datetime(2023, 8, 13, 10, 15, 0),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["example"],
)
def airflow_env_vars_pipeline():
    @task()
    def practice_task(**context):
        print(f"PYTHONPATH: {sys.path}")
        print(f"Current working directory: {os.getcwd()}")
        print(f"DAG file path: {__file__}")
        print("DAG folder contents:")
        for root, dirs, files in os.walk("/usr/local/airflow/dags"):
            for f in files:
                print(os.path.join(root, f))

    practice_task()


airflow_env_vars_pipeline()
