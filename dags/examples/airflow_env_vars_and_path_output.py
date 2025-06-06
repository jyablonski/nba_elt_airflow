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
    # schedule="*/2 * * * *",
    start_date=datetime(2023, 8, 13, 10, 15, 0),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["example", "log_group"],
)
def airflow_env_vars_pipeline():
    @task()
    def practice_task(**context):
        print("::group::PYTHONPATH")
        print(f"{sys.path}")
        print("::endgroup::")

        # current working directory is at:
        # `/usr/local/airflow`
        # pass in template searchpath absolute path like so:
        # template_searchpath="/usr/local/airflow/dags/sql",
        print("::group::DIRECTORY")
        print(f"Current working directory: {os.getcwd()}")
        print(f"DAG file path: {__file__}")
        print("DAG folder contents:")
        for root, dirs, files in os.walk("/usr/local/airflow/dags"):
            for f in files:
                print(os.path.join(root, f))
        print("::endgroup::")

        print("::group::ENVIRONMENT VARIABLES")
        for key, value in os.environ.items():
            print(f"{key}={value}")

        print("::endgroup::")

        print("::group::CONTEXT")
        for key, value in context.items():
            print(f"{key}={value}")

        print("::endgroup::")

        run_date = datetime.strftime(context["logical_date"], "%Y-%m-%d")

        print(f"Finishing Run for {run_date}")

    practice_task()


airflow_env_vars_pipeline()
