""" Python Test DAG"""

from datetime import datetime

from airflow.decorators import dag, task
import boto3

from include.common import DEFAULT_ARGS
from include.utils import (
    get_schedule_interval,
    check_connections,
    write_to_slack,
)


@dag(
    "jacob_python_test_dag",
    schedule=get_schedule_interval("15 3-10 * * *"),
    start_date=datetime(2023, 8, 13, 10, 15, 0),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["example"],
)
def my_practice_dag():
    @task()
    def practice(**context):
        print(f"context is {context}")
        print(f"yo {context['ts']}")
        print(f"Hello world at {datetime.now()}")
        write_to_slack(slack_conn_id="slack_ui", context=context, message="hi")
        # time.sleep(10)
        return 1

    @task()
    def check_for_connection():
        check_connections("warehaus")
        test1 = 1
        print(test1)
        test2 = 2
        print(test2)
        client = boto3.client("sts")

        current_user = client.get_caller_identity()
        print(f"current user is {current_user} mfer")
        return 1

    practice()
    check_for_connection()


my_practice_dag()
