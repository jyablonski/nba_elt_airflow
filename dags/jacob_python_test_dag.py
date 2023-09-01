""" Python Test DAG"""
from datetime import datetime

from airflow.decorators import dag, task
import boto3

from include.utils import get_schedule_interval, jacobs_slack_alert, check_connections, write_to_slack

# send both an email alert + a slack alert to specified channel on any task failure
JACOBS_DEFAULT_ARGS = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": jacobs_slack_alert,
}


@dag(
    schedule=get_schedule_interval("15 3-10 * * *"),
    start_date=datetime(2023, 8, 13, 10, 15, 0),
    catchup=False,
    default_args=JACOBS_DEFAULT_ARGS,
    tags=["example"],
)
def my_practice_dag():
    @task
    def practice(**context):
        print(f"context is {context}")
        print(f"yo {context['ts']}")
        print(f"Hello world at {datetime.now()}")
        write_to_slack(slack_conn_id="slack_ui", context=context, message="hi")
        # time.sleep(10)
        return 1

    @task
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
