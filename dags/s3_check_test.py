from datetime import datetime, timedelta
import sys

from airflow.decorators import dag, task
import boto3

from include.aws_utils import check_s3_file_exists
from include.exceptions import S3PrefixCheckFail
from include.utils import get_schedule_interval, jacobs_slack_alert, loop_through_days

default_args = {
    "owner": "jacob",
    "depends_on_past": True,
    "email": "jyablonski9@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": jacobs_slack_alert,
}


@dag(
    "s3_check_test",
    schedule_interval=get_schedule_interval("0 12/4 * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
)
def s3_check_test():
    @task()
    def test_task(
        **context: dict,
    ):
        file_name = "manifest1.json"

        try:
            client = boto3.client(f"s3")
            check_s3_file_exists(
                client=client,
                bucket="nba-elt-dbt-ci",
                file_prefix=file_name,
            )

        except S3PrefixCheckFail:
            print(
                f"File not found, assuming there was no data for today.  exiting out ..."
            )
            return

        print(f"Found file {file_name} !")
        return {"hello": "world"}

    test_task()


dag = s3_check_test()
