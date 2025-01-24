from datetime import datetime

from airflow.decorators import dag, task
import boto3

from include.aws_utils import check_s3_file_exists
from include.common import DEFAULT_ARGS
from include.exceptions import S3PrefixCheckFail
from include.utils import get_schedule_interval


@dag(
    "s3_check_test",
    schedule_interval=get_schedule_interval("0 12/4 * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["example"],
)
def s3_check_test():
    @task()
    def test_task(
        **context: dict,
    ):
        file_name = "manifest1.json"

        try:
            client = boto3.client("s3")
            check_s3_file_exists(
                client=client,
                bucket="nba-elt-dbt-ci",
                file_prefix=file_name,
            )

        except S3PrefixCheckFail:
            print(
                "File not found, assuming there was no data for today.  exiting out ..."
            )
            return

        print(f"Found file {file_name} !")
        return {"hello": "world"}

    test_task()


dag = s3_check_test()
