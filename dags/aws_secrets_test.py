from datetime import datetime, timedelta
import sys

from airflow.decorators import dag, task
from airflow.models import Variable

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
    "aws_secrets_test",
    schedule_interval=get_schedule_interval("0 12/4 * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
)
def aws_secrets_test():
    @task()
    def test_task(
        **context: dict,
    ):
        # it pulls this from systems parameter store key named `/airflow/variables/airflow_test_key`
        # because the `"variables_prefix": "/airflow/variables"` option is set in the env vars
        d = Variable.get("airflow_test_key")
        print(f"Hello world at {datetime.now()}, d is {d}")

    test_task()


dag = aws_secrets_test()
