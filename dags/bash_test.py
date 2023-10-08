from datetime import datetime, timedelta
import os

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import boto3

from include.aws_utils import check_s3_file_exists
from include.exceptions import S3PrefixCheckFail
from include.utils import get_schedule_interval, jacobs_slack_alert

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
    "bash_test",
    schedule_interval=get_schedule_interval("0 12/4 * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
)
def bash_test_pipeline():
    @task()
    def test_task(
        **context: dict,
    ):
        print(f"hi")

    def bash_task():
        sts = boto3.client("sts")
        response = sts.assume_role(
            RoleArn="arn:aws:iam::717791819289:role/jacobs-airflow-bash-testing-role",
            RoleSessionName="jacob-airflow-bash-session",
        )

        os.environ["AWS_ACCESS_KEY_ID"] = response["Credentials"]["AccessKeyId"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = response["Credentials"]["SecretAccessKey"]
        os.environ["AWS_SESSION_TOKEN"] = response["Credentials"]["SessionToken"]
        os.environ["AIRFLOW_ENV_VAR"] = "hijacobfrombashtask"

        return BashOperator(
            task_id="bash_task",
            # "scripts" folder is under "/usr/local/airflow/dags"
            bash_command="${AIRFLOW_HOME}/include/scripts/test.sh ",
        )

    test_task() >> bash_task()


dag = bash_test_pipeline()
