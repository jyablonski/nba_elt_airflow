from datetime import datetime
import os

from airflow.decorators import dag, task
from airflow.hooks.subprocess import SubprocessHook
import boto3

from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval


@dag(
    "bash_test",
    # schedule_interval="0 0 12 1 4/6 ? *",
    schedule_interval=get_schedule_interval("0 12 1 5,9 *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["example"],
)
def bash_test_pipeline():
    @task()
    def test_task(
        **context: dict,
    ):
        print("hi")

    @task()
    def bash_task():
        sts = boto3.client("sts")
        response = sts.assume_role(
            RoleArn="arn:aws:iam::717791819289:role/jacobs-airflow-bash-testing-role2",
            RoleSessionName="jacob-airflow-bash-session",
        )

        os.environ["AWS_ACCESS_KEY_ID"] = response["Credentials"]["AccessKeyId"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = response["Credentials"]["SecretAccessKey"]
        os.environ["AWS_SESSION_TOKEN"] = response["Credentials"]["SessionToken"]
        os.environ["AIRFLOW_ENV_VAR"] = "hijacobfrombashtask"
        os.environ["RDSPORT"] = "3306"
        os.environ["COMNAME"] = "jyablonski Productions"
        os.environ["DBNAME"] = "jyablonski_production"
        os.environ["EP"] = "rds.jyablonski.dev"
        os.environ["MASTERUSER"] = "myuser"
        os.environ["MYPASS"] = "mypass"

        script_path = os.path.join(
            os.environ["AIRFLOW_HOME"], "include/scripts/test.sh"
        )
        subprocess_hook = SubprocessHook()
        subprocess_hook.run_command(["bash", script_path])

    test_task() >> bash_task()


dag = bash_test_pipeline()
