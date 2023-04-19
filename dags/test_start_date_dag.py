from datetime import datetime, timedelta
import time

from airflow.decorators import dag, task
import boto3

from include.utils import check_s3_file_exists

# can manually delete previous successful DAG runs w/ browse -> DAG Runs
# or can manually clear state previous successful DAG Runs

default_args = {
    "owner": "jacob",
    "depends_on_past": True,
    "email": "jyablonski9@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# returns a datetime object with today's date, with 00:00:00 H:M:S
today = datetime.combine(datetime.now().date().today(), datetime.min.time())

# even though the start date is today, if you enable the dag it wont run a scheduled run
# because it hasn't passed the first data interval end yet.
@dag(
    "test_start_date_dag",
    schedule_interval="30 5 * * *",
    start_date=today,
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["yoo"],
    params={"hello": "world"},
    render_template_as_native_obj=True,
)

def test_start_date_dag():
    @task()
    def test_task(**context):  # can call this **kwargs, but context makes more sense.

        print(context)
        print(context['params'])
        print(context['params']['hello'])
        timestamp = context["data_interval_end"].strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"timestampzzz is {timestamp}")

        client = boto3.client('s3')
        check_s3_file_exists(
            client,
            bucket="jacobsbucket97-dev",
            prefix="graphql/lambda_function.zip"
        )

        print(f"Sleeping for 30 seconds")
        time.sleep(30)

    test_task()


dag = test_start_date_dag()
# test_start_date_dag() # either of these work