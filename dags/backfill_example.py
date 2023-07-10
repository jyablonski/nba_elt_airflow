from datetime import datetime, timedelta
import time

from airflow.decorators import dag, task

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
    "backfill_example",
    schedule_interval=get_schedule_interval("*/2 * * * *"),
    start_date=datetime(2023, 4, 1),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
)
def backfill_example():
    @task()
    def test_task(**kwargs):

        timestamp = kwargs["data_interval_end"].strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"timestamp is {timestamp}")

        print(f"Sleeping for 30 seconds")
        time.sleep(30)

    test_task()


dag = backfill_example()
