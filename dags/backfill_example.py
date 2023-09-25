from datetime import datetime, timedelta
import time
import pkg_resources

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
        # print(f"timestamp is {timestamp}")
        installed_packages = pkg_resources.working_set
        installed_packages_list = sorted(
            ["%s==%s" % (i.key, i.version) for i in installed_packages]
        )

        for i in installed_packages_list:
            print(i)

        return {"hello": "world", "timestamp": timestamp}

    @task()
    def print_test_task_results(the_data: dict):
        print(f"crap jacob")
        print(the_data)

        pass

    print_test_task_results(test_task())


dag = backfill_example()
