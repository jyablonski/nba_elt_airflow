from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook

from include.utils import jacobs_slack_alert, get_schedule_interval


default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": "jyablonski9@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": jacobs_slack_alert,
}


@dag(
    "http_test",
    # schedule_interval="0 0 12 1 4/6 ? *",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
)
def test_pipeline():
    @task()
    def test_task(
        **context: dict,
    ):
        print(f"hi")
        hook = HttpHook(http_conn_id="api", method="GET")

        tester = hook.run("game_types")
        print(tester)
        print(tester.json())
        print(tester.status_code)

    test_task()


dag = test_pipeline()
