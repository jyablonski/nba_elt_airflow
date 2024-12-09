from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.pagerduty.notifications.pagerduty import (
    send_pagerduty_notification,
)

from include.utils import (
    get_schedule_interval,
    jacobs_pagerduty_notification,
    jacobs_slack_alert,
    jacobs_discord_alert,
    multi_failure_alert,
    read_dag_docs
)

default_args = {
    "owner": "jacob",
    "depends_on_past": True,
    "email": "jyablonski9@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # this is the pure provider package version of doing it
    # "on_failure_callback": send_pagerduty_notification(
    #     pagerduty_events_conn_id="pagerduty",
    #     summary="DAG {{ dag.dag_id }} Failure",
    #     severity="critical",
    #     source="airflow dag_id: {{dag.dag_id}}",
    #     dedup_key="{{dag.dag_id}}-{{ti.task_id}}",
    #     group="{{dag.dag_id}}",
    #     component="airflow",
    #     class_type="Data Pipeline",
    # ),
    # this is the custom version of doing it
    "on_failure_callback": jacobs_pagerduty_notification(severity="info"),
}


@dag(
    "pagerduty_v2_example",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    doc_md=read_dag_docs("pagerduty_v2_example"),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
)
def pagerduty_test():
    @task()
    def test_task(
        **context: dict,
    ):
        print("hi")

        raise ValueError("Test Failure")

    test_task()


dag = pagerduty_test()
