from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from include.utils import get_schedule_interval, jacobs_slack_alert
from CustomTimetable import UnevenIntervalsTimetable

default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": "jyablonski9@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": jacobs_slack_alert,
}

ID_DEFAULT = "10, 11, 12"


@dag(
    "multi_load_test",
    schedule_interval="0 12,16,20 * * *",
    # schedule=UnevenIntervalsTimetable(),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "run_type": Param(
            default="Incremental",
            type="string",
            title="Backfill Type",
            description="Select a Backfill Type",
            enum=["Full Backfill", "Incremental"],
        ),
    },
    render_template_as_native_obj=True,
    tags=["example"],
)
def multi_load_test_pipeline():
    @task()
    def test_task(
        **context: dict,
    ):
        if context["params"]["run_type"] == "Full Backfill":
            print("hello world")
            tables = ["table1", "table2", "table3"]
        else:
            print("Incremental")
            tables = ["table1"]

        if context["data_interval_end"].hour == 12:
            s3_prefix = "morning"
        elif context["data_interval_end"].hour == 16:
            s3_prefix = "afternoon"
        elif context["data_interval_end"].hour == 20:
            s3_prefix = "night"
        else:
            s3_prefix = "manual"

        print(context)
        print(f"try 2 {context['ts']}")

        return {
            "data_interval_end": context["data_interval_end"],
            "tables": tables,
            "s3_prefix": s3_prefix,
        }

    @task()
    def test_task_followup(
        run_config: dict[str, str],
        **context: dict,
    ):
        print(run_config)
        print(context)
        print(f"try 2 {context['ts']}")

    test_task_followup(test_task())


dag = multi_load_test_pipeline()
