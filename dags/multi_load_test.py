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
    "retries": 1,
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
    def generate_run_config(
        **context: dict,
    ) -> None:
        if context["params"]["run_type"] == "Full Backfill":
            tables = ["table1", "table2", "table3"]
            run_type = "backfill"
        else:
            tables = ["table1"]
            run_type = "incremental"

        if context["data_interval_end"].hour == 12:
            s3_prefix = "morning"
        elif context["data_interval_end"].hour == 16:
            s3_prefix = "afternoon"
        elif context["data_interval_end"].hour == 20:
            s3_prefix = "evening"
        else:
            s3_prefix = "manual"

        # Tables determines which tables to run for
        # S3 Prefix is used to store the files in the appropriate place in the S3 Bucket
        # Run Type determines whether it's a full backfill or an incremental load,
        #    which we would filter the tables down for using timestamp filters
        return {
            "run_type": run_type,
            "s3_prefix": s3_prefix,
            "tables": tables,
        }

    @task(retries=0, retry_delay=timedelta(minutes=4))
    def test_task_followup(
        **context: dict,
    ) -> None:
        print(context)
        run_config = context["ti"].xcom_pull(task_ids="generate_run_config")
        print(run_config)
        return None

    @task(retries=2)
    def test_task_final(
        **context: dict,
    ) -> None:
        run_config = context["ti"].xcom_pull(task_ids="generate_run_config")
        print(run_config)
        return None

    generate_run_config() >> test_task_followup() >> test_task_final()
    # test_task_final(test_task_followup(generate_run_config()))


dag = multi_load_test_pipeline()
