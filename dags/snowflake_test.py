from datetime import datetime, timedelta
import time

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


from include.utils import jacobs_slack_alert, get_schedule_interval
from include.snowflake_utils import (
    build_snowflake_table_from_s3,
    load_snowflake_table_from_s3,
    get_file_format,
    get_snowflake_conn,
    merge_snowflake_source_into_target,
)


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
    "snowflake_test",
    # schedule_interval="0 0 12 1 4/6 ? *",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
)
def snowflake_test_pipeline():
    @task()
    def build_table_task(
        **context: dict,
    ):
        conn = get_snowflake_conn("snowflake_conn")

        merge_snowflake_source_into_target(
            connection=conn,
            source_schema="test_schema",
            source_table="merge_test",
            target_schema="test_schema",
            target_table="merge_target",
            primary_keys=["id"],
        )

    build_table_task()


dag = snowflake_test_pipeline()
