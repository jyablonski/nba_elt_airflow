from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param


from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval
from include.snowflake_utils import (
    get_snowflake_conn,
    merge_snowflake_source_into_target,
)


@dag(
    "snowflake_load_test",
    # schedule_interval="0 0 12 1 4/6 ? *",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["snowflake"],
)
def snowflake_test_pipeline():
    @task()
    def build_table_task(
        **context: dict,
    ):
        conn = get_snowflake_conn("snowflake_conn")


        # TODO: Finish Merge later
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
