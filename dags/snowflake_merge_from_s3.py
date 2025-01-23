from datetime import datetime

from airflow.decorators import dag, task


from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval
from include.snowflake_utils import (
    get_snowflake_conn,
    merge_from_s3_to_snowflake,
)


@dag(
    "snowflake_merge_test",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["snowflake"],
)
def snowflake_merge_test_pipeline():
    @task()
    def merge_task(
        **context: dict,
    ):
        conn = get_snowflake_conn("snowflake_conn")

        print("starting dag")
        merge_from_s3_to_snowflake(
            connection=conn,
            stage="NBA_ELT_STAGE_PROD",
            schema="source",
            table="test_merge_function",
            s3_prefix="snowflake_table_loading/month=01/",
            file_format="test_schema.parquet_format_tf",
            primary_keys=["id"],
            order_by_fields=["created_at"],
            target_table_timestamp_col="created_at",
        )

    merge_task()


dag = snowflake_merge_test_pipeline()
