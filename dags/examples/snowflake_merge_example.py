from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup


from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval
from include.snowflake_utils import (
    get_snowflake_conn,
    merge_from_s3_to_snowflake,
)


@dag(
    "snowflake_merge_example",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["snowflake", "example"],
)
def snowflake_merge_test_pipeline():
    # Configuration for each table
    table_configs = [
        {
            "table": "orders_load_test",
            "s3_prefix": "snowflake_load_testing_v2/",
            "primary_keys": ["o_orderkey"],
            "order_by_fields": ["o_orderdate"],
            "target_table_timestamp_col": "metadata_ingest_time",
        },
        {
            "table": "customers_load_test",
            "s3_prefix": "customer_load_data_v1/",
            "primary_keys": ["c_customerkey"],
            "order_by_fields": ["c_creation_date"],
            "target_table_timestamp_col": "metadata_ingest_time",
        },
        {
            "table": "products_load_test",
            "s3_prefix": "product_data_v1/",
            "primary_keys": ["p_productkey"],
            "order_by_fields": ["p_launch_date"],
            "target_table_timestamp_col": "metadata_ingest_time",
        },
    ]

    @task()
    def merge_task(table_config: dict[str, str], **context: dict):
        conn = get_snowflake_conn("snowflake_conn")

        print(f"Starting merge for table {table_config['table']}")
        merge_from_s3_to_snowflake(
            connection=conn,
            stage="NBA_ELT_STAGE_PROD",
            schema="source",
            table=table_config["table"],
            s3_prefix=table_config["s3_prefix"],
            file_format="test_schema.parquet_format_tf",
            primary_keys=table_config["primary_keys"],
            order_by_fields=table_config["order_by_fields"],
            target_table_timestamp_col=table_config["target_table_timestamp_col"],
        )

    with TaskGroup("merge_tasks") as merge_tasks:
        for table_config in table_configs:
            merge_task.override(task_id=f"merge_{table_config['table']}")(table_config)

    merge_tasks


dag = snowflake_merge_test_pipeline()
