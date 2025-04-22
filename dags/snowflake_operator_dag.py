from datetime import datetime, timezone
import json

from airflow.decorators import dag, task
from airflow.models.param import Param
import pandas as pd


from include.aws_utils import write_to_s3
from include.common import DEFAULT_ARGS
from include.snowflake_utils import (
    get_snowflake_conn,
    load_snowflake_table_from_s3,
    build_snowflake_table_from_s3,
)
from include.utils import get_schedule_interval

from plugins.snowflake_load_operator import LoadSnowflakeFromS3Operator


@dag(
    "snowflake_operator_dag",
    schedule=get_schedule_interval(None),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["snowflake", "manual"],
)
def pipeline():
    load_table = LoadSnowflakeFromS3Operator(
        task_id="load_table_from_s3",
        snowflake_conn_id="snowflake_conn",
        stage="NBA_ELT_STAGE_PROD",
        schema="source",
        table="boxscores",
        s3_prefix="boxscores/validated/year=2025/month=01",
        file_format="production.test_schema.parquet_format_tf",
        truncate_table=False,
        # ingestion_start_date="2025-01-01",
        # ingestion_end_date="2025-01-07",
    )

    @task()
    def test_task(**context):
        print("hello world")

    load_table >> test_task()


pipeline()
