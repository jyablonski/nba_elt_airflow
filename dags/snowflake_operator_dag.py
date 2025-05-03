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
    params={
        "start_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            title="Backfill Start Date",
            description="Please select an optional date",
            nullable=True,
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            title="Backfill End Date",
            description="Please select an optional date",
            nullable=True,
        ),
    },
    render_template_as_native_obj=True,
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
        # TODO: figure out how to manage the year / month / day partitioning
        s3_prefix="boxscores/validated/year=2025/month=01",
        file_format="production.test_schema.parquet_format_tf",
        truncate_table=False,
        ingestion_start_date="{{ dag_run.conf.get('start_date', data_interval_end) }}",
        ingestion_end_date="{{ dag_run.conf.get('end_date', data_interval_end) }}",
    )

    @task()
    def test_task(**context):
        print("hello world")

    load_table >> test_task()


pipeline()
