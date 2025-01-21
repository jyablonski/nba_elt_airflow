from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval
from include.snowflake_params import SNOWFLAKE_PARAMS
from include.snowflake_utils import (
    build_snowflake_table_from_s3,
    load_snowflake_table_from_s3,
)

build_params = {
    "s3_file_prefix": Param(
        type="string",
        title="S3 File Prefix",
        description="S3 File Prefix to Build Table from",
    ),
    "load_table_afterwards": Param(
        default=True,
        type="boolean",
        title="Load Table Afterwards",
        description="Optional parameter to load the table after it's created",
    ),
}

dag_params = {**SNOWFLAKE_PARAMS, **build_params}


@dag(
    "snowflake_build_table",
    # schedule_interval="0 0 12 1 4/6 ? *",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["snowflake"],
    params=dag_params,
    render_template_as_native_obj=True,
)
def snowflake_build_table_pipeline():
    @task()
    def build_table_task(
        **context: dict,
    ):
        conn = SnowflakeHook(snowflake_conn_id="snowflake_conn", autocommit=True)
        engine = conn.get_sqlalchemy_engine()
        connection = engine.connect()

        build_snowflake_table_from_s3(
            connection=connection,
            schema=context["params"]["schema_name"],
            table=context["params"]["table_name"],
            stage=context["params"]["s3_stage"],
            s3_prefix=context["params"]["s3_file_prefix"],
            file_format=context["params"]["file_format"],
        )

        if context["params"]["load_table_afterwards"]:
            s3_prefix = context["params"]["s3_file_prefix"]

            load_snowflake_table_from_s3(
                connection=connection,
                schema=context["params"]["schema_name"],
                table=context["params"]["table_name"],
                stage=context["params"]["s3_stage"],
                s3_prefix=s3_prefix,
                file_format=context["params"]["file_format"],
            )

    build_table_task()


dag = snowflake_build_table_pipeline()
