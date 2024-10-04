from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


from include.utils import jacobs_slack_alert, get_schedule_interval
from include.snowflake_utils import (
    build_snowflake_table_from_s3,
    load_snowflake_table_from_s3,
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
    "snowflake_build_table",
    # schedule_interval="0 0 12 1 4/6 ? *",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
    params={
        "schema_name": Param(
            default="test_schema",
            type="string",
            title="Schema Name",
            description="Enter a Schema",
        ),
        "table_name": Param(
            type="string",
            title="Table Name",
            description="Enter a Table",
        ),
        "s3_stage": Param(
            default="s3://jyablonski-test-bucket123",
            type="string",
            title="S3 Stage",
            description="S3 Stage to Load Data to",
            enum=["@NBA_ELT_STAGE_PROD"],
        ),
        "s3_file_prefix": Param(
            type="string",
            title="S3 File Prefix",
            description="S3 File Prefix to Load Data to",
        ),
        "load_table_afterwards": Param(
            default=True,
            type="boolean",
            title="Load Table Afterwards",
            description="Optional parameter to load the table after it's created",
        ),
    },
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
        )

        if context["params"]["load_table_afterwards"]:
            s3_prefix = context["params"]["s3_file_prefix"]

            load_snowflake_table_from_s3(
                connection=connection,
                schema=context["params"]["schema_name"],
                table=context["params"]["table_name"],
                stage=context["params"]["s3_stage"],
                s3_prefix=s3_prefix,
            )

    build_table_task()


dag = snowflake_build_table_pipeline()
