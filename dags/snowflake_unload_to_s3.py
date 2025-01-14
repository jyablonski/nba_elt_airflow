from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param


from include.common import DEFAULT_ARGS
from include.snowflake_utils import get_snowflake_conn, unload_to_s3
from include.utils import get_schedule_interval


@dag(
    "snowflake_unload_to_s3",
    schedule=get_schedule_interval(None),
    start_date=datetime(2023, 7, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "snowflake_database": Param(
            default="production",
            type="string",
            title="Database Name",
            description="Enter a Database",
        ),
        "snowflake_schema": Param(
            default="test_schema",
            type="string",
            title="Schema Name",
            description="Enter a Schema",
        ),
        "snowflake_table": Param(
            default="test_table",
            type="string",
            title="Table Name",
            description="Enter a Table",
        ),
        "s3_stage": Param(
            default="NBA_ELT_STAGE_PROD",
            type="string",
            title="S3 Stage",
            description="S3 Stage to Load Data to",
            enum=["NBA_ELT_STAGE_PROD"],
        ),
        "s3_folder_prefix": Param(
            type="string",
            title="S3 Folder Prefix",
            description="""S3 Folder Prefix in the Bucket to Load Data to.
            Example: `test-table-copy`""",
        ),
        "file_format": Param(
            default="test_schema.parquet_format_tf",
            type="string",
            title="File Format",
            description="File Format to use",
            enum=["test_schema.parquet_format_tf"],
        ),
        "unload_limit": Param(
            default=0,
            type="integer",
            title="Unload Limit",
            description="""Optional Param to set a limit parameter on the query.
            0 defaults to no limit being applied.""",
        ),
    },
    tags=["snowflake", "manual"],
)
def pipeline():

    @task()
    def unload_task(**context):
        conn = get_snowflake_conn(conn_id="snowflake_conn")

        unload_limit = context["params"]["unload_limit"]

        if unload_limit == 0:
            unload_limit = False

        unload_to_s3(
            connection=conn,
            s3_stage=context["params"]["s3_stage"],
            s3_prefix=context["params"]["s3_folder_prefix"],
            database_name=context["params"]["snowflake_database"],
            schema_name=context["params"]["snowflake_schema"],
            table_name=context["params"]["snowflake_table"],
            file_format=context["params"]["file_format"],
            limit=unload_limit,
        )

        return True

    unload_task()


pipeline()
