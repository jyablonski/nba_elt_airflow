from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param


from include.common import DEFAULT_ARGS
from include.snowflake_params import COMMON_SNOWFLAKE_PARAMS
from include.snowflake_utils import get_snowflake_conn, load_snowflake_table_from_s3
from include.utils import get_schedule_interval

load_params = {
    "s3_file": Param(
        type="string",
        title="S3 File",
        description="""S3 File to load into Snowflake .
            Example: `snowflake_table_loading/test_file.parquet`""",
    ),
    "truncate_table_bool": Param(
        default=False,
        type="boolean",
        title="Truncate Table Option",
        description="Optional Parameter to truncate the table first",
    ),
}

dag_params = {**COMMON_SNOWFLAKE_PARAMS, **load_params}


@dag(
    "snowflake_load_from_s3",
    schedule=get_schedule_interval(None),
    start_date=datetime(2023, 7, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params=dag_params,
    tags=["snowflake", "manual"],
)
def pipeline():

    @task()
    def load_task(**context):
        conn = get_snowflake_conn(conn_id="snowflake_conn")

        load_snowflake_table_from_s3(
            connection=conn,
            stage=context["params"]["s3_stage"],
            schema=context["params"]["snowflake_schema"],
            table=context["params"]["snowflake_table"],
            s3_prefix=context["params"]["s3_file"],
            file_format=context["params"]["file_format"],
            truncate_table=context["params"]["truncate_table_bool"],
        )

        return True

    load_task()


pipeline()
