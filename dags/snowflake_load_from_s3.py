from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param


from include.common import DEFAULT_ARGS
from include.snowflake_utils import get_snowflake_conn, unload_to_s3
from include.utils import get_schedule_interval


@dag(
    "snowflake_load_from_s3",
    schedule=get_schedule_interval(None),
    start_date=datetime(2023, 7, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
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
