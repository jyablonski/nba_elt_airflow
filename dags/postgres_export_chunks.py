from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
import pandas as pd

from include.aws_utils import write_to_s3
from include.common import DEFAULT_ARGS
from include.postgres_utils import create_pg_sqlalchemy_conn
from include.utils import get_schedule_interval

S3_BUCKET = "jyablonski-test-bucket123"


@dag(
    "postgres_export_chunks",
    # schedule_interval="0 0 12 1 4/6 ? *",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["example"],
    params={
        "run_type": Param(
            default="Incremental",
            type="string",
            title="Backfill Type",
            description="Select a Backfill Type",
            enum=["Full Backfill", "Incremental"],
        ),
    },
    render_template_as_native_obj=True,
)
def postgres_chunk_pipeline():
    @task()
    def write_data_to_s3(
        **context: dict,
    ):
        run_config = context["ti"].xcom_pull(task_ids="generate_run_config")
        date = context["data_interval_end"].date()
        table_name = "aws_boxscores_source"
        conn = create_pg_sqlalchemy_conn(postgres_conn="nba_database")
        chunk_int = 1

        query = f"select * from nba_source.{table_name}"
        if context["params"]["run_type"] == "Incremental":
            query += f" where date(date) >= '{date - timedelta(days=1)}'"

        for chunk in pd.read_sql_query(sql=query, con=conn, chunksize=10000):
            write_to_s3(
                dataframe=chunk,
                s3_bucket=S3_BUCKET,
                s3_path=f"airflow-testing/{table_name}-{date}-chunk-{chunk_int}",
            )
            chunk_int += 1

    write_data_to_s3()


dag = postgres_chunk_pipeline()
