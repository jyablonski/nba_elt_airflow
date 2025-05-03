from datetime import datetime

from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval


@dag(
    "snowflake_clone_dev_db",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["snowflake"],
    # this is needed when using the SQLExecuteQueryOperator
    # so it can find the script
    template_searchpath="/usr/local/airflow/dags/sql",
)
def clone_db_pipeline():
    clone_db_task = SQLExecuteQueryOperator(
        task_id="clone_db_task",
        conn_id="snowflake_conn",
        sql="create_database_clone.sql",
        return_last=False,
        show_return_value_in_logs=True,
    )

    clone_db_task


dag = clone_db_pipeline()
