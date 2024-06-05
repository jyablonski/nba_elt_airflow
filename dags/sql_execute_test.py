from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from include.utils import get_schedule_interval, jacobs_slack_alert

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

ID_DEFAULT = "10, 11, 12"


@dag(
    "sql_execute_test",
    # schedule_interval="0 12/4 * * *",
    schedule_interval=get_schedule_interval("*/1 * * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
    template_searchpath="/usr/local/airflow/dags/sql",
    params={
        "start_date": Param(
            default=f"{datetime.today().date() - timedelta(days=1)}",
            type="string",
            format="date",
            title="Start Date Picker",
            description="Please select a Start Date, use the button on the left for a pup-up calendar. ",
        ),
        "end_date": Param(
            default=f"{datetime.today().date()}",
            type="string",
            format="date",
            title="End Date Picker",
            description="Please select a date, use the button on the left for a pup-up calendar. "
            "This date is *not* inclusive",
        ),
        "ids": Param(
            default=ID_DEFAULT,
            type="string",
            title="IDs Picker",
            description="Please enter a comma separated list of IDs to filter on."
            "Example: 1,2,3,4,5,6,7,8,9,10",
        ),
    },
)
def sql_test_pipeline():
    execute_query = SQLExecuteQueryOperator(
        task_id="execute_query",
        conn_id="nba_database",
        sql="test.sql",
        return_last=False,
        show_return_value_in_logs=True,
    )

    execute_query


dag = sql_test_pipeline()
