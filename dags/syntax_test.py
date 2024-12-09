from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.utils import get_schedule_interval, jacobs_slack_alert

# from airflow.providers.common.sql.hooks.sql import DbApiHook
#
# https://github.com/apache/airflow/blob/main/airflow/providers/postgres/hooks/postgres.py
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
    "syntax_test",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 10, 20),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
)
def syntax_test_pipeline():
    datasets = ["table1", "table2", "table3"]

    with TaskGroup(group_id="table_load") as table_load:
        for table in datasets:
            chain(
                *[
                    BashOperator(
                        task_id=f"load_{table}_{day}",
                        bash_command=f"echo {table}_{day} $START_DATE $END_DATE $DATASET",
                        env={
                            "START_DATE": f"{{{{ (data_interval_start - macros.timedelta(days={day + 1})).strftime('%Y%m%d') }}}}",
                            "END_DATE": f"{{{{ (data_interval_start - macros.timedelta(days={day + 1})).strftime('%Y%m%d') }}}}",
                            "DATASET": f"{{{{ dag_run.conf.get('DATASET', '{table}') }}}}",
                        },
                    )
                    for day in reversed(range(2))
                ]
            )

    table_load


dag = syntax_test_pipeline()
