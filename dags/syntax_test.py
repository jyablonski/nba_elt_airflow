from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup

from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval


# from airflow.providers.common.sql.hooks.sql import DbApiHook
#
# https://github.com/apache/airflow/blob/main/airflow/providers/postgres/hooks/postgres.py


@dag(
    "syntax_test",
    schedule_interval=get_schedule_interval("0 * * * *"),
    start_date=datetime(2024, 10, 20),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
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
