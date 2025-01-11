from datetime import datetime

from airflow.decorators import dag, task

from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval, read_dag_docs


@dag(
    "pagerduty_v2_example",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    doc_md=read_dag_docs("pagerduty_v2_example"),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["example"],
)
def pagerduty_test():
    @task()
    def test_task(
        **context: dict,
    ):
        print("hi")

        raise ValueError("Test Failure")

    test_task()


dag = pagerduty_test()
