from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval


@dag(
    "aws_secrets_test",
    schedule_interval=get_schedule_interval("0 0,6,12,18 * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["example"],
)
def aws_secrets_test():
    @task()
    def test_task(
        **context: dict,
    ):
        # it pulls this from systems parameter store key named `/airflow/variables/airflow_test_key`
        # because the `"variables_prefix": "/airflow/variables"` option is set in the env vars
        d = Variable.get("airflow_test_key")
        print(f"Hello world at {datetime.now()}, d is {d}")

    test_task()


dag = aws_secrets_test()
