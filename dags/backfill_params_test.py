from datetime import datetime, timedelta

from airflow import AirflowException
from airflow.decorators import dag, task

from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval, loop_through_days


@dag(
    "backfill_params_test_v2",
    schedule_interval=get_schedule_interval("*/2 * * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"start_date": (datetime.now().date() - timedelta(days=1))},
    render_template_as_native_obj=True,
    tags=["example"],
)
def package_dependencies():
    @task()
    def test_task(
        run_date: datetime.date,
        **context: dict,
    ):
        start_date = run_date
        end_date = (context["data_interval_end"] - timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )

        print(f"start date is {run_date}")
        print(f"original data interval end is {context['data_interval_end']}")
        print(f"end date is {end_date}")
        # print(f"end date is {kwargs['ds']}")

        loop_through_days(start_date=start_date, end_date=end_date)

        raise AirflowException("hello world")

        # return {"hello": "world"}

    test_task(run_date="{{ params['start_date'] }}")


dag = package_dependencies()
