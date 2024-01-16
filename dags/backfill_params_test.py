from datetime import datetime, timedelta
import pkg_resources

from airflow import AirflowException
from airflow.decorators import dag, task

from include.utils import get_schedule_interval, jacobs_slack_alert, loop_through_days

default_args = {
    "owner": "jacob",
    "depends_on_past": True,
    "email": ["jyablonski9@gmail.com", "jyablonski.aws2@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": jacobs_slack_alert,
}


@dag(
    "backfill_params_test_v2",
    schedule_interval=get_schedule_interval("*/2 * * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
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
