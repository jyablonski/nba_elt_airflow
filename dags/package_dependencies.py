from datetime import datetime, timedelta
import pkg_resources

from airflow.decorators import dag, task

# testing some astronomer airflow stuff
# from google.analytics import data_v1alpha
# from google.analytics.data_v1beta.types import (
#     DateRange,
#     Dimension,
#     Metric,
#     RunReportRequest,
#     RunReportResponse,
# )
# from google.oauth2 import service_account

from include.common import DEFAULT_ARGS
from include.utils import (
    get_schedule_interval,
    start_log_block,
    end_log_block,
)


@dag(
    "package_dependencies",
    schedule_interval=get_schedule_interval("*/2 * * * *"),
    start_date=datetime(2023, 4, 1),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["example", "template"],
)
def package_dependencies():
    @task()
    def test_task(**kwargs):
        start_log_block(group="Package Version Logs")
        installed_packages = pkg_resources.working_set
        installed_packages_list = sorted(
            ["%s==%s" % (i.key, i.version) for i in installed_packages]
        )

        for i in installed_packages_list:
            print(i)

        end_log_block()

        return {"hello": "world"}

    test_task()


dag = package_dependencies()
