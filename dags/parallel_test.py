from datetime import datetime
import time

from airflow.decorators import dag, task

# testing some astronomer airflow stuff
# from google.analytics import data_v1alpha`
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
)


@dag(
    "parallel_test",
    schedule=get_schedule_interval("*/2 * * * *"),
    start_date=datetime(2023, 4, 1),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["example", "template"],
)
def parallel_test():
    @task()
    def task_one(**kwargs):
        print("task 1")

    @task()
    def task_two(**kwargs):
        print("task 2")
        time.sleep(5)

    @task()
    def task_three(**kwargs):
        print("task 3")

    @task()
    def task_four(**kwargs):
        print("task 4")
        time.sleep(10)

    @task()
    def task_five(**kwargs):
        print("task 5")

    # t1 = BashOperator(task_id="hello_world1", bash_command='echo "Hi 1!!"')
    # t2 = BashOperator(task_id="hello_world2", bash_command='echo "Hi 2!!"')
    # t3 = BashOperator(task_id="hello_world3", bash_command='echo "Hi 3!!"')
    # t4 = BashOperator(task_id="hello_world4", bash_command='echo "Hi 4!!"')
    # t5 = BashOperator(task_id="hello_world5", bash_command='echo "Hi 5!!"')

    # this doesnt work wtih the taskflow api, it runs task_one() twice for some reason
    # Task 1 -> [Task 2, Task 4] -> [Task 3, Task 5]
    t1 = task_one()
    t2 = task_two()
    t3 = task_three()
    t4 = task_four()
    t5 = task_five()

    # Set dependencies using stored variables
    t1 >> [t2, t4] >> t5
    t1 >> t3

    # task_one() >> [task_two(), task_four()]
    # task_one() >> task_three() >> task_five()

    # task_one() >> [task_two(), task_three()]
    # task_two() >> [task_four(), task_five()]
    # task_three() >> task_five()

    # this works just fine
    # t1 >> [t2, t4]
    # t1 >> t3 >> t5


dag = parallel_test()
