# from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.decorators import task

# from utils import jacobs_slack_alert

# # https://airflow.apache.org/docs/apache-airflow/2.3.0/concepts/dynamic-task-mapping.html
# default_args = {
#     "owner": "jacob",
#     "depends_on_past": False,
#     "email": ["jyablonski9@gmail.com"],
#     "email_on_failure": True,
#     "email_on_retry": True,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=30),
#     "on_failure_callback": jacobs_slack_alert,
# }


# @task
# def make_list():
#     # This can also be from an API call, checking a database, -- almost anything you like, as long as the
#     # resulting list/dictionary can be stored in the current XCom backend.
#     return [1, 2, {"a": "b"}, "str"]


# @task
# def consumer(arg):
#     print(list(arg))


# with DAG(
#     "jacobs_dynamic_task_dag",
#     # schedule_interval="0 11 * * *",
#     schedule_interval=None,
#     start_date=datetime(2021, 10, 1),
#     dagrun_timeout=timedelta(minutes=5),
#     default_args=default_args,
#     catchup=False,
#     tags=["test", "qa", "slack", "dynamic_tasks"],
# ) as dag:

#     @task
#     def add_one(x: int):
#         return x + 1

#     @task
#     def sum_it(values):
#         total = sum(values)
#         print(f"Total was {total}")

#     # added_values = add_one.expand(x=[1, 2, 3])
#     # sum_it(added_values)

#     # you're calling the add_one task on each input you give it.
#     # so here, 8 subtasks are actually getting created.
#     first = add_one.expand(x=[1, 2, 3, 4, 5, 6, 7, 8])

#     # and then you store that value into first and sum it.
#     second = sum_it(first)

#     # added_values >> sum_it
#     # consumer.expand(arg=make_list())
#     first >> second
