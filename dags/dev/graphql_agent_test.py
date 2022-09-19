from datetime import datetime, timedelta
import json
import logging
from typing import Dict
import sys

from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import boto3
import requests

from utils import jacobs_airflow_email, jacobs_discord_alert, jacobs_slack_alert

jacobs_default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": jacobs_discord_alert,
}

api = "https://graphql.jyablonski.dev/graphql"
table = "redditComments"


def graphql_query():
    query = """
    {
    redditComments {
        scrapeDate
        author
        comment
        flair
        score
        url
        compound
        neg
        neu
        pos
    }
    }
    """
    return query


@dag(
    "graphql_agent_test",
    schedule_interval="@daily",
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=["test", "graphql", "dev", "jacob"],
    default_args=jacobs_default_args,
)

# you have to return json for xcoms to work.
def taskflow():
    @task(task_id="api_trigger", retries=0)
    def api_trigger() -> Dict[str, str]:
        return requests.post(api, json={"query": graphql_query()}).json()

    @task
    def write_to_s3(data: Dict[str, str]):
        s3 = boto3.client("s3")
        if data is None:
            raise AirflowException("S3 Task Failed")
        s3.put_object(
            Body=json.dumps(data["data"][table]),
            Bucket="jacobsbucket97-dev",
            Key=f"json_test/graphql_{table}_{datetime.now().date()}.json",
        )
        pass

    email_notification = EmailOperator(
        task_id="email_notification",
        to="jyablonski9@gmail.com",
        subject="graphql dag completed",
        html_content=jacobs_airflow_email(),
    )

    write_to_s3(api_trigger()) >> email_notification


dag = taskflow()


### THE OLD WAY
# with DAG(
#     'tutorial_etl_dag',
#     # These args will get passed on to each operator
#     # You can override them on a per-task basis during operator initialization
#     default_args={'retries': 2},
#     description='ETL DAG tutorial',
#     schedule_interval=None,
#     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
#     catchup=False,
#     tags=['example'],
# ) as dag:
#     dag.doc_md = __doc__
#     def extract(**kwargs):
#         ti = kwargs['ti']
#         data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
#         ti.xcom_push('order_data', data_string)
#     def transform(**kwargs):
#         ti = kwargs['ti']
#         extract_data_string = ti.xcom_pull(task_ids='extract', key='order_data')
#         order_data = json.loads(extract_data_string)

#         total_order_value = 0
#         for value in order_data.values():
#             total_order_value += value

#         total_value = {"total_order_value": total_order_value}
#         total_value_json_string = json.dumps(total_value)
#         ti.xcom_push('total_order_value', total_value_json_string)
#     def load(**kwargs):
#         ti = kwargs['ti']
#         total_value_string = ti.xcom_pull(task_ids='transform', key='total_order_value')
#         total_order_value = json.loads(total_value_string)

#         print(total_order_value)
#     extract_task = PythonOperator(
#         task_id='extract',
#         python_callable=extract,
#     )
#     extract_task.doc_md = dedent(
#         """\
#     #### Extract task
#     A simple Extract task to get data ready for the rest of the data pipeline.
#     In this case, getting data is simulated by reading from a hardcoded JSON string.
#     This data is then put into xcom, so that it can be processed by the next task.
#     """
#     )

#     transform_task = PythonOperator(
#         task_id='transform',
#         python_callable=transform,
#     )
#     transform_task.doc_md = dedent(
#         """\
#     #### Transform task
#     A simple Transform task which takes in the collection of order data from xcom
#     and computes the total order value.
#     This computed value is then put into xcom, so that it can be processed by the next task.
#     """
#     )

#     load_task = PythonOperator(
#         task_id='load',
#         python_callable=load,
#     )
#     load_task.doc_md = dedent(
#         """\
#     #### Load task
#     A simple Load task which takes in the result of the Transform task, by reading it
#     from xcom and instead of saving it to end user review, just prints it out.
#     """
#     )

#     extract_task >> transform_task >> load_task
