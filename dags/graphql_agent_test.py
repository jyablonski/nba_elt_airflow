from datetime import datetime, timedelta
import json
import logging
import os
from typing import Dict
import sys

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import boto3
import requests

from include.utils import jacobs_airflow_email, jacobs_discord_alert, jacobs_slack_alert

jacobs_default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": jacobs_slack_alert,
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
    schedule_interval=None,
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=["test", "graphql", "dev", "jacob"],
    default_args=jacobs_default_args,
)

# you have to return json for xcoms to work.
def taskflow():
    @task(task_id="api_trigger", retries=0)
    def api_trigger() -> Dict[str, str]:
        df = requests.post(api, json={"query": graphql_query()})
        if df.status_code != 200:
            raise AirflowException(f"API Data for {table} is empty Failed")
        return df.json()

    # passing data from 1st task to the 2nd task
    @task
    def write_to_s3(data: Dict[str, str]):
        print(data)
        s3 = boto3.client("s3")

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
