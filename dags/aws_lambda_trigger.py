from datetime import datetime, timedelta
import json

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)

from include.utils import get_schedule_interval, jacobs_slack_alert

default_args = {
    "owner": "jacob",
    "depends_on_past": True,
    "email": "jyablonski9@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": jacobs_slack_alert,
}


@dag(
    "aws_lambda_trigger",
    schedule_interval=get_schedule_interval(None),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    params={
        "num_rows": Param(
            default=100,
            type="integer",
            title="Number of Rows",
            description="This Param determines how many rows the Fake Data Generator will create",
        ),
        "file_name": Param(
            type="string",
            title="File Name",
            description="This Param determines the file name that the Fake Data Generator will create. "
            "This file is stored in `jyablonski-nba-elt-prod/fake_data/*`",
        ),
    },
    render_template_as_native_obj=False,
    tags=["example"],
)
def aws_lambda_trigger_pipeline():
    @task()
    def test_task(
        **context: dict,
    ):
        print(f"Hello world {context}")

    # Accessing the DAG params dynamically
    num_rows = "{{ params.num_rows }}"
    file_name = "{{ params.file_name }}"
    print("hi")

    invoke_lambda_function = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda_function",
        function_name="fake_data_generator",
        payload=json.dumps({"num_rows": num_rows, "file_path": file_name}),
    )

    @task()
    def follow_up(**context):
        lambda_response = context["ti"].xcom_pull(task_ids="invoke_lambda_function")
        print(f"Lambda Response was {lambda_response}")
        return None

    test_task() >> invoke_lambda_function >> follow_up()


dag = aws_lambda_trigger_pipeline()
