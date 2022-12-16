""" Example Airflow DAG with custom plugin"""
from datetime import datetime, timedelta
import http
import os

from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator

from utils import (
    my_function,
    get_ssm_parameter,
    jacobs_airflow_email,
    practice_xcom_function,
    jacobs_slack_alert,
    jacobs_discord_alert,
)

# https://stackoverflow.com/questions/46059161/airflow-how-to-pass-xcom-variable-into-python-function

# send both an email alert + a slack alert to specified channel on any task failure
JACOBS_DEFAULT_ARGS = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": jacobs_slack_alert  # jacobs_discord_alert - apparently dont work at the same time together
    # [jacobs_slack_alert, jacobs_discord_alert]
}

# my_function()

DBT_PROFILE_DIR = "~/.dbt/"
DBT_PROJECT_DIR = "~/airflow/dags/dbt/"

with DAG(
    "jacobs_test_dag",
    # schedule_interval="0 11 * * *",
    schedule_interval=None,
    start_date=datetime(2021, 10, 1),
    dagrun_timeout=timedelta(minutes=60),
    default_args=JACOBS_DEFAULT_ARGS,
    catchup=False,
    tags=["test", "qa", "slack", "TEAM A"],
) as dag:

    # fk

    dummy_task = EmptyOperator(task_id="dummy_task")

    bash_push = BashOperator(
        task_id="bash_push",
        bash_command='echo "bash_push demo"  && '
        'echo "Manually set xcom value '
        '{{ ti.xcom_push(key="manually_pushed_key", value="manually_pushed_value") }}" && '
        'echo "why is this the return value"',
    )

    # bash_pull = BashOperator(
    #     task_id='bash_pull',
    #     bash_command='echo "bash pull demo" && '
    #     f'echo "The xcom pushed manually is {bash_push.output["manually_pushed_key"]}" && '
    #     f'echo "The returned_value xcom is {bash_push.output}" && '
    #     'echo "finished"',
    #     do_xcom_push=False,
    # )
    # jacobs_ecs_task = EcsRunTaskOperator(
    #     task_id="jacobs_airflow_ecs_task_test",
    #     aws_conn_id="aws_ecs",
    #     cluster="jacobs_fargate_cluster",
    #     task_definition="jacobs_task_airflow",
    #     launch_type="FARGATE",
    #     overrides={
    #         "containerOverrides": [
    #             {
    #                 "name": "jacobs_container_airflow",
    #                 "environment": [
    #                     {
    #                         "name": "dag_run_ts",
    #                         "value": "{{ ts }}",
    #                     },  # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
    #                     {
    #                         "name": "dag_run_date",
    #                         "value": " {{ ds }}",
    #                     },  # USE THESE TO CREATE IDEMPOTENT TASKS / DAGS
    #                 ],
    #             }
    #         ]
    #     },
    #     network_configuration=jacobs_network_config,
    #     awslogs_group="jacobs_ecs_logs_airflow",
    #     awslogs_stream_prefix="ecs/jacobs_container_airflow", # THIS WILL ALLOW YOU TO START STREAMING THE ECS LOGS IN CLOUDWATCH -TO- THE AIRFLOW LOGS
    #     # in terraform the stream prefix is just ecs, in airflow here u have to include ecs/<container_name> aka the whole thing
    #     do_xcom_push=True, # This pushes the last line of code in the script as an xcom return value.  can just push S3 file path instead.
    # ) # this works, basic idea is xcom push True means that it will grab the last event logged in the ecs logs and send that as an xcom

    # jacobs_xcom_function = PythonOperator(
    #     task_id="jacobs_xcom_function",
    #     python_callable = practice_xcom_function
    # )

    # dbt_deps = BashOperator(
    #   task_id="dbt_deps",
    #   bash_command=f"dbt deps --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}"
    # )

    # python_dummy_task = PythonOperator(
    #   task_id="python_dummy_task",
    #   python_callable=my_function
    # )

    # send_email_notification = EmailOperator(
    #   task_id="send_email_notification",
    #   to="jyablonski9@gmail.com",
    #   subject="Airflow Test Dag run on {{ ds }}",
    #   html_content="""
    #   <h3>Process {{ ts }} Completed</h3>
    #   <br>
    #   ds start: {{ data_interval_start }}
    #   <br>
    #   ds end: {{ data_interval_end }}
    #   <br>
    #   ds: {{ ds }}
    #   <br>
    #   ds nodash: {{ ds_nodash }}
    #   <br>
    #   ts: {{ ts }}custom
    #   <br>
    #   ts nodash: {{ts_nodash }}
    #   <br>
    #   dag: {{ dag }}
    #   <br>
    #   task: {{ task }}
    #   <br>
    #   run_id: {{ run_id }}
    #   <br>    #
    #   dag run ❌: {{ dag_run }} ❌
    #   <br>
    #   owner ✅: {{ task.owner}} ✅
    #   <br>
    #   xcom value manual: {{ ti.xcom_pull(key="manually_pushed_key", task_ids='bash_push') }}
    #   <br>
    #   xcom value return: {{ ti.xcom_pull(key="return_value", task_ids='bash_push') }}
    #   """
    # )

    send_email_notification_custom = EmailOperator(
        task_id="send_email_notification_custom",
        to="jyablonski9@gmail.com",
        subject="Airflow Test Dag run on {{ ds }}",
        html_content=jacobs_airflow_email(),
    )

    # send_slack_notification_failure_test = SlackWebhookOperator(
    #     task_id="send_slack_notification_failure_test",
    #     http_conn_id = "https://hooks.slack.com/services",
    #     webhook_token="/T02MXLF5XL2/B031T8G8VS5/z4bsKEc5pLPSHE8womRJFdqF",
    #     # http_conn_id=os.environ.get("CONNECTION_ID"),
    #     message="Dag run NEW {{ dag_run }} completed on {{ ds }} from owner ✅: {{ task.owner}} ✅",
    #     channel="#airflow-channel",
    # )

    send_slack_notification = SlackWebhookOperator(
        task_id="send_slack_notification",
        # http_conn_id="slack",
        http_conn_id="slack_PRACTICE_FAILURE",
        message="""
            :large_green_circle: Task Success
            *Task*: {{ task }}
            *DAG*: {{ dag }}
            *Execution Time*: {{ ts }}
            *Owner*: {{ task.owner }}
            ✅✅"
            """,
        channel="#airflow-channel",
    )

    send_discord_notification = DiscordWebhookOperator(
        task_id="send_discord_notification",
        http_conn_id="discord",
        message="""
            :large_green_circle: Task Success
            *Task*: {{ task }}
            *DAG*: {{ dag }}
            *Execution Time*: {{ ts }}
            *Owner*: {{ task.owner }}
            ✅✅"
            """,
    )

    # dummy_task >> [python_dummy_task, dbt_deps] >> send_email_notification

    dummy_task >> bash_push >> send_slack_notification >> send_discord_notification
