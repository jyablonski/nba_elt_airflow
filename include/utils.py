import os

from airflow.settings import Session
from airflow.models.connection import Connection
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator

try:
    from .exceptions import NoConnectionExists
except:
    from exceptions import NoConnectionExists

SLACK_CONN_ID = "slack"


def get_instance_type(instance_type: str = os.environ.get("ASTRO_INSTANCE_TYPE")):
    """
    Function used to grab the instance type of Airflow for Dev / Stg / Prod environments

    Args:
        instance_type (str): The Instance Type of the Instance.  Defaults to grabbing from `ASTRO_INSTANCE_TYPE`

    Returns:
        The Instance Type of the Instance, which can be passed into other functions like `get_schedule_interval`
            to control whether the DAG should be triggered in lower environments or only in Prod.

    """
    if instance_type is None:
        instance_type = "dev"

    return instance_type


def get_schedule_interval(
    cron_schedule: str = None,
    is_override: bool = False,
    instance_type: str = get_instance_type(),
):
    """
    Function to control the Cron Scheduling of DAGs across multiple environments.
    Typical use case is you have a DAG running in Prod but you don't want it to be scheduled in
    Dev and/or Staging.

    Args:
        cron_schedule (str): The Cron Schedule for the DAG

        is_override (bool): Boolean value to allow the DAG to be scheduled in Stg environment

        instance_type (str): The Instance Type of the Airflow Instance

    Returns:
        The Cron Schedule for the DAG
    """
    if (is_override is True) & (instance_type == "stg"):
        cron_schedule = cron_schedule
    elif instance_type == "prod":
        cron_schedule = cron_schedule
    else:
        cron_schedule = None

    return cron_schedule


def write_to_slack(slack_conn_id: str, context, message: str):
    """
    Function that writes a message to Slack.  A Slack Hook Connection needs
    to be created in the Airflow UI in order for this to work.

    Args:
        slack_conn_id (str): Name of the Slack Connection made in the UI

        context: Airflow Context for the Task where this is ran

        message (str): The Message to send in Slack

    Returns:
        None, but writes the Message specified to Slack
    """
    slack_hook = SlackWebhookOperator(
        http_conn_id=slack_conn_id,
    )
    ti = context["task_instance"]

    slack_msg = f"""
        *Task*: {ti.task_id}
        *DAG*: {ti.dag_id}
        *Execution Date*: {context["execution_date"]}
        *Log URL*: {ti.log_url}
        *Message*: {message}"""

    print(f"Sending Slack Message for {slack_conn_id}")
    slack_hook.send(text=slack_msg)


def jacobs_airflow_email():
    email = """
      <h3>Process {{ ts }} Completed</h3>
      <br>
      ds start: {{ data_interval_start }}
      <br>
      ds end: {{ data_interval_end }}
      <br>
      ds: {{ ds }}
      <br>
      ds nodash: {{ ds_nodash }}
      <br>
      ts: {{ ts }}
      <br>
      ts nodash: {{ts_nodash }}
      <br>
      dag: {{ dag }}
      <br>
      task: {{ task }}
      <br>
      run_id: {{ run_id }}
      <br>
      dag run: {{ dag_run }}
      <br>
      owner: {{ task.owner}}
      """
    return email


# these are task / dag failure alert webhooks for slack + discord
# you have to set both of them up in admin -> connections


def jacobs_slack_alert(context):
    # the context houses all of the metadata for the task instance currently being ran, and the dag it's connected to.
    # slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    ti = context["task_instance"]
    slack_msg = f"""
            :red_circle: Task Failed. 
            *Exception*: {context['exception']}
            *Task*: {ti.task_id}
            *Dag*: {ti.dag_id} 
            *Owner*: {ti.task.owner}
            *Execution Time*: {context["execution_date"]}  
            *Log Url*: {ti.log_url} 
            """
    #  *context*: {context} for the exhaustive list
    failed_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id="slack",
        message=slack_msg,
        channel="#airflow-channel",
    )
    return failed_alert.execute(context=context)


# if you have multiple people you can ping 1 user or multiple users like below
def discord_owner_ping(task_owner: str):
    if task_owner == "jacob":
        return "<@95723063835885568> <@995779012347572334>"
    else:
        return task_owner


def jacobs_discord_alert(context):
    # https://github.com/apache/airflow/blob/main/airflow/providers/discord/operators/discord_webhook.py
    # just make a discord connection with host as https://discord.com/api/ and extra as {"webhook_endpoint": "webhooks/000/xxx-xxx"}
    ti = context["task_instance"]
    # print(ti)
    discord_msg = f"""
            :red_circle: Task Failed. 
            *Exception*: {context['exception']}
            *Task*: {ti.task_id}
            *Dag*: {ti.dag_id} 
            *Owner*: {discord_owner_ping(ti.task.owner)}
            *Execution Time*: {context["execution_date"]}  
            *Log Url*: {ti.log_url} 
            """
    #  *context*: {context} for the exhaustive list
    failed_alert = DiscordWebhookOperator(
        task_id="discord_failure_callback_test",
        http_conn_id="discord",
        message=discord_msg,
        # avatar_url='https://a0.awsstatic.com/libra-css/images/logos/aws_logo_smile_1200x630.png', can change avatar this way
    )
    return failed_alert.execute(context=context)


def check_connections(conn: str, **context):
    session = Session()
    conns = session.query(Connection).all()

    # conns is a list of airflow connections, have to turn them into raw strings to do comparison
    conns = [str(x) for x in conns]
    if conn not in conns:
        raise NoConnectionExists(
            f"Requested Connection {conn} is not in Airflow Connections"
        )
    return 1
