from datetime import datetime, timedelta
import os

from airflow.settings import Session
from airflow.models.connection import Connection
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from airflow.providers.pagerduty.notifications.pagerduty import (
    send_pagerduty_notification,
)


try:
    from .exceptions import NoConnectionExists
except:  # noqa: E722
    from exceptions import NoConnectionExists

SLACK_CONN_ID = "slack"


def get_instance_type(instance_type: str = None) -> str:
    """
    Function used to grab the instance type of Airflow for Dev / Prod environments

    Args:
        instance_type (str): The Instance Type of the Instance. Defaults to grabbing from `ASTRO_INSTANCE_TYPE`

    Returns:
        str: The normalized Instance Type, which can be passed into other functions
    """
    if instance_type is None:
        instance_type = os.environ.get("ASTRO_INSTANCE_TYPE", "dev")

    if "prod" in instance_type:
        return "prod"
    else:
        return "dev"


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

        is_override (bool): Boolean value to allow the DAG to be scheduled in dev environment

        instance_type (str): The Instance Type of the Airflow Instance

    Returns:
        The Cron Schedule for the DAG
    """
    if is_override and instance_type == "dev":
        return cron_schedule
    elif instance_type == "prod":
        return cron_schedule
    else:
        return None


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
    slack_hook = SlackWebhookHook(
        slack_webhook_conn_id=slack_conn_id,
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


def jacobs_slack_alert(slack_webhook_conn_id: str = "slack") -> None:
    """
    Function that sends a Slack Message when a Task Fails using the
    `on_failure_callback` parameter in the DAG.

    Slack offers two options, either a webhook URL which is connected
    to a specific channel or a Slack bot which has access to all channels.
    If you want to write to multiple channels you either need to set
    up a bot or configure a Slack Hook per Channel.

    Args:
        slack_webhook_conn_id (str): The name of the Slack Connection
            in the Airflow UI

    Returns:
        None, but writes the Message specified to Slack
    """

    # this subfunction was needed to pass the context and allow users
    # to select a slack connection when calling this in the on_failure_callback

    # `_alert` can reference `slack_webhook_conn_id` and any other variables
    # defined in the parent function.
    def _alert(context: dict[str, str]) -> None:
        ti = context["task_instance"]
        slack_msg = f"""
                :red_circle: Task Failed. 
            *Exception*: {context["exception"]}
            *Task*: {ti.task_id}
            *Dag*: {ti.dag_id} 
            *Owner*: {ti.task.owner}
            *Execution Time*: {context["execution_date"]}  
            *Log Url*: {ti.log_url} 
                """
        failed_alert = SlackWebhookOperator(
            task_id="slack_task",
            slack_webhook_conn_id=slack_webhook_conn_id,
            message=slack_msg,
        )
        return failed_alert.execute(context=context)

    return _alert


def jacobs_pagerduty_notification(
    pagerduty_events_conn_id: str = "pagerduty",
    severity: str = "error",
    class_type: str = "Data Pipeline",
) -> None:
    """
    Function to send a PagerDuty Notification when a Task Fails.

    Args:
        pagerduty_events_conn_id (str): The name of the PagerDuty Connection
            in the Airflow UI

        severity (str): The Severity of the PagerDuty Alert. Can be
            `info`, `warning`, `error`, `critical`

        class_type (str): The Class Type of the PagerDuty Alert

    Returns:
        None, but sends a PagerDuty Notification

    """

    def _send_notification(context: dict[str, str]):
        print("hello wrold")
        ti = context["task_instance"]
        summary = f"DAG {ti.dag_id} Failure"
        source = f"airflow dag_id: {ti.dag_id}"
        dedup_key = f"{ti.dag_id}-{ti.task_id}"

        alert = send_pagerduty_notification(
            pagerduty_events_conn_id=pagerduty_events_conn_id,
            summary=summary,
            severity=severity,
            source=source,
            dedup_key=dedup_key,
            group=ti.dag_id,
            component="airflow",
            class_type=class_type,
        )

        # LETS FKN GO
        return alert.notify(context=context)

    return _send_notification


def discord_owner_ping(task_owner: str) -> str:
    """
    Function to ping a user in Discord for use in the `jacobs_discord_alert`
    Function. If you want to ping multiple users you can separate them with a space.

    If you have multiple people you can ping 1 user or multiple users like below
    right click their username and copy user id to get the id

    Args:
        task_owner (str): The Owner of the Task that Failed

    Returns:
        The Discord User ID of the User
    """
    if task_owner == "jacob":
        return "<@95723063835885568> <@995779012347572334>"
    else:
        return task_owner


def jacobs_discord_alert(context):
    # https://github.com/apache/airflow/blob/main/airflow/providers/discord/operators/discord_webhook.py
    # just make an http connection with host as https://discord.com/api/
    # and extra as {"webhook_endpoint": "webhooks/000/xxx-xxx"}
    ti = context["task_instance"]
    # print(ti)
    discord_msg = f"""
            :red_circle: Task Failed. 
            *Exception*: {context["exception"]}
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


def multi_failure_alert(context) -> None:
    """
    On Failure Callback that sends a Slack Message and a Discord Message.
    Can be altered to send to other services like PagerDuty, etc.

    Args:
        context: Airflow Context

    Returns:
        None, but sends a Slack Message and a Discord Message
    """
    # not sure why this ()(context) is needed but it is
    # for it to work
    jacobs_slack_alert()(context)
    jacobs_discord_alert(context)
    return


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


def loop_through_days(start_date: str, end_date: str):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    current_date = start_date

    while current_date <= end_date:
        # do your processing here for each day in between the 2 dates
        print(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)


def send_email(smtp_hook, email_body: str):
    smtp_hook.send_email_smtp(
        to="jyablonski9@gmail.com",
        subject="hello world",
        html_content=email_body,
    )


def start_log_block(group: str) -> None:
    print(f"::group::{group}")
    return


def end_log_block() -> None:
    print("::endgroup::")
    return


def read_dag_docs(dag_name: str) -> str:
    docs_path = f"dags/docs/{dag_name}.md"

    if not os.path.exists(docs_path):
        return f"No Documentation found for {dag_name}, please create a doc under \
            docs/{dag_name}.md to add Documentation"

    with open(docs_path) as f:
        return f.read()


def generate_env_schedule(
    prod_schedule: str, dev_schedule: str | None = None
) -> str | None:
    instance_type = get_instance_type()

    if instance_type == "prod":
        return prod_schedule
    else:
        return dev_schedule
