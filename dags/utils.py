import os
import boto3


from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator

SLACK_CONN_ID = "slack"

# when making a new aws account, make a jacobs_airflow_user with
# s3, ec2, ses, cloudwatch logs, ecs, ssm, ecstaskexecution, ec2container service policies
# and then create access key / secret pair and store them in ~/.aws
# accessed via systems manager -> parameter store


def practice_xcom_function(number: int = 5):
    print(f"the number is {number}!")
    return number


def get_owner(parameter: str) -> str:
    print(f"The owner is {parameter}!")
    return parameter


# to use this you have to store the raw values in systems secure manager first
# accessed via systems manager -> parameter store
def get_ssm_parameter(parameter_name: str, decryption: bool = True) -> str:
    """
    Function to grab parameters from SSM

    note: withdecryption = false will make pg user not work bc its a securestring.
        ignored for String and StringList parameter types

    Args:
        parameter_name (string) - name of the parameter you want

        decryption (Boolean) - Parameter if decryption is needed to access the parameter (default True)

    Returns:
        parameter_value (string)
    """
    try:
        ssm = boto3.client("ssm")
        resp = ssm.get_parameter(Name=parameter_name, WithDecryption=decryption)
        return resp["Parameter"]["Value"]
    except BaseException as error:
        print(f"SSM Failed, {error}")
        df = []
        return df


def my_function():
    print(
        f"""
           ssm_test: {get_ssm_parameter('jacobs_ssm_test')},
           subnet_1: {get_ssm_parameter('jacobs_ssm_subnet1')},
           subnet2: {get_ssm_parameter('jacobs_ssm_subnet2')},
           sg: {get_ssm_parameter('jacobs_ssm_sg_task')}
          """
    )


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
    if task_owner == 'jacob':
        return '<@95723063835885568> <@995779012347572334>'
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
