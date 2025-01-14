import json

from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.models import Variable
import awswrangler as wr
import boto3
import pandas as pd

from include.utils import get_instance_type

try:
    from .exceptions import S3PrefixCheckFail
except:
    from exceptions import S3PrefixCheckFail


def check_s3_file_exists(client: boto3.client, bucket: str, file_prefix: str) -> bool:
    """
    Function to check if a file exists in an S3 Bucket.

    Args:
        client (S3 Client) - Boto3 S3 Client Object

        bucket (str) - Name of the S3 Bucket (`jyablonski-dev`)

        file_prefix (str) - Name of the S3 File (`tables/my-table/my-table-2023-05-25.parquet`)

    Returns:
        None, but will raise an error if the file doesn't exist.
    """
    result = client.list_objects_v2(
        Bucket=bucket,
        Prefix=file_prefix,
        MaxKeys=1,
    )
    if "Contents" in result.keys():
        print(f"S3 File Exists for {bucket}/{file_prefix}")
        return True
    else:
        raise S3PrefixCheckFail(f"S3 Prefix for {bucket}/{file_prefix} doesn't exist")


# to use this you have to store the raw values in systems secure manager first
# accessed via systems manager -> parameter store
def get_ssm_parameter(
    parameter_name: str, decryption: bool = True, is_json: bool = False
) -> str | dict:
    """
    Function to grab parameters from SSM

    note: withdecryption = false will make pg user not work bc its a securestring.
        ignored for String and StringList parameter types

    When storing these in SSM do it on one line or JSON gets fkd up :)

    Args:
        parameter_name (string) - name of the parameter you want

        decryption (bool) - Optional parameter to specify if decryption is needed to access the parameter (default True)

        is_json (bool) - Optional parameter to specify if the parameter is a json dictionary

    Returns:
        parameter_value (string)
    """
    try:
        ssm = boto3.client("ssm")
        resp = ssm.get_parameter(Name=parameter_name, WithDecryption=decryption)

        if is_json is True:
            resp = json.loads(resp["Parameter"]["Value"])
            return resp
        else:
            return resp["Parameter"]["Value"]
    except BaseException as error:
        print(f"SSM Failed, {error}")
        df = {}
        return df


def get_secret_value(secret_name: str):
    """
    Function to grab a Secret from AWS Secrets Manager

    Args:
        secret_name (str): The Name of the Secret in Secrets Manager

    Returns:
        The Secret in a Dictionary

    """
    try:
        client = boto3.client("secretsmanager")
        creds = json.loads(
            client.get_secret_value(SecretId=secret_name)["SecretString"]
        )
        return creds
    except BaseException as e:
        raise e(f"Error Occurred while grabbing secret {secret_name}, {e}")


def write_to_s3(dataframe: pd.DataFrame, s3_bucket: str, s3_path: str) -> bool:
    try:
        if len(dataframe) == 0:
            print(
                f"Dataframe is empty, not writing to s3://{s3_bucket}/{s3_path}.parquet"
            )
            return True

        print(f"Writing DataFrame to s3://{s3_bucket}/{s3_path}.parquet")
        wr.s3.to_parquet(
            df=dataframe,
            path=f"s3://{s3_bucket}/{s3_path}.parquet",
            compression="snappy",
        )
        return True
    except BaseException as e:
        raise e(
            f"Error Occurred while writing dataframe to {s3_bucket}/{s3_path}.parquet"
        )


# def get_container_name_from_task_definition(task_definition: str) -> str:
#     """
#     Function to pull `container_name` from an ECS Task Definition. Needed
#     in order to add Airflow Env Vars onto an ECS Task when triggering it.
#     The container name cannot be changed; hence we need to provide it

#     Args:
#         task_definition (str): Name of the Task Definition in AWS

#     Returns:
#         `container_name` string of the ECS Task Definition
#     """
#     ecs_client = boto3.client("ecs")
#     response = ecs_client.describe_task_definition(taskDefinition=task_definition)
#     return response["taskDefinition"]["containerDefinitions"][0]["name"]


def create_ecs_task_operator(
    task_id: str,
    ecs_task_definition: str,
    container_name: str,
    environment_vars: dict = {},
    ecs_cluster: str | None = None,
    network_config: dict | None = None,
    awslogs_group: str | None = None,
    awslogs_stream_prefix: str | None = None,
    aws_conn_id: str = "aws_ecs",
    do_xcom_push: bool = True,
) -> EcsRunTaskOperator:
    """
    Creates an EcsRunTaskOperator with predefined configurations.

    Args:
        task_id (str): Task ID for the operator.

        ecs_task_definition (str): Name of the ECS Task Definition in AWS

        container_name (str): Name of the container in the ECS Task Definition.
            Has to be exact copy or else this fails because AWS is AWS and these
            Operators suck

        environment_vars (dict): Additional environment variables to pass to the    ECS container.

        ecs_cluster (str): ECS cluster name (optional).

        network_config (dict): Network configuration for the task (optional).

        awslogs_group (str): AWS CloudWatch log group name.

        awslogs_stream_prefix (str): AWS CloudWatch log stream prefix.

        aws_conn_id (str): Airflow AWS connection ID.

        do_xcom_push (bool): Whether to push the result to XCom.

    Returns:
        EcsRunTaskOperator: Configured EcsRunTaskOperator.
    """
    env = get_instance_type()

    # default to environment-based ECS cluster and network configuration
    if ecs_cluster is None:
        ecs_cluster = f"ecs_cluster_{env}"

    if network_config is None:
        network_config = Variable.get(
            "network_config", deserialize_json=True, default_var={}
        )

    # base environment variables that are always included
    base_environment_vars = {
        "DAG_RUN_TS": "{{ ts }}",
        "DAG_RUN_DATE": "{{ ds }}",
        "ENV": env,
    }

    # combine base and additional environment variables
    combined_environment_vars = {**base_environment_vars, **environment_vars}

    # convert environment variables dictionary into the required ECS format
    container_environment = [
        {"name": key, "value": value}
        for key, value in combined_environment_vars.items()
    ]

    return EcsRunTaskOperator(
        task_id=task_id,
        aws_conn_id=aws_conn_id,
        cluster=ecs_cluster,
        task_definition=ecs_task_definition,
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": container_name,
                    "environment": container_environment,
                }
            ]
        },
        network_configuration=network_config,
        awslogs_group=awslogs_group,
        awslogs_stream_prefix=awslogs_stream_prefix,
        do_xcom_push=do_xcom_push,
    )
