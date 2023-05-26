import json

import boto3

try:
    from .exceptions import S3PrefixCheckFail
except:
    from exceptions import S3PrefixCheckFail


def check_s3_file_exists(client, bucket: str, file_prefix: str):
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
    else:
        raise S3PrefixCheckFail(f"S3 Prefix for {bucket}/{file_prefix} doesn't exist")


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
