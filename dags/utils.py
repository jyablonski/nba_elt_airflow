import os
import boto3

# accessed via systems manager -> parameter store

def practice_xcom_function(number: int = 5):
    print(f"the number is {number}!")
    return number

def get_owner(parameter: str) -> str:
    print(f"The owner is {parameter}!")
    return parameter


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


def airflow_email_prac_function():
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
