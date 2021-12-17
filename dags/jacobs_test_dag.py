""" Example Airflow DAG with custom plugin"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from utils import my_function, get_ssm_parameter, airflow_email_prac_function, practice_xcom_function

# https://stackoverflow.com/questions/46059161/airflow-how-to-pass-xcom-variable-into-python-function

JACOBS_DEFAULT_ARGS = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

# my_function()

DBT_PROFILE_DIR = "~/.dbt/"
DBT_PROJECT_DIR = "~/airflow/dags/dbt/"

with DAG(
    "jacobs_test_dag",
    # schedule_interval="0 11 * * *",
    start_date=datetime(2021, 10, 1),
    dagrun_timeout=timedelta(minutes=60),
    default_args=JACOBS_DEFAULT_ARGS,
    catchup=False,
    tags=["test", "qa"],
) as dag:

    # fk

    dummy_task = DummyOperator(task_id="dummy_task")

    bash_push = BashOperator(
        task_id='bash_push',
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

    jacobs_xcom_function = PythonOperator(
        task_id="jacobs_xcom_function",
        python_callable = practice_xcom_function
    )

    # dbt_deps = BashOperator(
    #   task_id="dbt_deps",
    #   bash_command=f"dbt deps --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJECT_DIR}"
    # )

    # python_dummy_task = PythonOperator(
    #   task_id="python_dummy_task",
    #   python_callable=my_function
    # )

    send_email_notification = EmailOperator(
      task_id="send_email_notification",
      to="jyablonski9@gmail.com",
      subject="Airflow Test Dag run on {{ ds }}",
      html_content="""
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
      ts: {{ ts }}custom
      <br>
      ts nodash: {{ts_nodash }}
      <br>
      dag: {{ dag }}
      <br>
      task: {{ task }}
      <br>
      run_id: {{ run_id }}
      <br>    # 
      dag run: {{ dag_run }}
      <br>
      owner: {{ task.owner}}
      <br>
      xcom value manual: {{ ti.xcom_pull(key="manually_pushed_key", task_ids='bash_push') }}
      <br>
      xcom value return: {{ ti.xcom_pull(key="return_value", task_ids='bash_push') }}
      """
    )
    

    # send_email_notification_custom = EmailOperator(
    #     task_id="send_email_notification_custom",
    #     to="jyablonski9@gmail.com",
    #     subject="Airflow Test Dag run on {{ ds }}",
    #     html_content=airflow_email_prac_function(),
    # )

    # dummy_task >> [python_dummy_task, dbt_deps] >> send_email_notification

    dummy_task >> bash_push >> jacobs_xcom_function >> send_email_notification
