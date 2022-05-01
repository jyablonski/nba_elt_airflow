from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

# git remote add origin git@github.com:jyablonski/nba_elt_airflow.git
# git push --set-upstream origin master

# https://stackoverflow.com/questions/50591886/airflow-s3keysensor-how-to-make-it-continue-running
# basically make a lambda function to call the trigger_dag whenever a file lands in s3 via the airflow rest api.
with DAG(
    "s3_sensor",
    schedule_interval="0 11 * * *",
    start_date=datetime(2021, 11, 7),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
) as dag:

    # shouldnt rly ever use this - do this instead https://stackoverflow.com/questions/50591886/airflow-s3keysensor-how-to-make-it-continue-running
    sensor = S3KeySensor(
        task_id="s3_sensor_test",
        bucket_key="boxscores/boxscores-*",
        wildcard_match=True,
        bucket_name="jacobsbucket97",
        timeout=18 * 60 * 60,
        poke_interval=10,
    )

    dummy_task1 = DummyOperator(task_id="dummy_task1")

    dummy_task2 = DummyOperator(task_id="dummy_task2")

    dummy_task3 = DummyOperator(task_id="dummy_task3")

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="jyablonski9@gmail.com",
        subject="Airflow NBA S3 File SENSED",
        html_content="<h3>Process Completed</h3>",
    )

    sensor >> [dummy_task1, dummy_task2, dummy_task3] >> send_email_notification
