from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator

from include.utils import get_schedule_interval, jacobs_slack_alert


def transfer_files_to_sftp(context, **kwargs):
    task_instance = context["task_instance"]
    test_entries = [1, 2, 3, 4, 5]

    # this opens up a new sftp connection every single time
    # this is exactly why i dont like operators, but there is no sftp hook
    for file_key in test_entries:
        print(f"processing ci/manifest_{file_key}.json")
        s3_to_sftp_task = S3ToSFTPOperator(
            task_id=f"transfer_file_{file_key}",
            s3_bucket="nba-elt-dbt-ci",
            s3_key=f"ci/manifest_{file_key}.json",
            sftp_conn_id="sftp_test",
            sftp_path="s-3c1f70c1714a4b398.server.transfer.us-east-1.amazonaws.com",
            dag=dag,
        )
        s3_to_sftp_task.execute(task_instance.get_template_context())


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
    "sftp_test_new",
    schedule_interval=get_schedule_interval("15 3-10 * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
)
def sftp_test_pipeline():
    @task()
    def test_task(
        **context: dict,
    ):
        transfer_files_to_sftp(context=context)
        pass

    test_task()


dag = sftp_test_pipeline()
