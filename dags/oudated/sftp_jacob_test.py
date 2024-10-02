# from datetime import datetime, timedelta
# from tempfile import NamedTemporaryFile

# from airflow.decorators import dag, task
# from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.ssh.hooks.ssh import SSHHook


# from include.utils import get_schedule_interval, jacobs_slack_alert


# def send_to_sftp(
#     sftp_client,
#     sftp_path: str,
#     s3_client,
#     s3_bucket: str,
#     s3_key: str,
# ):
#     print(f"Processing {s3_bucket}/{s3_key}, writing to {sftp_path}")
#     with NamedTemporaryFile("w") as f:
#         s3_client.download_file(s3_bucket, s3_key, f.name)
#         sftp_client.put(f.name, sftp_path)


# default_args = {
#     "owner": "jacob",
#     "depends_on_past": True,
#     "email": "jyablonski9@gmail.com",
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 0,
#     "retry_delay": timedelta(minutes=5),
#     "on_failure_callback": jacobs_slack_alert,
# }


# @dag(
#     "sftp_jacob_test",
#     schedule_interval=get_schedule_interval("15 3-10 * * *"),
#     start_date=datetime(2023, 9, 23, 15, 0, 0),
#     catchup=True,
#     max_active_runs=1,
#     default_args=default_args,
#     tags=["example"],
# )
# def sftp_jacob_test_pipeline():
#     @task()
#     def test_task(
#         **context: dict,
#     ):
#         ssh_hook = SSHHook(ssh_conn_id="sftp_test")
#         s3_hook = S3Hook()

#         client = s3_hook.get_conn()
#         sftp_conn = ssh_hook.get_conn().open_sftp()

#         test_entries = [1, 2, 3, 4, 5]

#         for i in test_entries:
#             send_to_sftp(
#                 sftp_client=sftp_conn,
#                 sftp_path=f"tables/{i}/manifest.json",
#                 s3_client=client,
#                 s3_bucket="nba-elt-dbt-ci",
#                 s3_key=f"ci/manifest_{i}.json",
#             )
#         pass

#     test_task()


# dag = sftp_jacob_test_pipeline()
