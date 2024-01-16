from datetime import datetime, timedelta
import time

from airflow.decorators import dag, task
from airflow.providers.smtp.hooks.smtp import SmtpHook

from include.utils import send_email, jacobs_slack_alert


default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": "jyablonski9@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": jacobs_slack_alert,
}


@dag(
    "smtp_test",
    # schedule_interval="0 0 12 1 4/6 ? *",
    schedule_interval=None,
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
)
def bash_test_pipeline():
    @task()
    def test_task(
        **context: dict,
    ):
        print(f"hi")

        # context manager so that it manages opening & closing the
        # smtp server
        with SmtpHook(smtp_conn_id="smtp_default") as smtp_hook:
            smtp_hook.send_email_smtp(
                to="jyablonski9@gmail.com",
                subject="hello world",
                from_email="jyablonski9@gmail.com",
                html_content="hi",
            )
        # try:
        #     send_email(smtp_hook=hook, email_body="hello world body")
        # except BaseException as e:
        #     time.sleep(3)
        #     raise e

    test_task()


dag = bash_test_pipeline()
