from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from include.utils import jacobs_slack_alert


jacobs_default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": ["jyablonski9@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=30),
    "on_failure_callback": jacobs_slack_alert,
}

# do this to assume a different iam role
# batch_conn_id = "BATCH_AWS_CONNECTION"

# conn = Connection(
#     conn_id=batch_conn_id,
#     conn_type="aws",
#     extra={
#         "region_name": "us-east-1",
#         "role_arn": "arn:aws:iam::28821312332:role/test-role-in-other-account"
#     },
# )

# env_key=f"AIRFLOW_CONN_{conn.conn_id}"
# conn_uri=conn.get_uri()
# os.environ[env_key]=conn_uri


def jacobs_ecs_task(dag: DAG) -> BatchOperator:

    return BatchOperator(
        task_id="submit_batch_job",
        dag=dag,
        job_name="jacobs-airflow-job",  # can be named anything
        job_queue="jacobs-batch-queue",  # has to be setup in aws batch
        job_definition="arn:aws:batch:us-east-1:288364792694:job-definition/jacobs-job-definition:5",  # has to be setup in aws batch
        overrides={
            "environment": [{"name": "string", "value": "string"},],
            "command": ["echo hello world"],
        },
        parameters={"scheduledStartTime": "{{ data_interval_end }}"},
    )


def create_dag() -> DAG:
    """
    xxx
    """

    dag = DAG(
        "aws_batch_template",
        catchup=False,
        default_args=jacobs_default_args,
        schedule_interval=None,
        start_date=datetime(2021, 11, 20),
        max_active_runs=1,
        tags=["ecs", "template"],
    )
    t1 = jacobs_ecs_task(dag)

    t1

    return dag


dag = create_dag()
