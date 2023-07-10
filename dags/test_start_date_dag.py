from datetime import datetime, timedelta


from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from include.utils import get_schedule_interval, jacobs_slack_alert

# can manually delete previous successful DAG runs w/ browse -> DAG Runs
# or can manually clear state previous successful DAG Runs

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

network_config = {
    "awsvpcConfiguration": {
        "securityGroups": ["1"],
        "subnets": ["2", "3",],
        "assignPublicIp": "ENABLED",
    }  # has to be enabled otherwise it cant pull image from ecr??
}


# even though the start date is today, if you enable the dag it wont run a scheduled run
# because it hasn't passed the first data interval end yet.
@dag(
    "test_start_date_dag",
    schedule_interval=get_schedule_interval("30 5 * * *"),
    start_date=datetime(2023, 7, 1),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["example"],
    params={"park": 0},
    render_template_as_native_obj=True,
)
def test_start_date_dag():
    @task()
    def test_task(
        park_id_param: int = "{{ params['park'] }}", **context
    ):  # can call this **kwargs, but context makes more sense.
        park_id = context["params"]["park"]
        print("{{ params['park'] }}")

        print("yoo {{ params['park'] }}")
        print(context)
        print(context["params"])
        print(f"park_id is {park_id}")

        return EcsRunTaskOperator(
            task_id="jacobs_airflow_ecs_ec2_task_dev",
            aws_conn_id="aws_ecs_ec2",
            cluster="jacobs-ecs-ec2-cluster",
            task_definition="hello-world-ec2",
            launch_type="EC2",
            # launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {  # change this to any of the task_definitons created in ecs
                        "name": "hello-world-ec2",
                        "environment": [
                            {
                                "name": "dag_run_ts",
                                "value": "{{ ts }}",
                            },  # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
                            {"name": "dag_run_date", "value": " {{ ds }}",},
                            {"name": "test", "value": park_id},
                        ],
                    }
                ],
                "executionRoleArn": "arn:aws:iam::288364792694:role/jacobs_ecs_role",
                "taskRoleArn": "arn:aws:iam::288364792694:role/jacobs_ecs_role",
            },
            network_configuration=network_config,
            awslogs_group="/ecs/hello-world-ec2",
            awslogs_stream_prefix="ecs/hello-world-ec2",
            # this ^ stream prefix shit is prefixed with ecs/ followed by the container name which comes from line 48.
            do_xcom_push=True,
        )
        # timestamp = context["data_interval_end"].strftime("%Y-%m-%dT%H:%M:%SZ")
        # print(f"timestampzzz is {timestamp}")

        # client = boto3.client('s3')
        # check_s3_file_exists(
        #     client,
        #     bucket="jacobsbucket97-dev",
        #     file_prefix="graphql/lambda_function.zip"
        # )

        # print(f"Sleeping for 30 seconds")
        # time.sleep(30)

    test_task()


dag = test_start_date_dag()
# test_start_date_dag() # either of these work
