from datetime import datetime
from airflow.decorators import dag, task


from include.aws_utils import create_ecs_task_operator
from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval


@dag(
    "ecs_test",
    schedule=get_schedule_interval(cron_schedule="0 12 * * *"),
    start_date=datetime(2023, 7, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["example"],
)
def pipeline():
    
    def ecs_task():
        return create_ecs_task_operator(
            task_id="ecs_task_example",
            ecs_task_definition="jacobs_fake_task",
            ecs_cluster="jacobs_fargate_cluster",
            environment_vars={"env1": "val1", "env2": "val2"},
        )

    @task()
    def print_hello_world():
        print("hello world")

        return 1

    ecs_task() >> print_hello_world()


pipeline()
