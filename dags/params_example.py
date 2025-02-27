from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval, read_dag_docs


@dag(
    "params_example",
    schedule_interval=get_schedule_interval("*/2 * * * *"),
    start_date=datetime(2024, 2, 1),
    catchup=False,
    max_active_runs=1,
    doc_md=read_dag_docs("params_example"),
    default_args=DEFAULT_ARGS,
    params={
        "test": Param(
            default=None,
            type=["null", "number", "string"],
            description="hello world description",
            title="Test Param",
        ),
        "start_date": Param(
            default=None,
            type=["null", "string"],
            description="Start Date to Run for.  ex: 2024-02-01",
            title="Start Date",
            format="date",
        ),
        "start_timestamp": Param(
            default=None,
            type=["null", "string"],
            description="Start Timestamp to Run for.  ex: 2024-02-01",
            title="Start Timestamp",
            format="date-time",
        ),
        "list_items": Param(
            default=None,
            type=["null", "array"],
            description="List of Items to run for.  *EACH ITEM* in the array has to be on its own line",
            title="List of Items",
        ),
        "is_full_backfill": Param(
            default=False,
            type="boolean",
            description="Is the Run a Full Backfill?  ex: false",
            title="Is Full Backfill",
        ),
        "pick_one": Param(
            "Store A",
            type="string",
            title="Select one Value",
            description="You can use JSON schema enum's to generate drop down selection boxes.",
            enum=["Store A", "Store 2", "Store Z", "Taddadootie"],
        ),
    },
    render_template_as_native_obj=True,
    tags=["example"],
)
def params_example_pipeline():
    @task()
    def test_task(**context):
        print("hello world")
        print(context["data_interval_end"])
        print(f"{context['params']['test']}")
        print(f"{context['params']['start_date']}")
        print(f"{context['params']['start_timestamp']}")
        print(f"{context['params']['list_items']}")
        print(f"{context['params']['is_full_backfill']}")

        # print(context["data_interval_end"])
        # if context["data_interval_end"]

    test_task()


dag = params_example_pipeline()
