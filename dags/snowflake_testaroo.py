from datetime import datetime, timezone
import json

from airflow.decorators import dag, task
from airflow.models.param import Param
import pandas as pd


from include.aws_utils import write_to_s3
from include.common import DEFAULT_ARGS
from include.snowflake_utils import (
    get_snowflake_conn,
    load_snowflake_table_from_s3,
    build_snowflake_table_from_s3,
)
from include.utils import get_schedule_interval


@dag(
    "snowflake_testaroo",
    schedule=get_schedule_interval(None),
    start_date=datetime(2023, 7, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["snowflake", "manual"],
)
def pipeline():
    @task()
    def test_task(**context):
        row = {
            "id": 1,
            "full_name": "Jane Doe",
            "email": "jane.doe@example.com",
            "is_active": True,
            "signup_date": datetime.today().isoformat(),
            "last_login_ts": datetime.now(
                timezone.utc
            ).isoformat(),  # ISO-8601 UTC timestamp
            "birth_date": "1990-07-15",
            "created_at_unix": int(
                datetime.now(timezone.utc).timestamp()
            ),  # epoch time
            "account_balance": 1532.75,
            "metadata_json_str": json.dumps(
                {"plan": "premium", "referral": True, "tags": ["beta", "new"]}
            ),
            "preferences": {
                "notifications": {"email": True, "sms": False},
                "theme": "dark",
                "language": "en-US",
            },
        }

        df = pd.DataFrame(data=[row])
        s3_prefix = "parquet/test1.parquet"

        write_to_s3(
            dataframe=df,
            s3_bucket="jyablonski-nba-elt-prod",
            s3_path=s3_prefix,
        )

    test_task()


pipeline()
