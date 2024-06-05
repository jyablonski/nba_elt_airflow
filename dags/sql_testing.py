from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.utils import get_schedule_interval, jacobs_slack_alert

# from airflow.providers.common.sql.hooks.sql import DbApiHook
#
# https://github.com/apache/airflow/blob/main/airflow/providers/postgres/hooks/postgres.py
default_args = {
    "owner": "jacob",
    "depends_on_past": False,
    "email": "jyablonski9@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": jacobs_slack_alert,
}


@dag(
    "sql_test",
    schedule_interval=get_schedule_interval("0 12/4 * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "start_date": Param(
            default=f"{datetime.today().date()}", type="string", format="date"
        )
    },
    tags=["example"],
)
def sql_test_pipeline():
    @task()
    def test_task(
        run_date: datetime.date,
        **context: dict,
    ):
        print(run_date)

        pg_hook = PostgresHook(postgres_conn_id="nba_database")

        # list of tuples
        data = pg_hook.get_records("select * from information_schema.tables;")
        print(type(data))
        print(data)

        # pandas dataframe
        data2 = pg_hook.get_pandas_df("select * from information_schema.tables;")
        print(type(data2))
        print(data2)

        # create the table if it does not exist
        # data2.to_sql(
        #     name="jacobs_airflow_tester",
        #     con=pg_hook.get_sqlalchemy_engine(),
        #     schema="public",
        #     if_exists="replace",
        #     chunksize=1000,
        # )

        d = pg_hook.get_uri()
        print(d)

        d1 = pg_hook.get_conn()
        print(d1)

        pass

    test_task(run_date="{{ params['start_date'] }}")


dag = sql_test_pipeline()
