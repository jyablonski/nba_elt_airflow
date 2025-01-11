from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

from include.common import DEFAULT_ARGS
from include.utils import get_schedule_interval


@dag(
    "sql_testing_v2",
    schedule_interval=get_schedule_interval("0 12/4 * * *"),
    start_date=datetime(2023, 9, 23, 15, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["example"],
)
def sql_test_pipeline():
    @task()
    def test_task(
        **context: dict,
    ):
        table = "jacob_loop_test"
        run_date = datetime(2023, 12, 29).date()

        pg_hook = PostgresHook(postgres_conn_id="nba_database")
        conn = pg_hook.get_conn()
        engine = pg_hook.get_sqlalchemy_engine()
        conn.autocommit = True
        cursor = conn.cursor()

        # airflow.providers.***.hooks.***.PostgresHook'
        print(type(pg_hook))

        # 'psycopg2.extensions.connection'
        print(type(conn))

        # 'sqlalchemy.engine.base.Engine'>
        print(type(engine))

        # 'psycopg2.extensions.cursor'
        print(type(cursor))

        print(f"Hook URI: {pg_hook.get_uri()}")

        # this works
        with engine.begin() as engine_conn:
            engine_conn.execute(
                f"insert into public.{table}(run_date)  values ('1997-01-15');"
            )

        # so does cursor.execute
        while True and run_date >= datetime(2023, 12, 1).date():
            print(f"running for {run_date}")
            cursor.execute(
                f"insert into public.{table}(run_date)  values ('{run_date}');"
            )
            run_date = run_date - timedelta(days=1)

        print(context)
        ts = context["data_interval_end"]
        df = pd.DataFrame(
            {"id": [1, 2, 3], "value": [100, 200, 300], "created": [ts, ts, ts]}
        )

        # have to use
        df.to_sql("jacob_pandas_test", con=engine, schema="public", if_exists="append")

        # this also works
        with engine.begin() as engine_conn:
            df.to_sql(
                "jacob_pandas_test",
                con=engine_conn,
                schema="public",
                if_exists="append",
            )

    test_task()


dag = sql_test_pipeline()
