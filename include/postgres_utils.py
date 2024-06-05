from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection


def create_pg_sqlalchemy_conn(postgres_conn: str) -> Connection:
    """
    Function to create a Postgres SQLAlchemy Connection for use w/
    Pandas `read_sql_query` and `to_sql` methods.

    Args:
        postgres_conn (str): The name of the Postgres Connection in Airflow

    Returns:
        connection (SQLAlchemy Connection): The SQLAlchemy Connection Object
    """
    # was getting some dumb fucking `invalid dsn: invalid connection option "__extra__"`
    # error using the `pg_hook.get_sqlalchemy_engine()` method because AIRFLOW
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn)

    conn = pg_hook.get_connection(postgres_conn)
    conn_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

    engine = create_engine(conn_url)
    connection = engine.connect()

    return connection
