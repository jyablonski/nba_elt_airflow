from datetime import datetime

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


# these 2 functinos below are used to retrieve & store the last record timestamp values
# during pipeline pulls, with the goal of being able to use this information for efficient
# incremental pulls on sources that support filtering.
def get_last_load_timestamp(
    pipeline_name: str, table_name: str, postgres_conn_id: str = "postgres_default"
) -> datetime | None:
    """
    Retrieve the last successful load timestamp for a specific pipeline and table.

    This timestamp represents the maximum source record timestamp from the previous
    pipeline run and should be used as the starting point for incremental data extraction.

    Args:
        pipeline_name: Name of the data pipeline (e.g., 'salesforce_sync')

        table_name: Name of the source table (e.g., 'customers')

        postgres_conn_id: Airflow connection ID for Postgres database

    Returns:
        datetime: The last_record_timestamp if found, None if this is the first run
    """
    query = """
        SELECT last_record_timestamp
        FROM pipeline_metadata
        WHERE 
            pipeline_name = %s
            AND table_name = %s
    """

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    result = hook.get_first(query, parameters=[pipeline_name, table_name])

    return result[0] if result else None


def update_pipeline_load_metadata(
    pipeline_name: str,
    table_name: str,
    max_source_timestamp: datetime,
    postgres_conn_id: str = "postgres_default",
) -> None:
    """
    Update pipeline metadata tracking after successful table load.

    Records the maximum timestamp from source data and the current execution time
    to enable incremental loading in subsequent pipeline runs.

    Args:
        pipeline_name: Name of the data pipeline (e.g., 'salesforce_sync')

        table_name: Name of the source table being loaded (e.g., 'customers')

        max_source_timestamp: Maximum timestamp value from the source data records

        postgres_conn_id: Airflow connection ID for Postgres database

    Returns:
        None
    """
    # `EXCLUDED.last_record_timestamp` will update the row w/ the new timestamp
    # we're passing in
    query = """
        INSERT INTO pipeline_metadata 
        (pipeline_name, table_name, last_record_timestamp, last_run_timestamp)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (pipeline_name, table_name) 
        DO UPDATE SET 
            last_record_timestamp = EXCLUDED.last_record_timestamp,
            last_run_timestamp = CURRENT_TIMESTAMP
    """

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(query, parameters=[pipeline_name, table_name, max_source_timestamp])

    return None
