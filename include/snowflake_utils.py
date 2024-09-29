from typing import Any

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from sqlalchemy.engine.base import Connection

try:
    from .exceptions import SnowflakeCheckError
except:
    from exceptions import SnowflakeCheckError


def log_copy_into_results(results: tuple[Any, ...]) -> None:
    """
    Logs the results of a Snowflake COPY INTO statement into a readable dictionary.

    Parameters:
        results (tuple): The tuple output from the Snowflake COPY INTO command.

    Returns:
        None, but logs the formatted results.
    """
    # these are the column names returned by copy into statements
    # https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
    column_names = [
        "file",
        "status",
        "rows_parsed",
        "rows_loaded",
        "error_limit",
        "error_seen",
        "first_error",
        "first_error_line",
        "first_error_character",
        "first_error_column_name",
    ]

    formatted_results = dict(zip(column_names, results))
    print(f"Formatted Results: {formatted_results}")

    return None


def get_file_format(s3_prefix: str) -> str:
    if s3_prefix.endswith(".csv"):
        return "csv_format"
    elif s3_prefix.endswith(".parquet"):
        return "parquet_format"
    elif s3_prefix.endswith(".json"):
        return "json_format"
    else:
        raise ValueError(f"File Format not supported for {s3_prefix}")


def get_snowflake_conn():
    conn = SnowflakeHook(snowflake_conn_id="snowflake_conn", autocommit=True)
    connection = conn.get_sqlalchemy_engine().connect()
    return connection


def build_snowflake_table_from_s3(
    connection: Connection,
    stage: str,
    schema: str,
    table: str,
    s3_prefix: str,
) -> None:
    """
    Function to build a table in Snowflake based on a File in S3

    Args:
        connection (SQLAlchemy): The connection to the Snowflake Database

        stage (str): The stage in Snowflake that points to the S3 URL

        schema (str): The schema to build the table in

        table (str): The name of the table to build

        s3_prefix (str): The S3 Prefix to build the table from

    Returns:
        None, but builds the table in Snowflake as specified.

    """
    file_format = get_file_format(s3_prefix=s3_prefix)
    file_location = f"{stage}/{s3_prefix}"

    sql = f"""
    CREATE OR REPLACE TABLE {schema}.{table}
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
            LOCATION=>'{file_location}',
            FILE_FORMAT=>'{file_format}'
            )
        )
    );
    """

    try:
        print(f"Executing {sql}")
        # it returns a list of tuples, so we need to get the first element of the first tuple
        results = connection.execute(statement=sql).fetchall()[0][0]

        print(results)

        if results != f"Table {table.upper()} successfully created.":
            raise BaseException(
                f"Error Occurred while building {schema}.{table} for file {file_location}, table not created"
            )
    except BaseException as e:
        # Instead of raising e with a message, you can raise a new exception or modify the original
        raise Exception(
            f"Error Occurred while building {schema}.{table} for file {file_location}: {str(e)}"
        ) from e

    return sql


def load_snowflake_table_from_s3(
    connection: Connection,
    stage: str,
    file_format: str,
    schema: str,
    table: str,
    s3_prefix: str,
    truncate_table: bool = False,
) -> str:
    """
    Function to load a table in Snowflake based on a File in S3

    Args:
        connection (SQLAlchemy): The connection to the Snowflake Database

        stage (str): The stage in Snowflake that points to the S3 URL

        file_format (str): The file format to use when building the table

        schema (str): The schema to build the table in

        table (str): The name of the table to build

        s3_prefix (str): The S3 Prefix to build the table from

        truncate_table (bool): Whether to truncate the table before loading
    """
    sql_truncate = ""
    if truncate_table:
        sql_truncate = f"""
        truncate table {schema}.{table};
        """

    sql = f"""
        {sql_truncate}
        copy into {schema}.{table}
        from {stage}/{s3_prefix}
        file_format = '{file_format}'
        match_by_column_name = 'CASE_INSENSITIVE';

    """

    print(f"Executing {sql}")
    results = connection.execute(statement=sql).fetchall()[0]
    log_copy_into_results(results=results)

    return None


def check_snowflake_table_count(
    connection: Connection,
    database: str,
    schema: str,
    table_name: str,
    check_threshold: int = 0,
):
    """ """
    sql = f"""select count(*) from {database}.{schema}.{table_name};"""

    try:
        results = connection.execute(sql).fetchone()
        if results[0] <= check_threshold:
            raise SnowflakeCheckError(
                f"Table {database}.{schema}.{table_name} has 0 Records after DAG Run"
            )
        else:
            print(
                f"{database}.{schema}.{table_name} Check Successful ({results[0]} rows)"
            )
            pass
    except BaseException as e:
        raise e(f"Error Occurred while checking {database}.{schema}.{table_name}, {e}")


def set_session_tag(connection: Connection, query_tag: str):
    """ """
    sql = f"alter session set query_tag='{query_tag}"
    try:
        connection.execute(sql)
    except BaseException as e:
        print(f"Error Occurred while executing '{sql}', {e}")
