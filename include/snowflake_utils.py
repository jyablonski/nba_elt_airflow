from typing import Any

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from sqlalchemy.engine.base import Connection

try:
    from .exceptions import SnowflakeCheckError
except:
    from exceptions import SnowflakeCheckError

# conn stuff
# {
#   "account": "qp11074.us-east-2.aws",
#   "warehouse": "airflow_role_prod_warehouse",
#   "database": "production",
#   "role": "airflow_role_prod",
#   "insecure_mode": false
# }


def log_results(results: tuple[Any, ...], statement_type: str) -> None:
    """
    Logs the results of Snowflake Copy / Merge statements into a readable dictionary.

    Parameters:
        results (tuple): The tuple output from the Snowflake command.

        statement_type (str): Type of Snowflake Statement (COPY INTO, MERGE, etc.)

    Returns:
        None, but logs the formatted results.
    """
    if statement_type == "COPY":
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
    elif statement_type == "MERGE":
        column_names = [
            "number_of_rows_inserted",
            "number_of_rows_updated",
        ]
    else:
        raise ValueError(f"Statement Type {statement_type} not supported")

    formatted_results = dict(zip(column_names, results))
    print(f"Formatted Results: {formatted_results}")

    return None


def get_file_format(s3_prefix: str) -> str:
    formats = {
        ".csv": "csv_format",
        ".parquet": "parquet_format",
        ".json": "json_format",
    }

    for extension, file_format in formats.items():
        if s3_prefix.endswith(extension):
            return file_format

    raise ValueError(f"File format not supported for {s3_prefix}")


def get_snowflake_conn(conn_id: str = "snowflake_conn") -> Connection:
    conn = SnowflakeHook(snowflake_conn_id=conn_id, autocommit=True)
    connection = conn.get_sqlalchemy_engine().connect()
    return connection


def build_snowflake_table_from_s3(
    connection: Connection,
    stage: str,
    schema: str,
    table: str,
    s3_prefix: str,
    file_format: str,
) -> None:
    """
    Function to build a table in Snowflake based on a File in S3

    Args:
        connection (SQLAlchemy): The connection to the Snowflake Database

        stage (str): The stage in Snowflake that points to the S3 URL

        schema (str): The schema to build the table in

        table (str): The name of the table to build

        s3_prefix (str): The S3 Prefix to build the table from

        file_format (str): File Format to use

    Returns:
        None, but builds the table in Snowflake as specified.

    """
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


def create_deduped_temp_table(
    connection: Connection,
    schema: str,
    source_table: str,
    primary_keys: list[str],
    order_by_fields: list[str],
) -> None:
    """
    Function to deduplicate data in a source table using `QUALIFY ROW_NUMBER()`
    and store it in a temporary table.

    Args:
        connection (Connection): The connection to the Snowflake Database.

        schema (str): The schema for the source and temporary tables.

        source_table (str): The name of the source table to deduplicate.

        primary_keys (list[str]): Columns to use for deduplication.

        order_by_fields (list[str])): Field to use for deduplication priority in ORDER BY.


    Returns:
        None, but creates a temporary deduplicated table in Snowflake.
    """
    temp_table = f"temp_{source_table}"
    primary_key_columns = ", ".join(primary_keys)
    order_by_field_columns = ", ".join(order_by_fields)

    dedup_sql = f"""\
    CREATE TEMPORARY TABLE {schema}.{temp_table} AS
    SELECT *
    FROM {schema}.{source_table}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {primary_key_columns}
        ORDER BY {order_by_field_columns}
    ) = 1;"""
    try:
        print(f"Executing: {dedup_sql}")
        connection.execute(statement=dedup_sql)

        print(f"Deduplicated data stored in {schema}.{temp_table} successfully.")

    except Exception as e:
        raise Exception(
            f"Error occurred while deduplicating data in {schema}.{source_table}: {e}"
        )


def load_snowflake_table_from_s3(
    connection: Connection,
    stage: str,
    schema: str,
    table: str,
    s3_prefix: str,
    file_format: str,
    truncate_table: bool = False,
) -> str:
    """
    Function to load a table in Snowflake based on a File in S3

    Args:
        connection (SQLAlchemy): The connection to the Snowflake Database

        stage (str): The stage in Snowflake that points to the S3 URL

        schema (str): The schema to build the table in

        table (str): The name of the table to build

        s3_prefix (str): The S3 Prefix to build the table from

        file_format (str): The file format to use for the COPY INTO statement

        truncate_table (bool): Whether to truncate the table before loading

    Returns:
        None, but loads the table in Snowflake as specified
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
    log_results(results=results, statement_type="COPY")

    return None


def merge_snowflake_source_into_target(
    connection: Connection,
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
    primary_keys: list[str],
) -> None:
    """
    Function to merge a source table into a target table in Snowflake

    Args:
        connection (SQLAlchemy): The connection to the Snowflake Database

        source_schema (str): The schema of the source table

        source_table (str): The name of the source table

        target_schema (str): The schema of the target table

        target_table (str): The name of the target table

        primary_keys (list[str]): The primary keys to use in the ON clause

    Returns:
        None, but merges the source table into the target table in Snowflake

    """
    target_schema = target_schema.upper()
    target_table = target_table.upper()

    try:

        # pull columns from the target table; that's the source of truth
        # we need these to build insert / update clauses for the merge
        get_cols_query = connection.execute(
            f""" \
            select column_name
            from information_schema.columns
            where 
                table_schema = '{target_schema}'
                and table_name = '{target_table}'"""
        ).fetchall()

        column_names = [item[0] for item in get_cols_query]

        # primary key ON clause
        on_clause = " AND ".join(f"TARGET.{pk} = SOURCE.{pk}" for pk in primary_keys)

        # update clause
        update_set_clause = ", ".join(
            f"TARGET.{col} = SOURCE.{col}"
            for col in column_names
            if col not in primary_keys
        )

        # insert clause
        insert_columns = ", ".join(column_names)
        insert_values = ", ".join(f"SOURCE.{col}" for col in column_names)

        # can add when matched and __deleted = 1 then delete clause if data
        # has a column like that
        sql = f"""
        MERGE INTO {target_schema}.{target_table} AS TARGET
        USING {source_schema}.{source_table} AS SOURCE
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET
                {update_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values});
        """

        print(f"Executing {sql}")
        results = connection.execute(statement=sql).fetchall()[0]
        log_results(results=results, statement_type="MERGE")
        print(
            f"Merge Successful for {source_schema}.{source_table} into {target_schema}.{target_table}"
        )
        return None
    except Exception as e:
        raise Exception(
            f"Error Occurred while merging {source_schema}.{source_table} into {target_schema}.{target_table}: {e}"
        )


def check_snowflake_table_count(
    connection: Connection,
    schema: str,
    table_name: str,
    check_threshold: int = 0,
) -> None:
    """
    Function to check the count of a table in Snowflake

    Args:
        connection (SQLAlchemy): The connection to the Snowflake Database

        schema (str): The schema of the table

        table_name (str): The name of the table

        check_threshold (int): The threshold to check against

    Returns:
        None, but raises an error if the table has 0 records

    Raises:
        SnowflakeCheckError: If the table has 0 records
    """
    object_name = f"{schema}.{table_name}"
    sql = f"""select count(*) from {object_name};"""

    try:
        results = connection.execute(sql).fetchone()
        if results[0] <= check_threshold:
            raise SnowflakeCheckError(
                f"Table {object_name} has 0 Records after DAG Run"
            )
        else:
            print(f"{object_name} Check Successful ({results[0]} rows)")
            return None
    except BaseException as e:
        raise e(f"Error Occurred while checking {object_name}, {e}")


def set_session_tag(connection: Connection, query_tag: str) -> None:
    """
    Function to set a Session Tag in Snowflake. This is useful for
    tracking queries in the Snowflake and querying the history.

    Args:
        connection (SQLAlchemy): The connection to the Snowflake Database

        query_tag (str): The tag to set for the session

    Returns:
        None, but sets the query tag for the Snowflake session
    """
    sql = f"alter session set query_tag='{query_tag}"
    try:
        connection.execute(sql)
    except BaseException as e:
        print(f"Error Occurred while executing '{sql}', {e}")


def unload_to_s3(
    connection: Connection,
    s3_stage: str,
    s3_prefix: str,
    database_name: str,
    table_name: str,
    schema_name: str,
    file_format: str,
    limit: int | None = None,
) -> None:
    """
    Function to store a query's results into S3

    Args:
        connection (Connection): The connection to the Snowflake Database

        s3_stage (str): The S3 Stage to store the results in

        s3_prefix (str): The S3 Prefix to store the results in

        database_name (str): The name of the database

        table_name (str): The name of the table

        schema_name (str): The name of the schema

        file_format (str): The file format to store the results in

        limit (int): The limit to apply to the query

    Returns:
        None, but stores the results of the query in S3
    """
    # this forces it to load into an actual directory at the file path,
    # and not just into whichever file path was given
    if not s3_prefix.endswith("/"):
        s3_prefix += "/"

    if limit:
        limit = f"limit {limit}"

    query = f"""\
    copy into @{s3_stage}/{s3_prefix}
    from (
        select *
        from {database_name}.{schema_name}.{table_name}
        {limit}
    )
    file_format = {file_format};"""

    connection.execute(statement=query)
    pass


def merge_from_s3_to_snowflake(
    connection: Connection,
    stage: str,
    schema: str,
    table: str,
    s3_prefix: str,
    file_format: str,
    primary_keys: list[str] | None = None,
    order_by_fields: list[str] | None = None,
    truncate_table: bool = False,
) -> None:
    """
    Function to load data from S3 to Snowflake, optionally deduplicate, and merge it into a target table.

    Args:
        connection (SQLAlchemy): The connection to the Snowflake Database

        stage (str): The stage in Snowflake that points to the S3 URL

        schema (str): The schema to load the data into

        table (str): The name of the table to load

        s3_prefix (str): The S3 Prefix to load the data from

        file_format (str): The file format to use

        primary_keys (list[str]): Columns to use for deduplication (optional)

        order_by_fields (list[str]): Fields to order by for deduplication (optional)

        truncate_table (bool): Whether to truncate the table before loading (optional)

    Returns:
        None, but performs the following operations:
        - Creates/loads the Snowflake table from S3 data
        - Deduplicates the data
        - Optionally merges data into a target table
    """
    loading_schema = "staging"
    temp_table = f"temp_{table}"

    # Step 1: Build Snowflake Table in staging from S3 (if not already created)
    build_snowflake_table_from_s3(
        connection=connection,
        stage=stage,
        schema=loading_schema,
        table=table,
        s3_prefix=s3_prefix,
        file_format=file_format,
    )

    # Step 2: Load the Snowflake Table from S3
    load_snowflake_table_from_s3(
        connection=connection,
        stage=stage,
        schema=loading_schema,
        table=table,
        s3_prefix=s3_prefix,
        file_format=file_format,
        truncate_table=truncate_table,
    )

    # Step 3: Deduplicate Data if primary keys and order_by_fields are provided
    create_deduped_temp_table(
        connection=connection,
        schema=loading_schema,
        source_table=table,
        primary_keys=primary_keys,
        order_by_fields=order_by_fields,
    )


    # Step 4: Perform Merge
    merge_snowflake_source_into_target(
        connection=connection,
        source_schema=loading_schema,
        source_table=temp_table,
        target_schema=schema,
        target_table=table,
        primary_keys=primary_keys,
    )
