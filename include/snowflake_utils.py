from typing import Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from sqlalchemy.engine.base import Connection
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.sqlalchemy import URL


try:
    from .exceptions import SnowflakeCheckError
except:
    from exceptions import SnowflakeCheckError


def snowflake_connection(
    user: str,
    password: str = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    schema: Optional[str] = None,
    database: Optional[str] = None,
    autocommit: bool = True,
    account: str = "qxb14358.us-east-1",
    private_key: str = None,
    private_key_passphrase: str = None,
):
    if private_key is not None:
        try:
            with open(private_key, "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=private_key_passphrase.encode(),
                    backend=default_backend(),
                )
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            engine = create_engine(
                URL(
                    account=account,
                    warehouse=warehouse,
                    database=database,
                    schema=schema,
                    user=user,
                    password=password,
                    role=role,
                ),
                connect_args={
                    "private_key": pkb,
                },
            )

            session = sessionmaker(bind=engine)()
            connection = engine.connect()

            return connection
        except BaseException as e:
            print(f"Error Occurred while connecting to {account}, {e}")
            raise e
    else:
        try:
            engine = create_engine(
                URL(
                    account=account,
                    warehouse=warehouse,
                    database=database,
                    schema=schema,
                    user=user,
                    password=password,
                    role=role,
                ),
            )
            session = sessionmaker(bind=engine)()
            connection = engine.connect()

            return connection
        except BaseException as e:
            print(f"Error Occurred while connecting to {account}, {e}")
            raise e


def build_snowflake_table_from_s3(
    connection: Connection,
    stage: str,
    file_format: str,
    database: str,
    schema: str,
    table_name: str,
) -> str:
    """
    Function to build a table in Snowflake based on a File in S3

    Args:
        connection (SQLAlchemy):

        stage (str):

        file_format (str):

        database (str):

        schema (str):

        table_name (str):

    Returns:
        None, but builds the table in Snowflake as specified.

    """
    sql = f"""
    CREATE OR REPLACE TABLE {database}.{schema}.{table_name}
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
            LOCATION=>'{stage}',
            FILE_FORMAT=>'{file_format}'
            )
        ));
    """

    try:
        print(f"Executing {sql}")
        connection.execute(sql)
        pass
    except BaseException as e:
        raise e(
            f"Error Occurred while building {database}.{schema}.{table_name} for file {stage}"
        )

    return sql


def load_snowflake_table_from_s3(
    stage: str,
    file_format: str,
    database: str,
    schema: str,
    table_name: str,
    truncate_table: bool = False,
) -> str:
    """
    a stage is pointed at an s3 url ex. s3://jyablonski-kafka-s3-sink/topics/movies

    stage = @movies_stage or @movies_stage/partition=0/movies+0+0000000000.snappy.parquet
    """
    sql_truncate = ""
    if truncate_table == True:
        sql_truncate = f"""
        truncate table {database}.{schema}.{table_name};
        """

    sql = f"""
        {sql_truncate}
        copy into {database}.{schema}.{table_name}
        from {stage}
        file_format = '{file_format}'
        match_by_column_name = 'CASE_INSENSITIVE';

    """

    return sql


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
