from datetime import datetime
import os
from typing import Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, create_engine
import snowflake.connector
from snowflake.sqlalchemy import MergeInto
from snowflake.sqlalchemy import URL

### Snowflake Connector
def connect_snowflake(
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
                    # password=None,
                    # private_key_passphrase.encode()
                    backend=default_backend()
                )
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption())

            ctx = snowflake.connector.connect(
                user=user,
                password=password,
                account=account,
                private_key=pkb,
                role=role,
                warehouse=warehouse,
                database=database,
                schema=schema,
                autocommit=autocommit,
            )

            return ctx
        except BaseException as e:
            print(f"Error Occurred while connecting to {account}, {e}")
            raise e
    else:
        try:
            ctx = snowflake.connector.connect(
                user=user,
                password=password,
                account=account,
                role=role,
                warehouse=warehouse,
                database=database,
                schema=schema,
                autocommit=autocommit,
            )

            return ctx
        except BaseException as e:
            print(f"Error Occurred while connecting to {account}, {e}")
            raise e

def query_snowflake(cursor: snowflake.connector.SnowflakeConnection, query: str):
    cs = cursor.cursor()

    try:
        cs.execute(query)
        # one_row = cs.fetchone() - this is to get values back 
        # print(one_row[0])
    except BaseException as e:
        print(f"Error Occurred, {e}")
        cs.close()
    finally:
        cs.close()

# ctx = connect_snowflake(
#     user='snowflake_kafka_sink_user',
#     password=os.environ.get('snowflake_pw'),
#     role='de_team'
# )

# df = query_snowflake(ctx, "SELECT current_version()")

### SQLAlchemy Stuff
def connect_snowflake_sqlalchemy(
    user: str,
    password: str,
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
                    backend=default_backend()
                )
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption())

            # url = f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}'
            engine = create_engine(URL(
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
                user=user,
                password=password,
                role=role,
                ),
                connect_args={
                    'private_key': pkb,
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
            # url = f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}'
            engine = create_engine(URL(
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

connection = connect_snowflake_sqlalchemy(
    user="jyablonski",
    password="xxx",
    role="accountadmin",
    warehouse="test_warehouse",
    database="snowpipe_db",
    schema="snowpipe",
    # private_key="astro_rsa_key.p8", # <---- This is the FILE PATH to the private key
    # private_key_passphrase=os.environ.get('pk_pass')
)

# df2 = pd.read_csv('campsite_test.csv')
# df2.to_sql('test_python_table_3', connection, index=False, if_exists = 'replace')

# df = pd.read_sql('select * from test_python_table_3 limit 10;', con = connection)

# connection.close()

def build_snowflake_table_s3(stage: str, file_format: str, database: str, schema: str, table_name: str):
    """
    a stage is pointed at an s3 url ex. s3://jyablonski-kafka-s3-sink/topics/movies

    stage = @movies_stage or @movies_stage/partition=0/movies+0+0000000000.snappy.parquet
    """
    sql = f"""
    CREATE TABLE {database}.{schema}.{table_name}
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
            LOCATION=>'{stage}',
            FILE_FORMAT=>'{file_format}'
            )
        ));
    """

    return sql

def load_snowflake_table_s3(stage: str, file_format: str, database: str, schema: str, table_name: str, truncate_table: bool = False):
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
    