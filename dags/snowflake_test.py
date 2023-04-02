# from datetime import datetime, timedelta
# import pandas as pd

# from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# from include.snowflake_utils import build_snowflake_table_s3, load_snowflake_table_s3

# with DAG(
#     "snowflake_dag",
#     start_date=datetime(2021, 7, 7),
#     description="A sample Airflow DAG to perform data quality checks using SQL Operators.",
#     doc_md=__doc__,
#     schedule_interval=None,
#     catchup=False,
#     default_args={"snowflake_conn_id": "warehaus"},
# ) as dag:

#     # create_snowflake_table = SnowflakeOperator(
#     #     task_id="create_snowflake_stage",
#     #     sql=build_snowflake_table_s3(
#     #         stage="@movies_stage/partition=0/movies+0+0000000000.snappy.parquet",
#     #         file_format="my_parquet_format",
#     #         database="snowpipe_db",
#     #         schema="snowpipe",
#     #         table_name="movies6",
#     #     ),
#     # )

#     load_snowflake_table = SnowflakeOperator(
#         task_id="load_snowflake_stage",
#         sql=load_snowflake_table_s3(
#             stage="@movies_stage/partition=0/movies+0+0000000000.snappy.parquet",
#             file_format="my_parquet_format",
#             database="snowpipe_db",
#             schema="snowpipe",
#             table_name="movies6",
#             truncate_table=True,
#         ),
#     )

#     load_snowflake_table
