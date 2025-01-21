from airflow.models.param import Param

# Shared parameters for reuse
SNOWFLAKE_PARAMS = {
    "snowflake_schema": Param(
        default="test_schema",
        type="string",
        title="Schema Name",
        description="Enter a Schema",
    ),
    "snowflake_table": Param(
        default="test_table",
        type="string",
        title="Table Name",
        description="Enter a Table",
    ),
    "s3_stage": Param(
        default="NBA_ELT_STAGE_PROD",
        type="string",
        title="S3 Stage",
        description="S3 Stage to Load Data to",
        enum=["NBA_ELT_STAGE_PROD"],
    ),
}

# TODO: finish params for snowflake dags
# Custom params specific to this DAG
# custom_params = {
#     "snowflake_table": Param(
#         default="custom_table",
#         type="string",
#         title="Custom Table Name",
#         description="Table Name specific to this DAG",
#     )
# }

# # Combine shared params with custom params
# dag_params = {**SNOWFLAKE_PARAMS, **custom_params}
