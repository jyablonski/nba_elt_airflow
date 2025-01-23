from airflow.models.param import Param

# Shared parameters for reuse
COMMON_SNOWFLAKE_PARAMS = {
    "snowflake_schema": Param(
        default="test_schema",
        type="string",
        title="Schema Name",
        description="Enter a Schema",
        enum=["test_schema", "source", "experimental", "marts"],
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
    "file_format": Param(
        default="test_schema.parquet_format_tf",
        type="string",
        title="File Format",
        description="File Format to use",
        enum=["test_schema.parquet_format_tf"],
    ),
}
