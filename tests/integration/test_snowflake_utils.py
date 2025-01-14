import pytest
from sqlalchemy.engine.base import Connection
from unittest.mock import patch, MagicMock

from include.snowflake_utils import (
    log_results,
    get_file_format,
    get_snowflake_conn,
    load_snowflake_table_from_s3,
)


@pytest.mark.parametrize(
    "results, statement_type, expected_output",
    [
        (
            ("file1.csv", "LOADED", 1000, 1000, 1, 0, None, None, None, None),
            "COPY",
            {
                "file": "file1.csv",
                "status": "LOADED",
                "rows_parsed": 1000,
                "rows_loaded": 1000,
                "error_limit": 1,
                "error_seen": 0,
                "first_error": None,
                "first_error_line": None,
                "first_error_character": None,
                "first_error_column_name": None,
            },
        ),
        (
            (10, 5),
            "MERGE",
            {
                "number_of_rows_inserted": 10,
                "number_of_rows_updated": 5,
            },
        ),
    ],
)
def test_log_results_parametrized(results, statement_type, expected_output):
    with patch("builtins.print") as mock_print:
        log_results(results=results, statement_type=statement_type)
        mock_print.assert_called_once_with(f"Formatted Results: {expected_output}")


@pytest.mark.parametrize(
    "s3_prefix, expected_format",
    [
        ("s3://bucket/folder/file.csv", "csv_format"),
        ("s3://bucket/folder/file.parquet", "parquet_format"),
        ("s3://bucket/folder/file.json", "json_format"),
    ],
)
def test_get_file_format_valid_extensions(s3_prefix, expected_format):
    assert get_file_format(s3_prefix=s3_prefix) == expected_format


@pytest.mark.parametrize(
    "s3_prefix",
    [
        "s3://bucket/folder/file.txt",
        "s3://bucket/folder/file.xml",
    ],
)
def test_get_file_format_invalid_extension(s3_prefix):
    with pytest.raises(ValueError, match=f"File format not supported for {s3_prefix}"):
        get_file_format(s3_prefix=s3_prefix)


@patch("include.snowflake_utils.SnowflakeHook")
def test_get_snowflake_conn(mock_snowflake_hook):
    conn_name = "test_conn"

    mock_engine = MagicMock()
    mock_connection = MagicMock(spec=Connection)

    mock_engine.connect.return_value = mock_connection
    mock_snowflake_hook.return_value.get_sqlalchemy_engine.return_value = mock_engine

    connection = get_snowflake_conn(conn_id=conn_name)

    mock_snowflake_hook.assert_called_once_with(
        snowflake_conn_id=conn_name, autocommit=True
    )
    mock_snowflake_hook.return_value.get_sqlalchemy_engine.assert_called_once()
    mock_engine.connect.assert_called_once()

    assert connection == mock_connection


@pytest.mark.parametrize(
    "truncate_table, expected_sql_partial",
    [
        (
            False,
            "copy into test_schema.test_table",
        ),
        (
            True,
            "truncate table test_schema.test_table",
        ),
        # (
        #     True,
        #     "truncate table test_schema.test_table_fake", # this would fail
        # ),
    ],
)
@patch("include.snowflake_utils.log_results")
def test_load_snowflake_table_from_s3(
    mock_log_results, truncate_table, expected_sql_partial
):
    mock_connection = MagicMock()

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("mocked_result",)]
    mock_connection.execute.return_value = mock_cursor

    load_snowflake_table_from_s3(
        connection=mock_connection,
        stage="test_stage",
        file_format="CSV",
        schema="test_schema",
        table="test_table",
        s3_prefix="s3/folder",
        truncate_table=truncate_table,
    )

    executed_sql = mock_connection.execute.call_args.kwargs["statement"].strip()

    assert (
        expected_sql_partial in executed_sql
    ), f"SQL does not contain expected partial statement: {expected_sql_partial}"

    mock_log_results.assert_called_once_with(
        results=("mocked_result",), statement_type="COPY"
    )
