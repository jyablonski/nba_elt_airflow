import pytest
from sqlalchemy.engine.base import Connection
from unittest.mock import patch, MagicMock

from include.snowflake_utils import (
    log_results_copy,
    log_results_merge,
    get_file_format,
    get_snowflake_conn,
    load_snowflake_table_from_s3,
)


# Test cases for log_results_copy
@pytest.mark.parametrize(
    "results, expected_summary",
    [
        # Case 1: Single file successfully loaded
        (
            [("file1.csv", "LOADED", 1000, 1000, 1, 0, None, None, None, None)],
            {
                "total_files": 1,
                "total_rows_parsed": 1000,
                "total_rows_loaded": 1000,
                "total_errors_seen": 0,
                "files_with_errors": [],
            },
        ),
        # Case 2: Multiple files with one error
        (
            [
                ("file1.csv", "LOADED", 1000, 1000, 1, 0, None, None, None, None),
                ("file2.csv", "ERROR", 500, 400, 1, 1, "Some error", 23, 45, "column1"),
            ],
            {
                "total_files": 2,
                "total_rows_parsed": 1500,
                "total_rows_loaded": 1400,
                "total_errors_seen": 1,
                "files_with_errors": ["file2.csv"],
            },
        ),
    ],
)
def test_log_results_copy(results, expected_summary):
    with patch("builtins.print") as mock_print:
        log_results_copy(results=results)

        # Check if the high-level summary was printed
        mock_print.assert_any_call(f"High-Level Summary: {expected_summary}")


# Test cases for log_results_merge
@pytest.mark.parametrize(
    "results, expected_summary",
    [
        # Case 1: Rows inserted and updated
        (
            [(10, 20)],
            {
                "rows_inserted": 10,
                "rows_updated": 20,
            },
        ),
        # Case 2: No rows inserted or updated
        (
            [(0, 0)],
            {
                "rows_inserted": 0,
                "rows_updated": 0,
            },
        ),
    ],
)
def test_log_results_merge(results, expected_summary):
    with patch("builtins.print") as mock_print:
        log_results_merge(results=results)

        # Check if the merge summary was printed
        mock_print.assert_any_call(f"MERGE Summary: {expected_summary}")


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
            "COPY INTO test_schema.test_table",
        ),  # Verify COPY statement is executed
        # (
        #     True,
        #     "TRUNCATE TABLE test_schema.test_table",
        # ),  # Verify TRUNCATE statement is executed
        # TODO: fix truncate test. it calls 2 statements so
        # it doesn't capture the first one
    ],
)
@patch("include.snowflake_utils.log_results_copy")
def test_load_snowflake_table_from_s3(
    mock_log_results_copy, truncate_table, expected_sql_partial
):
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("mocked_result",)]
    mock_connection.execute.return_value = mock_cursor

    load_snowflake_table_from_s3(
        connection=mock_connection,
        stage="test_stage",
        schema="test_schema",
        table="test_table",
        s3_prefix="s3/folder",
        file_format="CSV",
        truncate_table=truncate_table,
    )

    executed_sql = mock_connection.execute.call_args.kwargs["statement"].strip()
    assert (
        expected_sql_partial in executed_sql
    ), f"SQL does not contain expected partial statement: {expected_sql_partial}"

    mock_log_results_copy.assert_called_once_with(results=[("mocked_result",)])
