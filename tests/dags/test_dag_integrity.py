from contextlib import contextmanager
import os
import logging
from unittest.mock import patch

import pytest
from airflow.models import DagBag

from include.utils import get_schedule_interval

APPROVED_TAGS = {
    "example",
    "nba_elt_project",
    "manual",
    "snowflake",
    "template",
    "test",
}


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        # we prepend "(None,None)" to ensure that a test object is always created even if its a no op.
        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


# @patch("include.aws_utils.create_ecs_task_operator")
@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(mock_create_ecs_task_operator, rel_path, rv):
    """Test for import errors on a file"""
    # mock_create_ecs_task_operator.return_value = None  # Mock the operator
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")

@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    test if a DAG is tagged and if those TAGs are in the approved list
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS


@pytest.mark.parametrize(
    "dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_retries(dag_id, dag, fileloc):
    """
    test if a DAG has retries set
    """
    dag_retries = 0
    assert (
        dag.default_args.get("retries", None) >= dag_retries
    ), f"{dag_id} in {fileloc} does not have retries not set to {dag_retries}."


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_schedule_interval_enabled(dag_id, dag, fileloc):
    """
    test if a DAG has the `get_schedule_interval` function attached
    """
    schedule_interval_check = get_schedule_interval(dag.schedule_interval)

    assert (
        dag.schedule_interval == schedule_interval_check
    ), f"{dag_id} in {fileloc} does not have the Schedule Interval Function Attached to manage Scheduling"


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_slack_callback_enabled(dag_id, dag, fileloc):
    """
    test if a DAG has the Slack Callback set if a Task fails
    """
    assert "alert" or "notification" in str(
        dag.default_args["on_failure_callback"]
    ), f"{dag_id} in {fileloc} has no Slack Alert Attached"
