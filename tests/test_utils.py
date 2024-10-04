import os
import pytest
from include.utils import (
    get_instance_type,
    get_schedule_interval,
)


@pytest.mark.parametrize(
    "env_var, env_value, expected",
    [
        ("INSTANCE_TYPE_TEST_DEV", "dev", "dev"),
        ("INSTANCE_TYPE_TEST_DEV_1", "dev-1", "dev"),
        ("INSTANCE_TYPE_TEST_DEV_LOCAL", "jacob-dev", "dev"),
        ("INSTANCE_TYPE_STG", "stg", "dev"),
        ("INSTANCE_TYPE", "prod", "prod"),
    ],
)
def test_get_instance_type(env_var, env_value, expected):
    os.environ[env_var] = env_value
    instance_type = get_instance_type(os.environ.get(env_var))
    assert instance_type == expected


@pytest.mark.parametrize(
    "env_var, env_value, is_override, expected",
    [
        ("INSTANCE_TYPE_DEV", "dev", False, None),
        ("INSTANCE_TYPE_DEV", "dev", True, "40 12 * * *"),
        ("INSTANCE_TYPE_STG", "stg", False, None),
        ("INSTANCE_TYPE", "prod", False, "40 12 * * *"),
    ],
)
def test_get_schedule_interval(env_var, env_value, is_override, expected):
    os.environ[env_var] = env_value
    cron_schedule = "40 12 * * *"

    sched_interval = get_schedule_interval(
        cron_schedule=cron_schedule,
        instance_type=os.environ.get(env_var),
        is_override=is_override,
    )

    assert sched_interval == expected
