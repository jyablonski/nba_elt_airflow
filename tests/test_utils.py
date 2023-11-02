import os

import pytest

from include.utils import (
    get_instance_type,
    get_schedule_interval,
)


def test_get_instance_type():
    os.environ["INSTANCE_TYPE_TEST_DEV"] = "dev"
    os.environ["INSTANCE_TYPE_TEST_DEV_1"] = "dev-1"
    os.environ["INSTANCE_TYPE_TEST_DEV_LOCAL"] = "jacob-dev"
    os.environ["INSTANCE_TYPE_STG"] = "stg"
    os.environ["INSTANCE_TYPE"] = "prod"

    instance_type_dev = get_instance_type(os.environ.get("INSTANCE_TYPE_TEST_DEV"))
    instance_type_dev_1 = get_instance_type(os.environ.get("INSTANCE_TYPE_TEST_DEV_1"))
    instance_type_dev_local = get_instance_type(
        os.environ.get("INSTANCE_TYPE_TEST_DEV_LOCAL")
    )
    instance_type_dev_stg = get_instance_type(os.environ.get("INSTANCE_TYPE_STG"))
    instance_type_dev_prod = get_instance_type(os.environ.get("INSTANCE_TYPE"))

    assert instance_type_dev == "dev"
    assert instance_type_dev_1 == "dev"
    assert instance_type_dev_local == "dev"
    assert instance_type_dev_stg == "stg"
    assert instance_type_dev_prod == "prod"


def test_get_schedule_interval():
    os.environ["INSTANCE_TYPE_DEV"] = "dev"
    os.environ["INSTANCE_TYPE_STG"] = "stg"
    os.environ["INSTANCE_TYPE"] = "prod"

    cron_schedule = "40 12 * * *"

    sched_interval_dev = get_schedule_interval(
        cron_schedule, instance_type=os.environ.get("INSTANCE_TYPE_DEV")
    )

    sched_interval_dev_override = get_schedule_interval(
        cron_schedule,
        instance_type=os.environ.get("INSTANCE_TYPE_DEV"),
        is_override=True,
    )

    sched_interval_stg = get_schedule_interval(
        cron_schedule, instance_type=os.environ.get("INSTANCE_TYPE_STG")
    )

    sched_interval_stg_override = get_schedule_interval(
        cron_schedule,
        instance_type=os.environ.get("INSTANCE_TYPE_STG"),
        is_override=True,
    )

    sched_interval_prod = get_schedule_interval(
        cron_schedule, instance_type=os.environ.get("INSTANCE_TYPE")
    )

    assert sched_interval_dev is None
    assert sched_interval_dev_override is None
    assert sched_interval_stg is None
    assert sched_interval_stg_override == cron_schedule
    assert sched_interval_prod == cron_schedule
