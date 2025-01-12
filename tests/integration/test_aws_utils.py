import boto3
from moto import mock_aws
import pandas as pd
import pytest

from include.exceptions import S3PrefixCheckFail
from include.aws_utils import check_s3_file_exists, write_to_s3


@mock_aws
def test_check_s3_file_exists():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket_name = "jyablonski_fake_bucket"

    conn.create_bucket(Bucket=bucket_name)
    conn.put_object(Bucket=bucket_name, Key=f"{bucket_name}-file.txt", Body="zzz")

    # assert it can successfuly check a file
    assert (
        check_s3_file_exists(
            client=conn,
            bucket=bucket_name,
            file_prefix=f"{bucket_name}-file.txt",
        )
        == None
    )

    # assert it raises a failure when it checks a file that doesn't exist
    with pytest.raises(S3PrefixCheckFail):
        check_s3_file_exists(
            client=conn,
            bucket=bucket_name,
            file_prefix="my-fake-ass-file-yo.txt",
        )


@mock_aws
def test_write_to_s3_success():
    conn = boto3.client("s3", region_name="us-east-1")
    bucket_name = "jyablonski_write_to_s3_bucket"
    conn.create_bucket(Bucket=bucket_name)

    file_path = "test.parquet"
    df = pd.DataFrame(
        {
            "brand": ["Yum Yum", "Yum Yum", "Indomie", "Indomie", "Indomie"],
            "style": ["cup", "cup", "cup", "pack", "pack"],
            "rating": [4, 4, 3.5, 15, 5],
        }
    )

    write_test = write_to_s3(
        dataframe=df,
        s3_bucket=bucket_name,
        s3_path=file_path,
    )

    assert write_test == True


# get some dumbass mf error about no such bucket exists, im over it
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@mock_aws
def test_write_to_s3_fail():
    client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "jyablonski_write_to_s3_bucket_fail"

    file_path = "test.parquet"
    df = pd.DataFrame(
        {
            "brand": ["Yum Yum", "Yum Yum", "Indomie", "Indomie", "Indomie"],
            "style": ["cup", "cup", "cup", "pack", "pack"],
            "rating": [4, 4, 3.5, 15, 5],
        }
    )

    with pytest.raises(TypeError):
        write_test = write_to_s3(
            dataframe=df,
            s3_bucket=bucket_name,
            s3_path=file_path,
        )
