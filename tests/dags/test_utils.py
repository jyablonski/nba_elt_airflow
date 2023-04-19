import boto3
import pytest
from moto import mock_s3

from include.exceptions import S3PrefixCheckFail

from include.utils import check_s3_file_exists


@mock_s3
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
            prefix=f"{bucket_name}-file.txt",
        )
        == None
    )

    # assert it raises a failure when it checks a file that doesn't exist
    with pytest.raises(S3PrefixCheckFail):
        check_s3_file_exists(
            client=conn,
            bucket=bucket_name,
            prefix="my-fake-ass-file-yo.txt",
        )
