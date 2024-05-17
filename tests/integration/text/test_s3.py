"""Integration tests for the s3 textfile io manager module."""

import boto3
from dagster_io_managers.text import S3TextFileIOManager
from moto import mock_aws


@mock_aws
def test_s3textfileiomanager_write_dataframe(aws_credentials, text_string):
    """Test the filepath method."""
    bucket_name = "my-test-bucket"
    io_manager = S3TextFileIOManager(bucket_name=bucket_name)
    s3 = boto3.client("s3")

    io_manager.write_to_file(text_string, "s3://my-test-bucket/some/key.txt")
    response = s3.list_objects_v2(Bucket=bucket_name)

    assert response["KeyCount"] == 1
