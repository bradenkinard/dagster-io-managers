"""Integration tests for the textfile io manager class."""

# ruff: noqa: SLF001

from pathlib import Path

import boto3
from dagster_io_managers.text import LocalTextFileIOManager, S3TextFileIOManager
from moto import mock_aws


def test_localtextfileiomanager_write_dataframe(temp_directory, text_string):
    """Test the filepath method."""
    io_manager = LocalTextFileIOManager(directory=temp_directory)
    key = "some/key.txt"
    path = temp_directory + "/" + key

    io_manager._write(text_string, path)

    assert Path(path).exists()


@mock_aws
def test_s3textfileiomanager_write_dataframe(aws_credentials, text_string):
    """Test the filepath method."""
    bucket_name = "my-test-bucket"
    io_manager = S3TextFileIOManager(bucket_name=bucket_name)
    s3 = boto3.client("s3")

    io_manager._write(text_string, "s3://my-test-bucket/some/key.txt")
    response = s3.list_objects_v2(Bucket=bucket_name)

    assert response["KeyCount"] == 1
