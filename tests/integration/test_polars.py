"""Integration tests for the polars io manager class."""

# ruff: noqa: SLF001

from pathlib import Path

import boto3
from dagster_io_managers.polars import LocalPolarsIOManager, S3PolarsIOManager
from moto import mock_aws


def test_localpolarsiomanager_write_dataframe(temp_directory, polars_df):
    """Test the filepath method."""
    io_manager = LocalPolarsIOManager(directory=temp_directory)
    key = "some/key.parquet"
    path = f"{temp_directory}/{key}"

    io_manager._write(polars_df, path)

    assert Path(path).exists()


@mock_aws
def test_s3polarsiomanager_write_dataframe(aws_credentials, polars_df):
    """Test the filepath method."""
    bucket_name = "my-test-bucket"
    io_manager = S3PolarsIOManager(bucket_name=bucket_name)
    s3 = boto3.client("s3")

    io_manager._write(polars_df, "s3://my-test-bucket/some/key.parquet")
    response = s3.list_objects_v2(Bucket=bucket_name)

    assert response["KeyCount"] == 1
