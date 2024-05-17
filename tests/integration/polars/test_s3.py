"""Integration tests for the s3 Polars io manager module."""

import boto3
from dagster_io_managers.polars import S3PolarsIOManager
from moto import mock_aws


@mock_aws
def test_s3polarsiomanager_write_dataframe(aws_credentials, polars_df):
    """Test the filepath method."""
    bucket_name = "my-test-bucket"
    io_manager = S3PolarsIOManager(bucket_name=bucket_name)
    s3 = boto3.client("s3")

    io_manager._write_dataframe(polars_df, "s3://my-test-bucket/some/key.parquet")  # noqa: SLF001
    response = s3.list_objects_v2(Bucket=bucket_name)

    assert response["KeyCount"] == 1
