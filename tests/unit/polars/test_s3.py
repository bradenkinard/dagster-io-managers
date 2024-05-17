"""Unit tests for the s3 Polars io manager module."""

from dagster_io_managers.polars import S3PolarsIOManager


def test_s3polarsiomanager_basepath():
    """Test the basepath property."""
    bucket_name = "my-test-bucket"
    io_manager = S3PolarsIOManager(bucket_name=bucket_name)

    assert io_manager._base_path == f"s3://{bucket_name}"  # noqa: SLF001
