"""Unit tests for the polars io manager class."""

from dagster_io_managers.polars import LocalPolarsIOManager, S3PolarsIOManager


def test_localpolarsiomanager_basepath(temp_directory):
    """Test the basepath property."""
    io_manager = LocalPolarsIOManager(directory=temp_directory)

    assert io_manager._base_path == temp_directory  # noqa: SLF001


def test_s3polarsiomanager_basepath():
    """Test the basepath property."""
    bucket_name = "my-test-bucket"
    io_manager = S3PolarsIOManager(bucket_name=bucket_name)

    assert io_manager._base_path == f"s3://{bucket_name}"  # noqa: SLF001
