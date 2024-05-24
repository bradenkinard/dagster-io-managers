"""Unit tests for the s3 text file io manager class."""

from dagster_io_managers.text import S3TextFileIOManager


def test_s3textiomanager_basepath():
    """Test the basepath property."""
    bucket_name = "my-test-bucket"
    io_manager = S3TextFileIOManager(bucket_name=bucket_name)

    assert io_manager._base_path == ""  # noqa: SLF001
