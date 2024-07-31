"""Unit tests for the text file io manager class."""

from dagster_io_managers.text import LocalTextFileIOManager, S3TextFileIOManager


def test_localtextiomanager_basepath(temp_directory):
    """Test the basepath property."""
    io_manager = LocalTextFileIOManager(directory=temp_directory)

    assert io_manager._base_path == temp_directory  # noqa: SLF001


def test_s3textiomanager_basepath():
    """Test the basepath property."""
    bucket_name = "my-test-bucket"
    io_manager = S3TextFileIOManager(bucket_name=bucket_name)

    assert io_manager._base_path == f"s3://{bucket_name}"  # noqa: SLF001
