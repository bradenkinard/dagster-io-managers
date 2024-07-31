"""Unit tests for the Huggingface datasets io manager class."""

from dagster_io_managers.datasets import LocalDatasetIOManager, S3DatasetIOManager


def test_localdatasetiomanager_basepath(temp_directory):
    """Test the basepath property."""
    io_manager = LocalDatasetIOManager(directory=temp_directory)

    assert io_manager._base_path == temp_directory  # noqa: SLF001


def test_s3datasetiomanager_basepath():
    """Test the basepath property."""
    bucket_name = "my-test-bucket"
    io_manager = S3DatasetIOManager(bucket_name=bucket_name)

    assert io_manager._base_path == f"s3://{bucket_name}"  # noqa: SLF001
