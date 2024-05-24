"""Unit tests for the s3 Huggingface datasests io manager class."""

from dagster_io_managers.datasets import S3DatasetIOManager


def test_s3datasetiomanager_basepath():
    """Test the basepath property."""
    bucket_name = "my-test-bucket"
    io_manager = S3DatasetIOManager(bucket_name=bucket_name)

    assert io_manager._base_path == f"s3://{bucket_name}"  # noqa: SLF001
