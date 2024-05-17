"""Unit tests for the aws-utils s3 module."""

from dagster_io_managers.aws_utils import s3


def test_basepath():
    """Test the basepath property."""
    bucket_name = "my-test-bucket"
    expected_path = f"s3://{bucket_name}"

    assert s3.get_bucketpath(bucket_name=bucket_name) == expected_path


def test_filepath():
    """Test the filepath method."""
    bucket_name = "my-test-bucket"
    key = "some/key"
    expected_path = f"s3://{bucket_name}/{key}"

    assert s3.get_filepath(bucket_name=bucket_name, key=key) == expected_path


def test_key_from_filepath():
    """Test the key_from_filepath method."""
    key = "some/key"
    filepath = "s3://my-test-bucket/" + key

    assert s3.get_key_from_filepath(filepath) == key


def test_bucket_from_filepath():
    """Test the bucket_from_filepath method."""
    bucket_name = "my-test-bucket"
    filepath = "s3://" + bucket_name + "/some/key"

    assert s3.get_bucket_from_filepath(filepath) == bucket_name
