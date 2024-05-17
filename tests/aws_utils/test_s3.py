"""Tests for the aws module."""

import boto3
from dagster_io_managers.aws_utils import s3
from moto import mock_aws


@mock_aws
def test_create_bucket_no_region(aws_credentials):
    """Test initializing S3Repository without a region."""
    bucket_name = "my-test-bucket"
    s3_client = boto3.client("s3")
    s3.create_bucket(bucket_name=bucket_name)

    # Check if the bucket was created
    response = s3_client.list_buckets()
    bucket_names = [bucket["Name"] for bucket in response["Buckets"]]
    assert bucket_name in bucket_names


@mock_aws
def test_create_bucket_with_region(aws_credentials):
    """Test initializing S3Repository with a specific region."""
    bucket_name = "my-test-bucket"
    region = "us-west-1"
    s3_client = boto3.client("s3")
    s3.create_bucket(bucket_name=bucket_name, region=region)

    # Check if the bucket was created in the specified region
    response = s3_client.get_bucket_location(Bucket=bucket_name)
    assert response["LocationConstraint"] == region


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


@mock_aws
def test_write_to_s3(aws_credentials):
    """Test the write_to_s3 method."""
    bucket_name = "my-test-bucket"
    key = "my-text-file.txt"
    body = "Hello, world!"
    s3_client = boto3.client("s3")
    s3.create_bucket(bucket_name=bucket_name)

    s3.write_to_s3(bucket_name=bucket_name, key=key, body=body)
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    assert response["Body"].read().decode("utf-8") == body


@mock_aws
def test_read_from_s3(aws_credentials):
    """Test the read_from_s3 method."""
    bucket_name = "my-test-bucket"
    key = "my-text-file.txt"
    body = "Hello, world!"
    s3_client = boto3.client("s3")
    s3.create_bucket(bucket_name=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)

    assert s3.read_from_s3(bucket_name=bucket_name, key=key) == body
