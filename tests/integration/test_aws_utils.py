"""Integration tests for the aws-utils module."""

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


@mock_aws
def test_write_to_s3(aws_credentials):
    """Test the write_to_s3 method."""
    bucket_name = "my-test-bucket"
    key = "my-text-file.txt"
    path = f"s3://{bucket_name}/{key}"
    body = "Hello, world!"
    s3_client = boto3.client("s3")
    s3.create_bucket(bucket_name=bucket_name)

    s3.write_to_s3(body=body, path=path)
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    assert response["Body"].read().decode("utf-8") == body


@mock_aws
def test_read_from_s3(aws_credentials):
    """Test the read_from_s3 method."""
    bucket_name = "my-test-bucket"
    key = "my-text-file.txt"
    path = f"s3://{bucket_name}/{key}"
    body = "Hello, world!"
    s3_client = boto3.client("s3")
    s3.create_bucket(bucket_name=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)

    assert s3.read_from_s3(path) == body
