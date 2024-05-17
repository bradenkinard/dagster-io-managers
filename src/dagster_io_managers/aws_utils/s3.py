"""Utility functions for working with AWS resources."""

import boto3


def get_bucketpath(bucket_name: str) -> str:
    """Return the base filepath for the S3 bucket."""
    return "s3://" + bucket_name


def get_filepath(bucket_name: str, key: str) -> str:
    """Return the full filepath for a given key."""
    return get_bucketpath(bucket_name) + "/" + key


def create_bucket(bucket_name: str, region: str | None = None) -> None:
    """Create the S3 bucket if it doesn't exist."""
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
    except s3.exceptions.ClientError:
        if region is None:
            s3.create_bucket(Bucket=bucket_name)
        else:
            location = {"LocationConstraint": region}
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location,
            )
