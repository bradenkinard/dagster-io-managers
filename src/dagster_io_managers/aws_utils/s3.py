"""Utility functions for working with AWS resources."""

import boto3


def get_bucketpath(bucket_name: str) -> str:
    """Return the base filepath for the S3 bucket."""
    return "s3://" + bucket_name


def get_filepath(bucket_name: str, key: str) -> str:
    """Return the full filepath for a given key."""
    return get_bucketpath(bucket_name) + "/" + key


def get_key_from_filepath(filepath: str) -> str:
    """Return the key from an S3 filepath."""
    keylist = filepath.replace("s3://", "").split("/")[1:]
    return "/".join(keylist)


def get_bucket_from_filepath(filepath: str) -> str:
    """Return the bucket from an S3 filepath."""
    return filepath.replace("s3://", "").split("/")[0]


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


def write_to_s3(
    body: str,
    path: str,
) -> None:
    """Write a string to S3."""
    if not path.startswith("s3://"):
        raise InvalidS3PathError(path)

    bucket = get_bucket_from_filepath(path)
    key = get_key_from_filepath(path)

    write_to_bucket_key(bucket, key, body)


def write_to_bucket_key(bucket_name: str, key: str, body: str) -> None:
    """Write a string to an S3 bucket key."""
    s3 = boto3.resource("s3")
    s3.Object(bucket_name, key).put(Body=body)


def read_from_s3(path: str) -> str:
    """Read a string from S3."""
    if not path.startswith("s3://"):
        raise InvalidS3PathError(path)

    bucket = get_bucket_from_filepath(path)
    key = get_key_from_filepath(path)

    return read_from_bucket_key(bucket, key)


def read_from_bucket_key(bucket_name: str, key: str, encoding: str = "utf-8") -> str:
    """Read a string from an S3 bucket."""
    s3 = boto3.client("s3")
    return s3.get_object(Bucket=bucket_name, Key=key)["Body"].read().decode(encoding)


# Invalid S3 path exception
class InvalidS3PathError(Exception):
    """Exception for invalid S3 paths."""

    def __init__(self, path: str) -> None:
        """Raise an exception for invalid S3 paths."""
        super().__init__(f"{path} is not a valid S3 path. Must start with 's3://'.")
