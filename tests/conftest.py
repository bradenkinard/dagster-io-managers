"""Fixtures for pytest."""

import os
from tempfile import TemporaryDirectory

import polars as pl
import pytest


@pytest.fixture()
def aws_credentials():
    """Mock AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture()
def temp_directory():
    """Create a temporary directory."""
    with TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture()
def polars_df():
    """Create a simple polars dataframe."""
    return pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
