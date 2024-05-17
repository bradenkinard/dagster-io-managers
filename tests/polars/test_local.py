"""Tests for the local polars io manager module."""

from pathlib import Path

import polars as pl
import pytest
from dagster_io_managers.polars import LocalPolarsIOManager


@pytest.fixture()
def dataframe():
    """Create a simple polars dataframe."""
    return pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


def test_localpolarsiomanager_basepath(temp_directory):
    """Test the basepath property."""
    io_manager = LocalPolarsIOManager(directory=temp_directory)

    assert io_manager._base_path == temp_directory  # noqa: SLF001


def test_localpolarsiomanager_write_dataframe(temp_directory, polars_df):
    """Test the filepath method."""
    io_manager = LocalPolarsIOManager(directory=temp_directory)
    key = "some/key.parquet"
    path = f"{temp_directory}/{key}"

    io_manager._write_dataframe(polars_df, path)  # noqa: SLF001

    Path(path).exists()
