"""Unit tests for the local polars io manager class."""

from dagster_io_managers.polars import LocalPolarsIOManager


def test_localpolarsiomanager_basepath(temp_directory):
    """Test the basepath property."""
    io_manager = LocalPolarsIOManager(directory=temp_directory)

    assert io_manager._base_path == temp_directory  # noqa: SLF001
