"""Integration tests for the local polars io manager module."""

from pathlib import Path

from dagster_io_managers.polars import LocalPolarsIOManager


def test_localpolarsiomanager_write_dataframe(temp_directory, polars_df):
    """Test the filepath method."""
    io_manager = LocalPolarsIOManager(directory=temp_directory)
    key = "some/key.parquet"
    path = f"{temp_directory}/{key}"

    io_manager._write_dataframe(polars_df, path)  # noqa: SLF001

    Path(path).exists()
