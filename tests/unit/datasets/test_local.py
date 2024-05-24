"""Unit tests for the local Huggingface datasets io manager class."""

from dagster_io_managers.datasets import LocalDatasetIOManager


def test_localdatasetiomanager_basepath(temp_directory):
    """Test the basepath property."""
    io_manager = LocalDatasetIOManager(directory=temp_directory)

    assert io_manager._base_path == temp_directory  # noqa: SLF001
