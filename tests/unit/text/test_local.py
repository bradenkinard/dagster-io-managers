"""Unit tests for the local text file io manager class."""

from dagster_io_managers.text import LocalTextFileIOManager


def test_localtextiomanager_basepath(temp_directory):
    """Test the basepath property."""
    io_manager = LocalTextFileIOManager(directory=temp_directory)

    assert io_manager._base_path == temp_directory  # noqa: SLF001
