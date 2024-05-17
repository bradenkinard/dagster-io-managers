"""Integration tests for the local textfile io manager module."""

from pathlib import Path

from dagster_io_managers.text import LocalTextFileIOManager


def test_localtextfileiomanager_write_dataframe(temp_directory, text_string):
    """Test the filepath method."""
    io_manager = LocalTextFileIOManager(directory=temp_directory)
    key = "some/key.txt"
    path = temp_directory + "/" + key

    io_manager.write_to_file(text_string, path)

    assert Path(path).exists()
